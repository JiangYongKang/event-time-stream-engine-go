package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
)

// Config 引擎配置。
type Config struct {
	WindowSizeMs      int64       // 滚动窗口大小（毫秒）
	AllowedLatenessMs int64       // 允许迟到时长（毫秒）
	NumShards         int         // 状态分片数（并发单元）
	Workers           int         // 工作协程数，0 表示等于 NumShards
	QueueCapacity     int         // 待处理队列容量（背压上限）
	CheckpointEvery   uint64      // 每处理多少条事件做一次检查点
	Logger            *log.Logger // 运行日志；nil 则丢弃
	// ProcessedHook 在每条事件成功计入窗口后回调（参数为累计处理数），
	// 主要用于测试确定性触发崩溃等场景。
	ProcessedHook func(processed uint64)
}

// Stats 运行期可观测指标。
type Stats struct {
	Processed          uint64 // 成功计入窗口的事件数
	Duplicates         uint64 // 被去重丢弃的事件数
	Late               uint64 // 被旁路的迟到事件数
	BackpressureBlocks uint64 // 生产者因队列满而阻塞的次数
	Checkpoints        uint64 // 已完成的检查点数
}

// shard 是窗口状态分片：同一 key 永远路由到同一分片，
// 因此同一窗口的状态更新只由持有该分片锁的协程进行。
type shard struct {
	mu      sync.Mutex
	windows map[string]*WindowState
}

// dedupShard 是去重集合分片，按事件 ID 哈希路由（与 key 无关），
// 保证同一事件 ID 无论携带什么 key 都能被去重。
type dedupShard struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

type metrics struct {
	processed   atomic.Uint64
	duplicates  atomic.Uint64
	late        atomic.Uint64
	bpBlocks    atomic.Uint64
	checkpoints atomic.Uint64
}

// Engine 是流处理引擎主体。
type Engine struct {
	cfg    Config
	source *FileSource
	sink   *FileSink
	store  *CheckpointStore
	logger *log.Logger

	assigner *Assigner
	wm       *WatermarkTracker
	shards   []shard
	dedup    []dedupShard

	queue chan Event // 有界队列：背压边界

	pauseMu    sync.RWMutex // 检查点屏障：写锁暂停生产者
	inflightMu sync.Mutex
	inflight   sync.Cond
	inflightN  int64

	pendingMu      sync.Mutex
	pendingResults []WindowResult
	pendingLates   []LateRecord

	cpCh           chan struct{}
	nextCheckpoint uint64
	m              metrics
}

func NewEngine(cfg Config, source *FileSource, sink *FileSink, store *CheckpointStore) *Engine {
	if cfg.NumShards <= 0 {
		cfg.NumShards = 4
	}
	if cfg.Workers <= 0 {
		cfg.Workers = cfg.NumShards
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 64
	}
	if cfg.CheckpointEvery == 0 {
		cfg.CheckpointEvery = 100
	}
	e := &Engine{
		cfg:      cfg,
		source:   source,
		sink:     sink,
		store:    store,
		assigner: NewAssigner(cfg.WindowSizeMs),
		wm:       NewWatermarkTracker(cfg.NumShards, cfg.AllowedLatenessMs),
		shards:   make([]shard, cfg.NumShards),
		queue:    make(chan Event, cfg.QueueCapacity),
		cpCh:     make(chan struct{}, 1),
		// 检查点 ID 从 1 开始：全新启动时 TruncateTo(0) 可清除全部历史输出。
		nextCheckpoint: 1,
	}
	e.inflight.L = &e.inflightMu
	for i := range e.shards {
		e.shards[i].windows = make(map[string]*WindowState)
	}
	e.dedup = make([]dedupShard, cfg.NumShards)
	for i := range e.dedup {
		e.dedup[i].seen = make(map[string]struct{})
	}
	if cfg.Logger != nil {
		e.logger = cfg.Logger
	} else {
		e.logger = log.New(nopWriter{}, "", 0)
	}
	return e
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// Stats 返回当前指标快照。
func (e *Engine) Stats() Stats {
	return Stats{
		Processed:          e.m.processed.Load(),
		Duplicates:         e.m.duplicates.Load(),
		Late:               e.m.late.Load(),
		BackpressureBlocks: e.m.bpBlocks.Load(),
		Checkpoints:        e.m.checkpoints.Load(),
	}
}

// Run 运行处理循环直到源耗尽（正常结束并做最终检查点）
// 或 ctx 取消（模拟崩溃：不做最终检查点，直接返回）。
func (e *Engine) Run(ctx context.Context) error {
	if err := e.recover(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range e.queue {
				e.processEvent(ev)
				e.decInflight()
			}
		}()
	}
	cpDone := make(chan struct{})
	go e.checkpointLoop(cpDone)

	// 生产者：读源 -> 有界队列。队列满即背压，阻塞并计数。
	produceErr := make(chan error, 1)
	go func() {
		produceErr <- e.produce(ctx)
	}()

	err := <-produceErr
	// 等待在途事件全部处理完毕再关闭队列。
	e.waitInflightZero()
	close(e.queue)
	wg.Wait()
	close(e.cpCh)
	<-cpDone

	if err != nil {
		// ctx 取消：模拟崩溃，不做最终检查点。
		return err
	}
	// 源耗尽：所有分区水位推到无穷大，剩余窗口全部完成，做最终检查点。
	e.wm.Close()
	return e.checkpoint()
}

// produce 顺序读取源并投递到有界队列。
func (e *Engine) produce(ctx context.Context) error {
	for {
		e.pauseMu.RLock()
		select {
		case <-ctx.Done():
			e.pauseMu.RUnlock()
			return ctx.Err()
		default:
		}
		startOffset := e.source.Offset()
		ev, ok, err := e.source.Next()
		if err != nil {
			e.pauseMu.RUnlock()
			return err
		}
		if !ok {
			e.pauseMu.RUnlock()
			return nil
		}
		e.incInflight()
		select {
		case e.queue <- ev:
		case <-ctx.Done():
			// 取消：事件未入队，回退读取位点，保证不丢。
			e.source.SeekOffset(startOffset)
			e.decInflight()
			e.pauseMu.RUnlock()
			return ctx.Err()
		default:
			// 队列满：背压，阻塞等待并计数（可观测）。
			e.m.bpBlocks.Add(1)
			e.logger.Printf("BACKPRESSURE queue full (cap=%d), producer blocked", e.cfg.QueueCapacity)
			select {
			case e.queue <- ev:
			case <-ctx.Done():
				e.source.SeekOffset(startOffset)
				e.decInflight()
				e.pauseMu.RUnlock()
				return ctx.Err()
			}
		}
		e.pauseMu.RUnlock()
	}
}

// processEvent 处理单条事件：去重 -> 水位线 -> 迟到判定 -> 窗口累计。
func (e *Engine) processEvent(ev Event) {
	idx := e.shardIndex(ev.Key)
	sh := &e.shards[idx]

	ds := &e.dedup[e.idIndex(ev.ID)]
	ds.mu.Lock()
	if _, dup := ds.seen[ev.ID]; dup {
		ds.mu.Unlock()
		e.m.duplicates.Add(1)
		e.logger.Printf("DEDUP event id=%s key=%s dropped as duplicate", ev.ID, ev.Key)
		return
	}
	ds.seen[ev.ID] = struct{}{}
	ds.mu.Unlock()

	e.wm.Observe(idx, ev.EventTime)
	wm := e.wm.Watermark()
	w := e.assigner.Assign(ev.EventTime)

	if ev.EventTime <= wm {
		e.m.late.Add(1)
		e.logger.Printf("LATE event id=%s key=%s event_time=%d watermark=%d -> side output (window [%d,%d) untouched)",
			ev.ID, ev.Key, ev.EventTime, wm, w.Start, w.End)
		e.pendingMu.Lock()
		e.pendingLates = append(e.pendingLates, LateRecord{
			Event:     ev,
			Reason:    fmt.Sprintf("event_time %d <= watermark %d", ev.EventTime, wm),
			Watermark: wm,
		})
		e.pendingMu.Unlock()
		return
	}

	key := windowKey(ev.Key, w)
	sh.mu.Lock()
	ws, ok := sh.windows[key]
	if !ok {
		ws = &WindowState{Key: ev.Key, Window: w}
		sh.windows[key] = ws
	}
	ws.Count++
	ws.Sum += ev.Value
	count, sum := ws.Count, ws.Sum
	sh.mu.Unlock()

	n := e.m.processed.Add(1)
	e.logger.Printf("WINDOW event id=%s key=%s event_time=%d -> window [%d,%d) count=%d sum=%.2f watermark=%d",
		ev.ID, ev.Key, ev.EventTime, w.Start, w.End, count, sum, wm)

	if n%e.cfg.CheckpointEvery == 0 {
		select {
		case e.cpCh <- struct{}{}:
		default:
		}
	}
	if e.cfg.ProcessedHook != nil {
		e.cfg.ProcessedHook(n)
	}
}

// checkpointLoop 响应检查点信号。
func (e *Engine) checkpointLoop(done chan<- struct{}) {
	defer close(done)
	for range e.cpCh {
		if err := e.checkpoint(); err != nil {
			e.logger.Printf("CHECKPOINT ERROR: %v", err)
		}
	}
}

// checkpoint 执行一次一致性检查点：
// 屏障（暂停生产者 + 等待在途清空）-> 完成到期窗口 ->
// 先提交输出到 sink，再持久化检查点（崩溃时未确认输出会被回滚）。
func (e *Engine) checkpoint() error {
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	e.waitInflightZero()

	wm := e.wm.Watermark()
	cpID := e.nextCheckpoint

	// 按固定顺序锁全部分片（窗口分片 + 去重分片），避免死锁。
	for i := range e.shards {
		e.shards[i].mu.Lock()
	}
	for i := range e.dedup {
		e.dedup[i].mu.Lock()
	}
	var windows []WindowState
	var seen []string
	for i := range e.shards {
		sh := &e.shards[i]
		for k, ws := range sh.windows {
			if ws.Window.End <= wm {
				// 窗口完成：移出状态，进入待写出结果。
				e.pendingMu.Lock()
				e.pendingResults = append(e.pendingResults, WindowResult{
					Key:          ws.Key,
					WindowStart:  ws.Window.Start,
					WindowEnd:    ws.Window.End,
					Count:        ws.Count,
					Sum:          ws.Sum,
					CheckpointID: cpID,
				})
				e.pendingMu.Unlock()
				e.logger.Printf("COMMIT window key=%s [%d,%d) count=%d sum=%.2f watermark=%d",
					ws.Key, ws.Window.Start, ws.Window.End, ws.Count, ws.Sum, wm)
				delete(sh.windows, k)
				continue
			}
			windows = append(windows, *ws)
		}
	}
	for i := range e.dedup {
		for id := range e.dedup[i].seen {
			seen = append(seen, id)
		}
	}
	for i := range e.shards {
		e.shards[i].mu.Unlock()
	}
	for i := range e.dedup {
		e.dedup[i].mu.Unlock()
	}

	e.pendingMu.Lock()
	results := e.pendingResults
	lates := e.pendingLates
	e.pendingResults = nil
	e.pendingLates = nil
	e.pendingMu.Unlock()
	for i := range lates {
		lates[i].CheckpointID = cpID
	}

	// 先写输出，再保存检查点。崩溃发生在两者之间时，
	// 恢复端会用 TruncateTo 回滚未确认输出，保证恰好一次。
	if err := e.sink.Commit(cpID, results, lates); err != nil {
		return fmt.Errorf("sink commit: %w", err)
	}
	cp := &Checkpoint{
		ID:        cpID,
		Offset:    e.source.Offset(),
		Watermark: e.wm.Snapshot(),
		Windows:   windows,
		SeenIDs:   seen,
	}
	if err := e.store.Save(cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	e.nextCheckpoint++
	e.m.checkpoints.Add(1)
	e.logger.Printf("CHECKPOINT id=%d offset=%d watermark=%d open_windows=%d results=%d lates=%d",
		cpID, cp.Offset, wm, len(windows), len(results), len(lates))
	return nil
}

// recover 启动时恢复：加载检查点 -> 恢复状态 -> 回滚未确认输出 -> 定位源。
// 检查点损坏或版本不符时返回可区分错误，绝不从中间静默继续。
func (e *Engine) recover() error {
	cp, err := e.store.Load()
	switch {
	case errors.Is(err, ErrCheckpointNotFound):
		e.logger.Printf("RECOVER no checkpoint found, starting fresh; rolling back any stale sink output")
		if terr := e.sink.TruncateTo(0); terr != nil {
			return terr
		}
		return e.source.SeekOffset(0)
	case err != nil:
		// ErrCheckpointCorrupt / ErrCheckpointVersion 等：明确失败。
		return fmt.Errorf("recover failed: %w", err)
	}

	for i := range e.shards {
		e.shards[i].mu.Lock()
	}
	for i := range e.dedup {
		e.dedup[i].mu.Lock()
	}
	for _, ws := range cp.Windows {
		idx := e.shardIndex(ws.Key)
		w := ws
		e.shards[idx].windows[windowKey(ws.Key, ws.Window)] = &w
	}
	for _, id := range cp.SeenIDs {
		e.dedup[e.idIndex(id)].seen[id] = struct{}{}
	}
	for i := range e.shards {
		e.shards[i].mu.Unlock()
	}
	for i := range e.dedup {
		e.dedup[i].mu.Unlock()
	}
	e.wm.Restore(cp.Watermark)
	if err := e.sink.TruncateTo(cp.ID); err != nil {
		return fmt.Errorf("rollback uncommitted sink output: %w", err)
	}
	if err := e.source.SeekOffset(cp.Offset); err != nil {
		return fmt.Errorf("seek source: %w", err)
	}
	e.nextCheckpoint = cp.ID + 1
	e.logger.Printf("RECOVER from checkpoint id=%d offset=%d watermark=%v open_windows=%d seen_ids=%d",
		cp.ID, cp.Offset, cp.Watermark, len(cp.Windows), len(cp.SeenIDs))
	return nil
}

func (e *Engine) shardIndex(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(e.shards)))
}

func (e *Engine) idIndex(id string) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32() % uint32(len(e.dedup)))
}

func windowKey(key string, w Window) string {
	return fmt.Sprintf("%s@%d", key, w.Start)
}

func (e *Engine) incInflight() {
	e.inflightMu.Lock()
	e.inflightN++
	e.inflightMu.Unlock()
}

func (e *Engine) decInflight() {
	e.inflightMu.Lock()
	e.inflightN--
	if e.inflightN == 0 {
		e.inflight.Broadcast()
	}
	e.inflightMu.Unlock()
}

func (e *Engine) waitInflightZero() {
	e.inflightMu.Lock()
	for e.inflightN > 0 {
		e.inflight.Wait()
	}
	e.inflightMu.Unlock()
}
