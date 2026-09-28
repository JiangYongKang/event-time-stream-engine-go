package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ErrSinkUnavailable 表示写出端在重试上限后仍然失败，
// 待写出积压无法继续增长，引擎明确停机（绝不静默丢结果）。
var ErrSinkUnavailable = errors.New("sink unavailable after retries")

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

	// MaxCheckpoints 保留的检查点代数上限，0 用默认值 3。
	MaxCheckpoints int
	// RestoreCheckpointID 指定从哪一代检查点恢复，0 表示从最新一代。
	RestoreCheckpointID uint64
	// MaxPendingResults 等待重试写出的窗口结果上限，0 用默认值 10000。
	// 达到上限时强制执行一次带退避的同步写出；写出持续失败则引擎明确报错停机。
	MaxPendingResults int
	// MaxPendingLates 等待重试写出的迟到记录上限，0 用默认值 10000。
	MaxPendingLates int
	// MaxCommitRetries 写出失败后的最大尝试次数（含首次），0 用默认值 5。
	MaxCommitRetries int
	// CommitRetryBackoff 写出重试退避间隔，0 用默认值 10ms。
	CommitRetryBackoff time.Duration
}

// Stats 运行期可观测指标。
type Stats struct {
	Processed          uint64 // 成功计入窗口的事件数
	Duplicates         uint64 // 被去重丢弃的事件数
	Late               uint64 // 被旁路的迟到事件数
	BackpressureBlocks uint64 // 生产者因队列满而阻塞的次数
	Checkpoints        uint64 // 已完成的检查点数

	CheckpointEvictions uint64 // 因代数上限被删除/恢复旧代时被裁剪的检查点文件数
	SinkRetries         uint64 // 写出失败后的重试次数（每次重新尝试 +1）
	SinkReplayResults   uint64 // 重试/恢复重放中重新写出的窗口结果条数
	SinkReplayLates     uint64 // 重试/恢复重放中重新写出的迟到记录条数
	PendingLimitHits    uint64 // 待写出积压达到上限、触发强制写出的次数
	SinkFatal           uint64 // 写出在重试上限后仍失败、引擎明确停机的次数
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

	cpEvictions atomic.Uint64
	sinkRetries atomic.Uint64
	replayRes   atomic.Uint64
	replayLates atomic.Uint64
	pendingHits atomic.Uint64
	sinkFatal   atomic.Uint64
}

// Engine 是流处理引擎主体。
type Engine struct {
	cfg    Config
	source *FileSource
	sink   Sink
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
	pendingCond    sync.Cond // 待写出积压回落到上限以下时唤醒被阻塞的生产者
	pendingResults []WindowResult
	pendingLates   []LateRecord

	cpCh           chan struct{}
	nextCheckpoint uint64
	lastCommitID   uint64 // 上次尝试提交的检查点 ID（用于识别同代重试）
	commitAttempts int    // 同代提交已尝试次数
	m              metrics

	runCtx  context.Context // 写出端持续失败时取消，驱动整条流水线停机
	cancel  context.CancelFunc
	fatalMu sync.Mutex
	fatal   error
}

func NewEngine(cfg Config, source *FileSource, sink Sink, store *CheckpointStore) *Engine {
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
	if cfg.MaxCheckpoints <= 0 {
		cfg.MaxCheckpoints = 3
	}
	if cfg.MaxPendingResults <= 0 {
		cfg.MaxPendingResults = 10_000
	}
	if cfg.MaxPendingLates <= 0 {
		cfg.MaxPendingLates = 10_000
	}
	if cfg.MaxCommitRetries <= 0 {
		cfg.MaxCommitRetries = 5
	}
	if cfg.CommitRetryBackoff <= 0 {
		cfg.CommitRetryBackoff = 10 * time.Millisecond
	}
	store.SetMaxGenerations(cfg.MaxCheckpoints)
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
	e.pendingCond.L = &e.pendingMu
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
	if fs, ok := sink.(*FileSink); ok {
		fs.SetLogger(e.logger)
	}
	return e
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// Stats 返回当前指标快照。
func (e *Engine) Stats() Stats {
	return Stats{
		Processed:           e.m.processed.Load(),
		Duplicates:          e.m.duplicates.Load(),
		Late:                e.m.late.Load(),
		BackpressureBlocks:  e.m.bpBlocks.Load(),
		Checkpoints:         e.m.checkpoints.Load(),
		CheckpointEvictions: e.m.cpEvictions.Load(),
		SinkRetries:         e.m.sinkRetries.Load(),
		SinkReplayResults:   e.m.replayRes.Load(),
		SinkReplayLates:     e.m.replayLates.Load(),
		PendingLimitHits:    e.m.pendingHits.Load(),
		SinkFatal:           e.m.sinkFatal.Load(),
	}
}

// Run 运行处理循环直到源耗尽（正常结束并做最终检查点）
// 或 ctx 取消（模拟崩溃：不做最终检查点，直接返回）。
// 写出端持续失败达到重试上限时，引擎也会明确停机并返回 ErrSinkUnavailable。
func (e *Engine) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.runCtx = runCtx
	e.cancel = cancel
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
		produceErr <- e.produce(runCtx)
	}()

	err := <-produceErr
	// 等待在途事件全部处理完毕再关闭队列。
	e.waitInflightZero()
	close(e.queue)
	wg.Wait()
	close(e.cpCh)
	<-cpDone

	// 写出端致命错误优先暴露（即使生产者是因取消而退出）。
	if ferr := e.getFatal(); ferr != nil {
		return ferr
	}
	if err != nil {
		// ctx 取消：模拟崩溃，不做最终检查点。
		return err
	}
	// 源耗尽：所有分区水位推到无穷大，剩余窗口全部完成，做最终检查点。
	e.wm.Close()
	if cerr := e.checkpoint(); cerr != nil {
		if ferr := e.getFatal(); ferr != nil {
			return ferr
		}
		return cerr
	}
	return nil
}

// failSink 标记写出端致命错误并取消流水线。
func (e *Engine) failSink(err error) {
	e.fatalMu.Lock()
	if e.fatal == nil {
		e.fatal = fmt.Errorf("%w: %v", ErrSinkUnavailable, err)
	}
	e.fatalMu.Unlock()
	e.m.sinkFatal.Add(1)
	e.logger.Printf("SINK FATAL: %v — stopping engine, pending results/lates retained on restart", err)
	e.runCancel()
}

func (e *Engine) runCancel() {
	if e.cancel != nil {
		e.cancel()
	}
}

func (e *Engine) getFatal() error {
	e.fatalMu.Lock()
	defer e.fatalMu.Unlock()
	return e.fatal
}

// produce 顺序读取源并投递到有界队列。
func (e *Engine) produce(ctx context.Context) error {
	for {
		// 待写出积压达到上限：背压（阻塞）并强制触发一次写出把积压刷出去；
		// 写出在重试上限后仍失败时 runCtx 被取消，这里随之中止而不是继续堆积。
		if err := e.waitPendingRoom(ctx); err != nil {
			return err
		}
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

// waitPendingRoom 在待写出积压达到上限时背压：计数、记日志、强制触发一次检查点
// （把积压刷给 sink），并阻塞到积压回落或流水线被取消。
func (e *Engine) waitPendingRoom(ctx context.Context) error {
	e.pendingMu.Lock()
	nRes, nLate := len(e.pendingResults), len(e.pendingLates)
	if nRes < e.cfg.MaxPendingResults && nLate < e.cfg.MaxPendingLates {
		e.pendingMu.Unlock()
		return nil
	}
	e.m.pendingHits.Add(1)
	e.logger.Printf("PENDING LIMIT HIT pending_results=%d/%d pending_lates=%d/%d: forcing flush and blocking producer",
		nRes, e.cfg.MaxPendingResults, nLate, e.cfg.MaxPendingLates)
	e.pendingMu.Unlock()

	// 非阻塞投递检查点信号（cpCh 容量 1，已有待处理信号则复用）。
	select {
	case e.cpCh <- struct{}{}:
	default:
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e.getFatal() != nil {
			return e.getFatal()
		}
		e.pendingMu.Lock()
		nRes, nLate = len(e.pendingResults), len(e.pendingLates)
		if nRes < e.cfg.MaxPendingResults && nLate < e.cfg.MaxPendingLates {
			e.pendingMu.Unlock()
			return nil
		}
		// 定时轮询释放锁，避免与检查点屏障形成锁顺序问题。
		e.pendingMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.cfg.CommitRetryBackoff):
		}
	}
}

// onPendingLimit 在迟到旁路等非生产者路径发现积压到上限时调用：
// 计数并强制触发一次写出（写出仍失败的停机由检查点的重试上限负责）。
func (e *Engine) onPendingLimit(kind string, limit int) {
	e.m.pendingHits.Add(1)
	e.logger.Printf("PENDING LIMIT HIT %s reached cap=%d: forcing checkpoint flush", kind, limit)
	select {
	case e.cpCh <- struct{}{}:
	default:
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
		hitLimit := len(e.pendingLates) >= e.cfg.MaxPendingLates
		e.pendingMu.Unlock()
		if hitLimit {
			e.onPendingLimit("late records", e.cfg.MaxPendingLates)
		}
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
			if e.getFatal() != nil {
				return
			}
			e.logger.Printf("CHECKPOINT DEFERRED: %v (batch retained, will replay in next checkpoint)", err)
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

	// 本代提交：同一 cpID 幂等重写；提交与保存全部成功后才推进 nextCheckpoint，
	// 因此失败后下一次检查点仍用同一个 cpID 重放，sink 端按代去重，绝不重复写出。
	attempt := 1
	if e.lastCommitID == cpID {
		e.commitAttempts++
		attempt = e.commitAttempts
	} else {
		e.lastCommitID = cpID
		e.commitAttempts = 1
	}

	// 先写输出，再保存检查点。崩溃发生在两者之间时，
	// 恢复端会用 TruncateTo 回滚未确认输出，保证恰好一次。
	if err := e.commitOutput(cpID, attempt, results, lates); err != nil {
		// 写不出：批次退回 pending，等待下次检查点补回；若已积压到上限则明确停机。
		e.restorePending(results, lates)
		if e.pendingAtLimit() {
			e.failSink(err)
		}
		return fmt.Errorf("sink commit: %w", err)
	}
	cp := &Checkpoint{
		ID:          cpID,
		Offset:      e.source.Offset(),
		Watermark:   e.wm.Snapshot(),
		Windows:     windows,
		SeenIDs:     seen,
		CreatedAtMs: time.Now().UnixMilli(),
	}
	if err := e.saveCheckpoint(cp, attempt); err != nil {
		e.restorePending(results, lates)
		if e.pendingAtLimit() {
			e.failSink(err)
		}
		return fmt.Errorf("save checkpoint: %w", err)
	}
	e.nextCheckpoint++
	e.m.checkpoints.Add(1)
	e.m.cpEvictions.Store(e.store.Evictions())
	e.pendingMu.Lock()
	if len(e.pendingResults) == 0 && len(e.pendingLates) == 0 {
		e.pendingCond.Broadcast()
	}
	e.pendingMu.Unlock()
	e.logger.Printf("CHECKPOINT id=%d attempt=%d offset=%d watermark=%d open_windows=%d results=%d lates=%d retained_generations<=%d",
		cpID, attempt, cp.Offset, wm, len(windows), len(results), len(lates), e.cfg.MaxCheckpoints)
	return nil
}

// commitOutput 按代幂等提交输出，失败按退避重试。
// 重试时 sink 会先剔除本代残留（含写了一半的残缺行）再整体重写，
// 因此同代多次尝试不会产生重复记录。
func (e *Engine) commitOutput(cpID uint64, attempt int, results []WindowResult, lates []LateRecord) error {
	for i := range results {
		results[i].CheckpointID = cpID
	}
	for i := range lates {
		lates[i].CheckpointID = cpID
	}
	var err error
	for try := attempt; try <= e.cfg.MaxCommitRetries; try++ {
		if try > attempt {
			e.m.sinkRetries.Add(1)
			e.m.replayRes.Add(uint64(len(results)))
			e.m.replayLates.Add(uint64(len(lates)))
			e.logger.Printf("SINK RETRY cp=%d try=%d/%d: replaying %d window result(s), %d late record(s) after failure (%v)",
				cpID, try, e.cfg.MaxCommitRetries, len(results), len(lates), err)
			select {
			case <-e.runCtx.Done():
				return e.runCtx.Err()
			case <-time.After(e.cfg.CommitRetryBackoff):
			}
		}
		if err = e.sink.Commit(cpID, results, lates); err == nil {
			if try > attempt {
				e.logger.Printf("SINK RETRY cp=%d: commit succeeded on try %d, replayed results=%d lates=%d",
					cpID, try, len(results), len(lates))
			}
			return nil
		}
		e.logger.Printf("SINK FAIL cp=%d try=%d/%d: %v (results=%d lates=%d retained for replay)",
			cpID, try, e.cfg.MaxCommitRetries, err, len(results), len(lates))
	}
	return err
}

// saveCheckpoint 持久化检查点（同名代数文件原子覆盖，可安全重试）。
func (e *Engine) saveCheckpoint(cp *Checkpoint, attempt int) error {
	var err error
	for try := attempt; ; try++ {
		err = e.store.Save(cp)
		if err == nil {
			return nil
		}
		if try >= e.cfg.MaxCommitRetries {
			return err
		}
		e.logger.Printf("CHECKPOINT SAVE RETRY id=%d try=%d/%d: %v", cp.ID, try+1, e.cfg.MaxCommitRetries, err)
		select {
		case <-e.runCtx.Done():
			return e.runCtx.Err()
		case <-time.After(e.cfg.CommitRetryBackoff):
		}
	}
}

// restorePending 把提交失败的批次放回待写出队列（顺序无关，重放时整体重写）。
func (e *Engine) restorePending(results []WindowResult, lates []LateRecord) {
	if len(results) == 0 && len(lates) == 0 {
		return
	}
	e.pendingMu.Lock()
	e.pendingResults = append(results, e.pendingResults...)
	e.pendingLates = append(lates, e.pendingLates...)
	e.pendingMu.Unlock()
}

// pendingAtLimit 判断待写出积压是否达到任一上限。
func (e *Engine) pendingAtLimit() bool {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return len(e.pendingResults) >= e.cfg.MaxPendingResults ||
		len(e.pendingLates) >= e.cfg.MaxPendingLates
}

// recover 启动时恢复：选择检查点代数 -> 恢复状态 -> 回滚未确认输出 -> 定位源。
//
//   - cfg.RestoreCheckpointID 为 0 时从最新代数恢复，否则必须从指定代数恢复；
//   - 指定旧代时，比它更新的代数一律裁剪删除，绝不混用；
//   - 升级前的 v1 单文件检查点仍可读取（见 ErrCheckpointVersion 的版本下限）；
//   - 检查点读不了、损坏或版本不兼容时返回可区分错误，绝不从中间静默继续。
func (e *Engine) recover() error {
	var (
		cp       *Checkpoint
		legacyV1 bool
		err      error
	)
	available, lerr := e.store.List()
	if lerr != nil {
		return fmt.Errorf("list checkpoints: %w", lerr)
	}
	if e.cfg.RestoreCheckpointID > 0 {
		cp, err = e.store.LoadID(e.cfg.RestoreCheckpointID)
		if err != nil {
			return fmt.Errorf("recover failed: %w", err)
		}
		e.logger.Printf("RECOVER explicitly requested generation %d (available newest-first=%v)",
			e.cfg.RestoreCheckpointID, available)
	} else {
		// 未指定代数：v2 代数目录优先；无代数时回退升级前的 v1 单文件。
		cp, err = e.store.Load()
		if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
			return fmt.Errorf("recover failed: %w", err)
		}
		if cp != nil {
			// v1 单文件来源的判定：代数目录为空（否则 Load 一定来自代数文件）。
			if len(available) == 0 {
				legacyV1 = cp.Version < checkpointVersion
			}
			err = nil
		}
	}

	if cp == nil {
		e.logger.Printf("RECOVER no checkpoint found, starting fresh; rolling back any stale sink output")
		if terr := e.sink.TruncateTo(0); terr != nil {
			return terr
		}
		return e.source.SeekOffset(0)
	}

	// 从指定旧代恢复：废弃比它更新的代数，保证之后的新代与恢复点连续。
	if pruned, perr := e.store.PruneFrom(cp.ID); perr != nil {
		return fmt.Errorf("prune generations newer than %d: %w", cp.ID, perr)
	} else if pruned > 0 {
		e.logger.Printf("RECOVER pruned %d generation(s) newer than requested cp=%d", pruned, cp.ID)
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
	e.lastCommitID = cp.ID
	e.commitAttempts = 0

	origin := "generation"
	if legacyV1 {
		origin = "legacy v1 single-file (pre-upgrade format)"
	}
	e.logger.Printf("RECOVER using %s checkpoint: generation=%d resume_offset=%d watermark=%v open_windows=%d seen_ids=%d",
		origin, cp.ID, cp.Offset, cp.Watermark, len(cp.Windows), len(cp.SeenIDs))
	for _, ws := range cp.Windows {
		e.logger.Printf("RECOVER window state key=%s [%d,%d) count=%d sum=%.2f",
			ws.Key, ws.Window.Start, ws.Window.End, ws.Count, ws.Sum)
	}
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
