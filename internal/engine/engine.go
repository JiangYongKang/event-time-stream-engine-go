package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// LateOverflowPolicy 决定“待写出的迟到记录达到上限”时如何处理新迟到事件。
type LateOverflowPolicy int

const (
	// LateOverflowReject：上限满后新迟到事件被明确标记拒绝（计入 LateRejected 并打日志），
	// 数据不静默丢失，处理路径不被阻塞。默认策略。
	LateOverflowReject LateOverflowPolicy = iota
	// LateOverflowBlock：上限满后处理协程阻塞等待，直到下一次成功提交腾出空间。
	// 注意：sink 持续不可用时该策略会一直阻塞（与背压语义一致）。
	LateOverflowBlock
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

	// RetainCheckpoints 保留的检查点代数；0 用默认值（3）。
	RetainCheckpoints int
	// RecoverFromGeneration > 0 时从指定代恢复；0 表示从最新代恢复。
	RecoverFromGeneration uint64
	// MaxPendingOutput 尚未成功写出的记录上限（待提交窗口结果 + 迟到记录合计）。
	// 0 用默认值（10000）。到上限后：窗口提交延后并重试写出（处理协程会等待）；
	// 迟到记录按 LateOverflow 处理。
	MaxPendingOutput int
	// LateOverflow 迟到记录达到 MaxPendingOutput 时的策略，默认 Reject。
	LateOverflow LateOverflowPolicy
}

// Stats 运行期可观测指标。
type Stats struct {
	Processed          uint64 // 成功计入窗口的事件数
	Duplicates         uint64 // 被去重丢弃的事件数
	Late               uint64 // 被旁路的迟到事件数
	LateRejected       uint64 // 因待写出上限被明确拒绝的迟到事件数
	BackpressureBlocks uint64 // 生产者因队列满而阻塞的次数
	Checkpoints        uint64 // 已完成的检查点数
	CheckpointPrunes   uint64 // 因保留代数被删除的历史检查点代总数
	ForkPrunes         uint64 // 从旧代恢复时被清理的分叉未来代数
	FlushTriggers      uint64 // 因待写出上限/写出失败而提前触发检查点的次数
	WindowDefers       uint64 // 因待写出上限而延后（留在状态中）的窗口数
	SinkRetries        uint64 // 写出失败后的续写次数（同批再次提交）
	SinkReplays        uint64 // 续写时跳过的已落盘记录数（去重，避免重复写）
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
	lateReject  atomic.Uint64
	bpBlocks    atomic.Uint64
	checkpoints atomic.Uint64
	cpPrunes    atomic.Uint64
	forkPrunes  atomic.Uint64
	flushTrig   atomic.Uint64
	winDefers   atomic.Uint64
	sinkRetries atomic.Uint64
	sinkReplays atomic.Uint64
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
	pendingCond    sync.Cond // 等待待写出记录腾出空间（LateOverflowBlock）

	cpCh           chan struct{}
	cpMu           sync.Mutex
	cpClosed       bool // cpCh 关闭后置位，signalCheckpoint 不再发送
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
	if cfg.RetainCheckpoints <= 0 {
		cfg.RetainCheckpoints = 3
	}
	if cfg.MaxPendingOutput <= 0 {
		cfg.MaxPendingOutput = 10000
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
	store.SetRetain(cfg.RetainCheckpoints)
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
		LateRejected:       e.m.lateReject.Load(),
		BackpressureBlocks: e.m.bpBlocks.Load(),
		Checkpoints:        e.m.checkpoints.Load(),
		CheckpointPrunes:   e.m.cpPrunes.Load(),
		ForkPrunes:         e.m.forkPrunes.Load(),
		FlushTriggers:      e.m.flushTrig.Load(),
		WindowDefers:       e.m.winDefers.Load(),
		SinkRetries:        e.m.sinkRetries.Load(),
		SinkReplays:        e.m.sinkReplays.Load(),
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
				e.processEvent(ctx, ev)
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
	e.closeCheckpointChannel()
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
func (e *Engine) processEvent(ctx context.Context, ev Event) {
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
		rec := LateRecord{
			Event:     ev,
			Reason:    fmt.Sprintf("event_time %d <= watermark %d", ev.EventTime, wm),
			Watermark: wm,
		}
		accepted := e.enqueueLate(ctx, rec)
		e.m.late.Add(1)
		if !accepted {
			// 上限拒绝：明确标记，绝不静默丢弃。
			e.m.lateReject.Add(1)
			e.logger.Printf("LATE-REJECTED event id=%s key=%s event_time=%d watermark=%d (pending output at cap=%d)",
				ev.ID, ev.Key, ev.EventTime, wm, e.cfg.MaxPendingOutput)
			return
		}
		e.logger.Printf("LATE event id=%s key=%s event_time=%d watermark=%d -> side output (window [%d,%d) untouched)",
			ev.ID, ev.Key, ev.EventTime, wm, w.Start, w.End)
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
// 屏障（暂停生产者 + 等待在途清空）->
// 先续写此前提交失败的待写出批次（不丢、不重）->
// 收割到期窗口（超过待写出上限的窗口留在状态里延后）->
// 提交本批输出到 sink，再持久化检查点代（崩溃时未确认输出会被回滚）。
//
// sink 提交失败不会推进检查点：待写出批次原样保留并安排下一次重试，
// 已落盘部分由 sink 按记录身份续写，绝不重复写。
func (e *Engine) checkpoint() error {
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	e.waitInflightZero()

	wm := e.wm.Watermark()
	cpID := e.nextCheckpoint

	// committedR/committedL 记录本代最终写出的批次（写入检查点，崩溃回滚后据此幂等重放）。
	var committedR []WindowResult
	var committedL []LateRecord

	// flush 把当前 pending 作为 cpID 批次提交；成功则记录批次，失败安排重试。
	flush := func() error {
		if e.pendingCount() == 0 {
			return nil
		}
		r, l, err := e.commitPending(cpID)
		if err != nil {
			e.m.flushTrig.Add(1)
			e.signalCheckpoint()
			return err
		}
		committedR = append(committedR, r...)
		committedL = append(committedL, l...)
		return nil
	}

	// 1) 先续写此前提交失败而积压的待写出批次（幂等：已落盘部分被跳过）。
	if err := flush(); err != nil {
		return err
	}

	// 2) 按固定顺序锁全部分片（窗口分片 + 去重分片），避免死锁。
	for i := range e.shards {
		e.shards[i].mu.Lock()
	}
	for i := range e.dedup {
		e.dedup[i].mu.Lock()
	}
	var windows []WindowState
	var seen []string
	type harvestedWin struct {
		shard int
		key   string
	}
	var harvested []harvestedWin
	deferred := 0
	for i := range e.shards {
		sh := &e.shards[i]
		for k, ws := range sh.windows {
			if ws.Window.End <= wm {
				if e.pendingCount() >= e.cfg.MaxPendingOutput {
					// 待写出已到上限：窗口保留在状态中，等提交腾位后下一轮再收割。
					windows = append(windows, *ws)
					deferred++
					continue
				}
				// 收割到待写出集合，但窗口此刻仍保留在分片状态中：
				// 直到检查点代保存成功才删除。这样即便 sink 写出后、检查点保存前进程被杀，
				// 重启也能从上一代状态重新得到这些窗口（sink 端先 TruncateTo 回滚再幂等重放）。
				r := WindowResult{
					Key:         ws.Key,
					WindowStart: ws.Window.Start,
					WindowEnd:   ws.Window.End,
					Count:       ws.Count,
					Sum:         ws.Sum,
				}
				e.pendingMu.Lock()
				e.pendingResults = append(e.pendingResults, r)
				e.pendingMu.Unlock()
				harvested = append(harvested, harvestedWin{shard: i, key: k})
				e.logger.Printf("COMMIT window key=%s [%d,%d) count=%d sum=%.2f watermark=%d",
					ws.Key, ws.Window.Start, ws.Window.End, ws.Count, ws.Sum, wm)
				// 窗口保留在分片中直到检查点保存成功（崩溃兜底）；
				// 但不计入存活快照 windows——它只由下面的 PendingResults 承载，
				// 恢复时作为“已提交批次”重放，从而不会被当成存活窗口重复聚合。
				continue
			}
			windows = append(windows, *ws)
		}
	}
	if deferred > 0 {
		e.m.winDefers.Add(uint64(deferred))
		e.logger.Printf("PENDING CAP reached (%d): %d completed window(s) deferred to next checkpoint",
			e.cfg.MaxPendingOutput, deferred)
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

	// 3) 提交新收割批次。失败时窗口仍在分片状态、结果仍在 pending，
	// 下一轮第 1 步按同一 cpID 续写（已落盘部分按身份跳过），检查点不推进。
	if err := flush(); err != nil {
		return err
	}

	// 4) 输出确认后、检查点代保存成功前，窗口依旧保留；保存成功才正式移出状态。
	// 先重新加锁收割删除点（屏障仍暂停生产者，没有新事件并发）。
	cp := &Checkpoint{
		ID:             cpID,
		Offset:         e.source.Offset(),
		Watermark:      e.wm.Snapshot(),
		Windows:        windows,
		SeenIDs:        seen,
		PendingResults: committedR,
		PendingLates:   committedL,
		CreatedAtUnix:  time.Now().Unix(),
	}
	info, err := e.store.SaveWithInfo(cp)
	if err != nil {
		// 检查点未保存：从 pending 撤回本批收割结果；窗口仍在状态中，下轮重新收割续写。
		// 已落盘的批次在恢复时会被 TruncateTo 回滚，再由状态重算，不丢不重。
		e.pendingMu.Lock()
		e.pendingResults = append(e.pendingResults, committedR...)
		e.pendingLates = append(e.pendingLates, committedL...)
		e.pendingMu.Unlock()
		e.m.flushTrig.Add(1)
		e.signalCheckpoint()
		return fmt.Errorf("save checkpoint: %w", err)
	}

	// 检查点已持久化：正式把收割窗口从状态中删除。
	for _, h := range harvested {
		e.shards[h.shard].mu.Lock()
		delete(e.shards[h.shard].windows, h.key)
		e.shards[h.shard].mu.Unlock()
	}

	e.nextCheckpoint++
	e.m.checkpoints.Add(1)
	if len(info.Pruned) > 0 {
		e.m.cpPrunes.Add(uint64(len(info.Pruned)))
	}
	openWindows := len(windows)
	e.logger.Printf("CHECKPOINT id=%d offset=%d watermark=%d open_windows=%d results=%d lates=%d retained_generations=%d pruned=%v%s",
		cpID, cp.Offset, wm, openWindows, len(committedR), len(committedL),
		e.store.Retain(), info.Pruned, migrateNote(info.Migrated))
	return nil
}

func migrateNote(migrated bool) string {
	if migrated {
		return "migrated from legacy single-file checkpoint layout"
	}
	return ""
}

// commitPending 把当前待写出集合作为 checkpointID 这一批提交给 sink。
// 成功则清空 pending、唤醒等待空间的协程，并返回已提交批次（供写入检查点）；
// 失败原样保留 pending（调用方安排重试），返回错误。
func (e *Engine) commitPending(checkpointID uint64) (results []WindowResult, lates []LateRecord, err error) {
	e.pendingMu.Lock()
	results = e.pendingResults
	lates = e.pendingLates
	e.pendingMu.Unlock()

	rep, cerr := e.sink.CommitBatch(checkpointID, results, lates)
	if cerr != nil {
		e.logger.Printf("SINK RETRY checkpoint=%d error=%v (results_pending=%d lates_pending=%d, written=%d/%d will be resumed)",
			checkpointID, cerr, len(results), len(lates), rep.WrittenResults+rep.WrittenLates,
			len(results)+len(lates))
		return nil, nil, fmt.Errorf("sink commit: %w", cerr)
	}

	replays := rep.ReplayedResults + rep.ReplayedLates
	if rep.Resume {
		e.m.sinkRetries.Add(1)
		e.m.sinkReplays.Add(uint64(replays))
		e.logger.Printf("SINK RESUME checkpoint=%d repaired_tail=%v replayed(skipped)=%d (results=%d lates=%d) written(results=%d lates=%d)",
			checkpointID, rep.RepairedTail, replays, rep.ReplayedResults, rep.ReplayedLates,
			rep.WrittenResults, rep.WrittenLates)
	} else {
		e.logger.Printf("SINK COMMIT checkpoint=%d results=%d lates=%d",
			checkpointID, rep.WrittenResults, rep.WrittenLates)
	}

	e.pendingMu.Lock()
	e.pendingResults = nil
	e.pendingLates = nil
	e.pendingCond.Broadcast()
	e.pendingMu.Unlock()
	return results, lates, nil
}

// recover 启动时恢复：按配置加载最新代或指定代检查点 -> 恢复状态 ->
// 清理分叉的未来代 -> 回滚未确认输出 -> 定位源。
// 检查点损坏或版本不符时返回可区分错误，绝不从中间静默继续。
func (e *Engine) recover() error {
	requested := e.cfg.RecoverFromGeneration
	var cp *Checkpoint
	var err error
	if requested > 0 {
		cp, err = e.store.LoadGeneration(requested)
	} else {
		cp, err = e.store.Load()
	}
	switch {
	case errors.Is(err, ErrCheckpointNotFound):
		if requested > 0 {
			ids, _ := e.store.Generations()
			return fmt.Errorf("recover failed: %w (available generations=%v)", err, ids)
		}
		// 首次运行（还没有任何检查点）：输出文件可能残留上一进程写出中途被杀的残行。
		// 先识别并裁掉半截残行；若发现“完整但损坏”的行则明确失败，绝不硬接。
		if rerr := e.repairSinkTails(); rerr != nil {
			return rerr
		}
		e.logger.Printf("RECOVER no checkpoint found, starting fresh; rolling back any stale sink output")
		if terr := e.sink.TruncateTo(0); terr != nil {
			return terr
		}
		return e.source.SeekOffset(0)
	case err != nil:
		// ErrCheckpointCorrupt / ErrCheckpointVersion 等：明确失败，不静默换一代硬接。
		ids, _ := e.store.Generations()
		return fmt.Errorf("recover failed (available generations=%v): %w", ids, err)
	}

	// 从旧代恢复：先清理该代之后分叉的未来代，保证恢复后线性重放、不重复计数。
	if n, derr := e.store.DeleteGenerationsAfter(cp.ID); derr != nil {
		return fmt.Errorf("prune forked generations: %w", derr)
	} else if n > 0 {
		e.m.forkPrunes.Add(uint64(n))
		e.logger.Printf("RECOVER pruning %d forked generation(s) newer than target %d before replay", n, cp.ID)
	}
	// 老路径指针副本刷新为目标代，避免它仍指向被清理的未来代。
	if data, rerr := os.ReadFile(e.store.genPath(cp.ID)); rerr == nil {
		if werr := writeAtomic(e.store.legacyPath, data); werr != nil {
			return fmt.Errorf("refresh checkpoint pointer after fork prune: %w", werr)
		}
	}

	// committedWin 标记本代已经收割并随批次提交的窗口。
	// 这些窗口为“写出已完成、保存前崩溃”兜底而保留在 Windows 快照里，
	// 但恢复后它们属于已提交结果（由 PendingResults 重放），不能再当作存活窗口，
	// 否则从 offset 重放时会被重复聚合。
	committedWin := map[string]struct{}{}
	for _, r := range cp.PendingResults {
		committedWin[windowKey(r.Key, Window{Start: r.WindowStart, End: r.WindowEnd})] = struct{}{}
	}

	for i := range e.shards {
		e.shards[i].mu.Lock()
	}
	for i := range e.dedup {
		e.dedup[i].mu.Lock()
	}
	var liveWindows []WindowState
	for _, ws := range cp.Windows {
		if _, done := committedWin[windowKey(ws.Key, ws.Window)]; done {
			continue // 已提交窗口：不恢复为存活状态
		}
		idx := e.shardIndex(ws.Key)
		w := ws
		e.shards[idx].windows[windowKey(ws.Key, ws.Window)] = &w
		liveWindows = append(liveWindows, ws)
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
	// 回滚前先处理写出中途被杀留下的半截残行（有检查点场景）：
	// 残行裁掉后随下面的批次重放补齐；真正损坏（完整行解析/CRC 失败）则明确报错。
	if rerr := e.repairSinkTails(); rerr != nil {
		return rerr
	}
	if err := e.sink.TruncateTo(cp.ID); err != nil {
		return fmt.Errorf("rollback uncommitted sink output: %w", err)
	}
	if err := e.source.SeekOffset(cp.Offset); err != nil {
		return fmt.Errorf("seek source: %w", err)
	}
	e.nextCheckpoint = cp.ID + 1

	// 重放该代记录的提交批次。正常情况下它已完整落盘（续写时全部记录被跳过）；
	// 若崩溃发生在写出中途，则残行被修补、已写部分被跳过、只补缺失部分：不丢不重。
	if len(cp.PendingResults) > 0 || len(cp.PendingLates) > 0 {
		rep, rerr := e.sink.CommitBatch(cp.ID, cp.PendingResults, cp.PendingLates)
		if rerr != nil {
			return fmt.Errorf("replay committed batch for checkpoint %d: %w", cp.ID, rerr)
		}
		e.logger.Printf("RECOVER replay checkpoint=%d repaired_tail=%v replayed(skipped)=%d (results=%d lates=%d) rewritten(results=%d lates=%d)",
			cp.ID, rep.RepairedTail, rep.ReplayedResults+rep.ReplayedLates,
			rep.ReplayedResults, rep.ReplayedLates, rep.WrittenResults, rep.WrittenLates)
	}

	from := "latest"
	if requested > 0 {
		from = "requested"
	}
	e.logger.Printf("RECOVER using %s checkpoint generation id=%d (format v%d) offset=%d watermark=%v open_windows=%d committed_in_cp=%d seen_ids=%d",
		from, cp.ID, cp.Version, cp.Offset, cp.Watermark, len(liveWindows), len(cp.PendingResults), len(cp.SeenIDs))
	for _, ws := range liveWindows {
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

// pendingCount 返回尚未成功写出的记录总数（窗口结果 + 迟到记录）。
func (e *Engine) pendingCount() int {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return len(e.pendingResults) + len(e.pendingLates)
}

func (e *Engine) signalCheckpoint() {
	e.cpMu.Lock()
	defer e.cpMu.Unlock()
	if e.cpClosed {
		return
	}
	select {
	case e.cpCh <- struct{}{}:
	default:
	}
}

// closeCheckpointChannel 关闭检查点信号通道并标记，关闭后 signalCheckpoint 为空操作。
func (e *Engine) closeCheckpointChannel() {
	e.cpMu.Lock()
	defer e.cpMu.Unlock()
	if !e.cpClosed {
		e.cpClosed = true
		close(e.cpCh)
	}
}

// enqueueLate 把一条迟到记录放入待写出集合，受 MaxPendingOutput 约束：
//   - Reject（默认）：满了先触发一次提前写出；仍满则返回 false（明确拒绝并计数）。
//   - Block：满了先触发一次提前写出，随后阻塞等待提交腾空间（与背压一致）。
//
// 等待期间不持有检查点屏障所需的任何分片锁，因此不会与检查点死锁。
func (e *Engine) enqueueLate(ctx context.Context, rec LateRecord) bool {
	e.pendingMu.Lock()
	if len(e.pendingResults)+len(e.pendingLates) < e.cfg.MaxPendingOutput {
		e.pendingLates = append(e.pendingLates, rec)
		e.pendingMu.Unlock()
		return true
	}
	e.pendingMu.Unlock()

	// 到上限：按约定立即触发一次写出。
	e.m.flushTrig.Add(1)
	e.logger.Printf("PENDING CAP reached (%d) for late event id=%s: triggering flush (policy=%s)",
		e.cfg.MaxPendingOutput, rec.Event.ID, latePolicyName(e.cfg.LateOverflow))
	e.signalCheckpoint()

	if e.cfg.LateOverflow == LateOverflowReject {
		e.pendingMu.Lock()
		ok := len(e.pendingResults)+len(e.pendingLates) < e.cfg.MaxPendingOutput
		if ok {
			e.pendingLates = append(e.pendingLates, rec)
		}
		e.pendingMu.Unlock()
		return ok
	}

	// Block：等待提交腾空间。
	e.pendingMu.Lock()
	for len(e.pendingResults)+len(e.pendingLates) >= e.cfg.MaxPendingOutput {
		if ctx.Err() != nil {
			// 关闭/取消：不再等待，交给调用方按拒绝处理。
			e.pendingMu.Unlock()
			return false
		}
		e.pendingCond.Wait()
	}
	e.pendingLates = append(e.pendingLates, rec)
	e.pendingMu.Unlock()
	return true
}

func latePolicyName(p LateOverflowPolicy) string {
	if p == LateOverflowBlock {
		return "block"
	}
	return "reject"
}

// repairSinkTails 在恢复开始时处理输出文件尾部：
// 半截残行（无换行的不完整写入）被裁掉并打日志，随后由检查点批次幂等补齐；
// 完整但解析失败 / CRC 不匹配的行属于真损坏，包装 ErrSinkCorrupt 明确失败，绝不硬接。
func (e *Engine) repairSinkTails() error {
	rep, err := e.sink.RepairTails()
	if err != nil {
		e.logger.Printf("RECOVER sink file genuinely corrupt (a complete line failed to parse/verify): %v; refusing to treat it as a partial tail", err)
		return fmt.Errorf("recover failed: %w", err)
	}
	if rep.ResultsTrim || rep.LateTrim {
		e.logger.Printf("RECOVER trimmed partial trailing write (process killed mid-write): results=%v lates=%v; torn bytes discarded and will be refilled idempotently",
			rep.ResultsTrim, rep.LateTrim)
	}
	return nil
}
