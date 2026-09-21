package engine

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/checkpoint"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/sink"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/source"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/state"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/watermark"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/window"
)

// Config 是引擎运行参数与资源上限。
type Config struct {
	WindowSize      time.Duration // 滚动窗口长度
	OutOfOrderness  time.Duration // 生成水位线预留的乱序余量
	AllowedLateness time.Duration // 水位线之外仍接受迟到事件的范围
	BatchSize       int           // 每轮每分区最多读取的事件数
	PendingQueueCap int           // 每分区待处理批次队列上限（背压阈值）
	CheckpointEvery time.Duration // 周期检查点间隔
	ProcessDelay    time.Duration // 每条事件注入的处理耗时（演示/测试背压）
	LogEveryEvent   bool          // 是否逐条打印事件与窗口判定
	// CrashAfterEvents > 0 时，累计处理达到该条数后立即取消运行（确定性故障注入）。
	// 崩溃发生在“批处理后、提交前”：位点与结果均未持久化，恢复时整批重放。
	CrashAfterEvents int64
	// CrashAfterBatches > 0 时，成功应用指定数量“事件批次”后立即崩溃
	// （位点仅在内存、未做检查点），用于确定性地演示未提交批次重放。
	CrashAfterBatches int64
	// StopAfterBatches > 0 时，成功应用指定数量批次后干净停机（非错误），
	// 并确保至少落一个包含活动窗口状态与读取位点的检查点，供后续恢复继续。
	StopAfterBatches int64
}

func (c *Config) withDefaults() {
	if c.WindowSize <= 0 {
		c.WindowSize = time.Minute
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 8
	}
	if c.PendingQueueCap <= 0 {
		c.PendingQueueCap = 4
	}
	if c.CheckpointEvery <= 0 {
		c.CheckpointEvery = 50 * time.Millisecond
	}
}

// Metrics 是引擎可观测指标快照。
type Metrics struct {
	Processed        int64
	DuplicateDropped int64
	LateSideOutput   int64
	WindowsCommitted int64
	Checkpoints      int64
	BackPressure     int64  // 触发队列上限、被迫阻塞/限速次数
	RestoredFrom     string // 恢复所依据的检查点 ID
	Watermark        time.Time
}

// Engine 编排数据接入、事件时间窗口、水位线、检查点与恰好一次写出。
type Engine struct {
	cfg Config
	src source.Source
	snk sink.Sink
	cpm *checkpoint.Manager
	log *log.Logger

	assigner *window.Assigner
	tracker  *watermark.Tracker
	store    *state.Store

	offsets map[int]*atomic.Int64

	cancel context.CancelFunc // CrashAfterEvents / StopAfterBatches 触发停机时使用

	commitMu sync.RWMutex // 写锁=检查点屏障；读锁=处理事件（保证无半更新）
	// procMu 串行化所有 worker 对共享状态（store + tracker 水位判定/推进）的访问。
	// 多个分区 worker 可并行读取/排队，但更新窗口状态与水位必须经过这把锁，
	// 否则一个 goroutine 的 IsLate 与另一个的 Observe 会产生竞态。
	procMu sync.Mutex

	pendingResMu  sync.Mutex
	pendingResult []model.Result
	pendingLate   []model.LateRecord
	ackedResults  map[string]struct{}
	ackedLate     map[string]struct{}

	mProcessed   atomic.Int64
	mDuplicate   atomic.Int64
	mLate        atomic.Int64
	mCommitted   atomic.Int64
	mCheckpoint  atomic.Int64
	mBackpress   atomic.Int64
	mBatches     atomic.Int64
	forceSave    atomic.Bool
	restoredFrom string

	wakeCh chan struct{}
}

// New 构造引擎。
func New(cfg Config, src source.Source, snk sink.Sink, cpm *checkpoint.Manager) *Engine {
	cfg.withDefaults()
	return &Engine{
		cfg: cfg, src: src, snk: snk, cpm: cpm,
		log:          log.Default(),
		assigner:     window.NewAssigner(cfg.WindowSize),
		tracker:      watermark.NewTracker(cfg.OutOfOrderness, cfg.AllowedLateness),
		store:        state.NewStore(),
		offsets:      make(map[int]*atomic.Int64),
		ackedResults: make(map[string]struct{}),
		ackedLate:    make(map[string]struct{}),
		wakeCh:       make(chan struct{}, 1),
	}
}

func (e *Engine) wake() {
	select {
	case e.wakeCh <- struct{}{}:
	default:
	}
}

// Run 启动处理循环直到数据源耗尽（或 ctx 取消），并自动从检查点恢复。
func (e *Engine) Run(ctx context.Context) error {
	parts := e.src.Partitions()
	for _, p := range parts {
		var off atomic.Int64
		e.offsets[p] = &off
	}
	e.tracker.RegisterPartitions(parts...)

	if err := e.recoverState(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.cancel = cancel

	// 协调器：周期性/按事件推进执行“定稿窗口 + 检查点屏障”。
	coordinatorDone := make(chan struct{})
	go func() {
		defer close(coordinatorDone)
		e.coordinatorLoop(runCtx)
	}()

	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			e.runPartition(runCtx, partition)
		}(p)
	}

	// 等待所有分区结束；外部取消或确定性故障注入都会终止运行，此时不做排空。
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()
	select {
	case <-allDone:
	case <-ctx.Done():
		wg.Wait()
		cancel()
		<-coordinatorDone
		return ctx.Err()
	case <-runCtx.Done():
		wg.Wait()
		<-coordinatorDone
		// 确定性故障注入 -> 错误；干净停机（StopAfterBatches 已先落检查点）-> nil。
		if e.cfg.CrashAfterEvents > 0 || e.cfg.CrashAfterBatches > 0 {
			return context.Canceled
		}
		return nil
	}
	if runCtx.Err() != nil {
		<-coordinatorDone
		if e.cfg.CrashAfterEvents > 0 || e.cfg.CrashAfterBatches > 0 {
			return context.Canceled
		}
		return nil
	}
	e.log.Printf("[engine] all partitions reached end-of-stream, advancing watermark to +Inf for drain")
	e.tracker.MarkComplete()
	e.wake()

	// 显式定稿所有剩余窗口并提交（独立 context，不被 runCtx 取消影响）。
	e.finalizeDueWindows()
	if err := e.commit(context.Background()); err != nil {
		cancel()
		<-coordinatorDone
		return err
	}
	cancel()
	<-coordinatorDone
	return nil
}

// runPartition 是单分区流水线：feeder 读取 -> 有界批次队列（背压）-> worker 处理。
func (e *Engine) runPartition(ctx context.Context, partition int) {
	type batch struct {
		events []model.Event
		next   int64
	}
	queue := make(chan batch, e.cfg.PendingQueueCap)

	// feeder：仅在队列有空位时读取下一批，消费速度受限时在此阻塞，
	// 待处理数据最多 = 队列容量 * BatchSize，绝不无界堆积。
	go func() {
		defer close(queue)
		offset := e.offsets[partition].Load()
		for {
			evs, next, err := e.src.Read(ctx, partition, offset, e.cfg.BatchSize)
			if err != nil {
				return
			}
			if len(evs) == 0 {
				return // 源末尾
			}
			// 队列满即达到资源上限：阻塞读取侧实现背压，并记录可观测事件。
			if len(queue) == cap(queue) {
				n := e.mBackpress.Add(1)
				e.log.Printf("[backpressure] partition=%d pending queue full (cap=%d), blocking feeder, totalBackpressureEvents=%d",
					partition, cap(queue), n)
			}
			select {
			case queue <- batch{events: evs, next: next}:
			case <-ctx.Done():
				return
			}
			offset = next
		}
	}()

	for b := range queue {
		// 模拟处理能力受限：慢于读取速度时，feeder 在有界队列上阻塞形成背压。
		if e.cfg.ProcessDelay > 0 {
			select {
			case <-time.After(e.cfg.ProcessDelay):
			case <-ctx.Done():
				return
			}
		}
		// 检查点屏障（RLock）+ 共享状态锁（procMu）：
		// 屏障保证与检查点互斥；procMu 保证多分区 worker 对 store/水位的
		// “判定+更新”序列不交错，杜绝竞态与半更新。
		e.commitMu.RLock()
		e.procMu.Lock()
		processed, dup, late := e.applyEvents(b.events)
		totalProcessed := e.mProcessed.Add(int64(processed))
		e.mDuplicate.Add(int64(dup))
		e.mLate.Add(int64(late))
		batches := e.mBatches.Add(1)
		e.offsets[partition].Store(b.next) // 位点在内存推进；尚未检查点持久化

		crashEvents := e.cfg.CrashAfterEvents > 0 && totalProcessed >= e.cfg.CrashAfterEvents
		crashBatches := e.cfg.CrashAfterBatches > 0 && batches >= e.cfg.CrashAfterBatches
		stop := e.cfg.StopAfterBatches > 0 && batches >= e.cfg.StopAfterBatches
		e.procMu.Unlock()

		if crashEvents || crashBatches {
			// 仍持屏障读锁 => 协调器不可能在此刻持久化本批位点/结果。
			e.commitMu.RUnlock()
			e.log.Printf("[crash] simulated crash: batch applied in memory (processed=%d batches=%d) but NOT checkpointed; will replay from last checkpoint offset",
				totalProcessed, batches)
			e.cancel()
			return
		}
		if stop {
			e.commitMu.RUnlock()
			e.log.Printf("[stop] clean stop requested after %d batches", batches)
			e.forceSave.Store(true) // 停机前强制持久化活动窗口状态与位点
			e.cancel()              // 协调器检测到取消后执行“定稿+检查点”，再退出
			return
		}
		e.commitMu.RUnlock()
		e.wake() // 通知协调器可能有关闭窗口/水位推进
	}
}

// applyEvents 顺序处理一批事件；乱序不改变窗口归属，重复不累加，
// 超出允许迟到范围的事件进入待旁路集合（绝不静默丢弃）。
// 调用方必须同时持有 commitMu 读锁与 procMu。
func (e *Engine) applyEvents(events []model.Event) (processed, dup, late int) {
	for _, ev := range events {
		isLate, wm := e.tracker.IsLate(ev.EventTime)
		w := e.assigner.Assign(ev.Key, ev.EventTime)
		wid := window.WindowID(w)

		// 1) 去重优先于迟到判定：重复事件无论到达多晚都不得重复计入，
		//    也不进入迟到旁路（它不是一条新迟到数据）。
		if e.store.IsKnownEvent(ev.ID) {
			dup++
			e.log.Printf("[dup  ] event=%s key=%s eventTime=%s window=%s -> duplicate, not counted again",
				ev.ID, ev.Key, ev.EventTime.Format(time.RFC3339Nano), wid)
			continue
		}

		// 2) 超出允许迟到范围的新事件进入旁路（绝不静默丢弃，也不回改结果）。
		if isLate {
			reason := model.LateExceedsAllowedLateness
			if e.store.IsCommitted(wid) {
				reason = model.LateWindowAlreadyCommitted
			}
			rec := model.LateRecord{
				Event: ev, Reason: reason, Watermark: wm,
				AllowedLateness: e.cfg.AllowedLateness, SideOutputAt: time.Now(),
			}
			e.addPendingLate(rec)
			late++
			e.log.Printf("[late ] event=%s key=%s eventTime=%s watermark=%s allowedLateness=%s -> side-output(%s)",
				ev.ID, ev.Key, ev.EventTime.Format(time.RFC3339Nano), wm.Format(time.RFC3339Nano),
				e.cfg.AllowedLateness, reason)
			continue
		}

		// 3) 正常/乱序但允许范围内的事件累加到窗口（Upsert 内部再做窗口级去重）。
		updated, dropped := e.store.Upsert(ev, w)
		if dropped {
			dup++
			e.log.Printf("[dup  ] event=%s key=%s eventTime=%s window=%s -> duplicate, not counted again",
				ev.ID, ev.Key, ev.EventTime.Format(time.RFC3339Nano), wid)
		} else {
			processed++
			e.log.Printf("[event] event=%s key=%s eventTime=%s partition=%d value=%d -> window=%s counted=%v",
				ev.ID, ev.Key, ev.EventTime.Format(time.RFC3339Nano), ev.Partition, ev.Value, wid, updated)
		}
		// 水位线仅由被接受事件的事件时间推进（迟到事件不会使水位后退）。
		e.tracker.Observe(ev.Partition, ev.EventTime)
	}
	return
}

func (e *Engine) addPendingLate(r model.LateRecord) {
	e.pendingResMu.Lock()
	defer e.pendingResMu.Unlock()
	for _, x := range e.pendingLate {
		if x.Event.ID == r.Event.ID {
			return
		}
	}
	e.pendingLate = append(e.pendingLate, r)
}

// coordinatorLoop 在水位推进/周期触发时定稿到期窗口，并按“先持久化、后写出”
// 的顺序做检查点提交，保证恰好一次。
func (e *Engine) coordinatorLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.CheckpointEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// 干净停机：定稿到期窗口并强制保存一个含活动状态/位点的检查点。
			// 崩溃注入/外部取消则不做任何提交（未提交内容留给恢复重放）。
			if e.cfg.StopAfterBatches > 0 &&
				e.mBatches.Load() >= e.cfg.StopAfterBatches {
				e.finalizeDueWindows()
				if err := e.commit(context.Background()); err != nil {
					e.log.Printf("[checkpoint] clean-stop commit error: %v", err)
				}
			}
			return
		case <-ticker.C:
			if err := e.commit(ctx); err != nil && ctx.Err() == nil {
				e.log.Printf("[checkpoint] commit error: %v", err)
			}
		case <-e.wakeCh:
			// 事件驱动：只有确实定稿了窗口/旁路时才立即提交；
			// 普通水位推进留给周期性提交，避免高频落盘。
			if e.finalizeDueWindows() {
				if err := e.commit(ctx); err != nil && ctx.Err() == nil {
					e.log.Printf("[checkpoint] commit error: %v", err)
				}
			}
		}
	}
}

// finalizeDueWindows 在检查点屏障内定稿到期窗口，返回是否产生了待写出内容。
func (e *Engine) finalizeDueWindows() bool {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.procMu.Lock()
	defer e.procMu.Unlock()
	wm := e.tracker.Watermark()
	if wm.IsZero() {
		return false
	}
	closed := e.store.FinalizeWindows(wm, e.cfg.AllowedLateness)
	if len(closed) == 0 {
		return false
	}
	// 确定性排序：窗口开始时间，再按键。
	sort.Slice(closed, func(i, j int) bool {
		if !closed[i].Start.Equal(closed[j].Start) {
			return closed[i].Start.Before(closed[j].Start)
		}
		return closed[i].Key < closed[j].Key
	})
	now := time.Now()
	e.pendingResMu.Lock()
	for _, c := range closed {
		res := model.Result{
			ID: c.WindowID, Key: c.Key, Start: c.Start, End: c.End,
			Count: c.Count, Sum: c.Sum, EmittedAt: now,
		}
		e.log.Printf("[window] finalize window=%s range=[%s,%s) count=%d sum=%d watermark=%s",
			c.WindowID, c.Start.Format(time.RFC3339Nano), c.End.Format(time.RFC3339Nano),
			c.Count, c.Sum, wm.Format(time.RFC3339Nano))
		e.pendingResult = append(e.pendingResult, res)
	}
	hasLate := len(e.pendingLate) > 0
	e.pendingResMu.Unlock()
	e.mCommitted.Add(int64(len(closed)))
	return len(closed) > 0 || hasLate
}

// commit 执行一次检查点屏障下的完整提交。
func (e *Engine) commit(ctx context.Context) error {
	e.commitMu.Lock() // 屏障：等待所有在处理批次完成，并阻止新处理
	defer e.commitMu.Unlock()
	e.procMu.Lock() // 串行化对 store/水位/位点的读取
	defer e.procMu.Unlock()

	e.pendingResMu.Lock()
	pendingRes := append([]model.Result(nil), e.pendingResult...)
	pendingLate := append([]model.LateRecord(nil), e.pendingLate...)
	e.pendingResMu.Unlock()

	dirty := len(pendingRes) > 0 || len(pendingLate) > 0
	force := e.forceSave.Swap(false)
	if !dirty && !force && e.mCheckpoint.Load() > 0 {
		// 无新增待写出内容、无强制要求且此前已有检查点：跳过无谓落盘。
		return nil
	}

	wins, seen, _ := e.store.Snapshot()
	off := make(map[int]int64, len(e.offsets))
	for p, a := range e.offsets {
		off[p] = a.Load()
	}
	cpID := time.Now().UTC().Format("20060102T150405.000000000")
	st := &checkpoint.State{
		ID: cpID, CreatedAt: time.Now().UTC(),
		Watermark:      e.tracker.Watermark(),
		PartitionTimes: e.tracker.Snapshot(),
		Offsets:        off,
		Windows:        wins,
		SeenIDs:        seen,
		AckedResultIDs: mapKeys(e.ackedResults),
		AckedLateIDs:   mapKeys(e.ackedLate),
	}
	st.EncodePending(pendingRes, pendingLate)

	// 1) 先原子持久化检查点（覆盖窗口状态、读取位点、待写出结果）。
	if err := e.cpm.Save(ctx, st); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	e.mCheckpoint.Add(1)
	e.log.Printf("[checkpoint] committed id=%s windows=%d offsets=%v pendingResults=%d pendingLate=%d watermark=%s",
		cpID, len(wins), off, len(pendingRes), len(pendingLate),
		e.tracker.Watermark().Format(time.RFC3339Nano))

	// 2) 检查点落盘后才写出；sink 按 ID 幂等，因此崩溃重放不会重复写出。
	for _, r := range pendingRes {
		existed, err := e.snk.WriteResult(ctx, r)
		if err != nil {
			return fmt.Errorf("write result: %w", err)
		}
		e.ackedResults[r.ID] = struct{}{}
		if existed {
			e.log.Printf("[sink ] result %s already present (idempotent replay)", r.ID)
		}
	}
	for _, r := range pendingLate {
		existed, err := e.snk.WriteLate(ctx, r)
		if err != nil {
			return fmt.Errorf("write late: %w", err)
		}
		e.ackedLate[r.Event.ID] = struct{}{}
		if existed {
			e.log.Printf("[sink ] late %s already present (idempotent replay)", r.Event.ID)
		}
	}

	// 3) 已确认持久化的待写出项才从内存清除（未确认项保留，崩溃后重放）。
	e.pendingResMu.Lock()
	e.pendingResult = e.pendingResult[len(pendingRes):]
	e.pendingLate = e.pendingLate[len(pendingLate):]
	e.pendingResMu.Unlock()
	return nil
}

// recoverState 加载并校验检查点；任何损坏/版本不符都明确报错，绝不静默继续。
func (e *Engine) recoverState(ctx context.Context) error {
	st, err := e.cpm.Load(ctx)
	if err != nil {
		if checkpoint.IsNoCheckpoint(err) {
			e.log.Printf("[recover] no checkpoint found, starting fresh")
			return nil
		}
		// 明确区分：清单损坏 / 数据损坏 / 版本不兼容 / 悬空中间产物。
		return fmt.Errorf("refuse to start: %w", err)
	}

	wins, seen := st.Windows, st.SeenIDs
	e.store.Restore(wins, seen, committedList(st))
	e.tracker.Restore(st.PartitionTimes)
	for p, off := range st.Offsets {
		if a, ok := e.offsets[p]; ok {
			a.Store(off)
		}
	}
	pendingRes, pendingLate := st.DecodePending()
	e.pendingResult = pendingRes
	e.pendingLate = pendingLate
	for _, id := range st.AckedResultIDs {
		e.ackedResults[id] = struct{}{}
	}
	for _, id := range st.AckedLateIDs {
		e.ackedLate[id] = struct{}{}
	}
	e.restoredFrom = st.ID

	e.log.Printf("[recover] restored from checkpoint id=%s watermark=%s offsets=%v activeWindows=%d committedWindows=%d pendingResults=%d pendingLate=%d",
		st.ID, st.Watermark.Format(time.RFC3339Nano), st.Offsets,
		len(wins), len(st.AckedResultIDs), len(pendingRes), len(pendingLate))

	// 恢复依据落地后，先把检查点中“已定稿待写出”的结果幂等补写到 sink。
	for _, r := range pendingRes {
		existed, err := e.snk.WriteResult(ctx, r)
		if err != nil {
			return fmt.Errorf("recover write result: %w", err)
		}
		e.ackedResults[r.ID] = struct{}{}
		e.log.Printf("[recover] replay pending result %s existed=%v", r.ID, existed)
	}
	for _, r := range pendingLate {
		existed, err := e.snk.WriteLate(ctx, r)
		if err != nil {
			return fmt.Errorf("recover write late: %w", err)
		}
		e.ackedLate[r.Event.ID] = struct{}{}
		e.log.Printf("[recover] replay pending late %s existed=%v", r.Event.ID, existed)
	}
	return nil
}

// SnapshotMetrics 返回当前指标快照（背压触发次数通过 NotifyBackpressure 累计）。
func (e *Engine) SnapshotMetrics() Metrics {
	return Metrics{
		Processed:        e.mProcessed.Load(),
		DuplicateDropped: e.mDuplicate.Load(),
		LateSideOutput:   e.mLate.Load(),
		WindowsCommitted: e.mCommitted.Load(),
		Checkpoints:      e.mCheckpoint.Load(),
		BackPressure:     e.mBackpress.Load(),
		RestoredFrom:     e.restoredFrom,
		Watermark:        e.tracker.Watermark(),
	}
}

func mapKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
