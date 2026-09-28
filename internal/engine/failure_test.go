package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakySink 在 FileSink 外面套一层故障注入：
//   - failNext > 0：接下来 failNext 次 Commit 直接返回错误（模拟磁盘写失败）；
//   - partialThenFail：在返回错误之前，先向结果文件追加一个不完整 JSON 行，
//     模拟“写出一部分后进程被杀/写失败”留下的残缺尾行。
type flakySink struct {
	inner           *FileSink
	mu              sync.Mutex
	failNext        int32
	failTotal       int32
	partialThenFail bool
	failOnceOnLates bool // 在下一个“携带迟到记录”的批次注入一次失败
	commits         atomic.Int32
}

func newFlakySink(rig testRig) *flakySink {
	fs := NewFileSink(rig.resultsPath, rig.latePath)
	return &flakySink{inner: fs}
}

func (f *flakySink) Fail(n int) { atomic.AddInt32(&f.failNext, int32(n)) }
func (f *flakySink) FailPartial(n int) {
	f.mu.Lock()
	f.partialThenFail = true
	f.mu.Unlock()
	atomic.AddInt32(&f.failNext, int32(n))
}
func (f *flakySink) FailWhenLates() {
	f.mu.Lock()
	f.failOnceOnLates = true
	f.mu.Unlock()
}
func (f *flakySink) CommitCalls() int32 { return f.commits.Load() }
func (f *flakySink) FailCalls() int32   { return atomic.LoadInt32(&f.failTotal) }

func (f *flakySink) Commit(cpID uint64, results []WindowResult, lates []LateRecord) error {
	f.commits.Add(1)
	f.mu.Lock()
	onLates := f.failOnceOnLates
	f.mu.Unlock()
	if onLates && len(lates) > 0 {
		f.mu.Lock()
		f.failOnceOnLates = false
		f.mu.Unlock()
		atomic.AddInt32(&f.failTotal, 1)
		return fmt.Errorf("injected disk write failure on batch with lates at cp=%d", cpID)
	}
	if atomic.AddInt32(&f.failNext, -1) >= 0 {
		atomic.AddInt32(&f.failTotal, 1)
		f.mu.Lock()
		partial := f.partialThenFail
		f.mu.Unlock()
		if partial && len(results) > 0 {
			if err := appendTornLine(f.inner.resultsPath); err != nil {
				return err
			}
		}
		return fmt.Errorf("injected disk write failure at cp=%d", cpID)
	}
	return f.inner.Commit(cpID, results, lates)
}

func (f *flakySink) TruncateTo(cpID uint64) error { return f.inner.TruncateTo(cpID) }

// appendTornLine 直接追加一段没有换行、无法解析的残缺 JSON（绕过幂等重写）。
func appendTornLine(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(`{"key":"torn","window_start":0,"count":99,"sum":99,"checkpoint_id":1`)
	return err
}

func runEngineWithSink(t *testing.T, rig testRig, cfg Config, sink Sink, ctx context.Context) (Stats, error) {
	t.Helper()
	src := NewFileSource(rig.inputPath)
	defer src.Close()
	store := NewCheckpointStore(rig.cpPath)
	e := NewEngine(cfg, src, sink, store)
	err := e.Run(ctx)
	return e.Stats(), err
}

func resultIndex(t *testing.T, rig testRig) map[string]WindowResult {
	t.Helper()
	out := map[string]WindowResult{}
	for _, r := range rig.results(t) {
		k := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
		if _, dup := out[k]; dup {
			t.Fatalf("duplicate window output: %s", k)
		}
		out[k] = r
	}
	return out
}

func assertSameResults(t *testing.T, got, want map[string]WindowResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("window count mismatch: got=%d want=%d\ngot=%v\nwant=%v", len(got), len(want), got, want)
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing window %s after failure handling", k)
		}
		if g.Count != w.Count || math.Abs(g.Sum-w.Sum) > 1e-9 {
			t.Fatalf("window %s: got count=%d sum=%.2f, want count=%d sum=%.2f", k, g.Count, g.Sum, w.Count, w.Sum)
		}
	}
}

// TestSinkFailureRetryNoDuplicates：写出阶段连续失败后，在同一检查点内重试成功，
// 结果必须与一次成功跑完完全一致且无重复写出，重试/重放在指标与日志中可见。
func TestSinkFailureRetryNoDuplicates(t *testing.T) {
	events := []Event{
		{ID: "e1", Key: "a", EventTime: 10_000, Value: 1},
		{ID: "e2", Key: "a", EventTime: 20_000, Value: 2},
		{ID: "e3", Key: "a", EventTime: 30_000, Value: 3},
		{ID: "e4", Key: "a", EventTime: 200_000, Value: 4}, // 推进水位，提交窗口
		{ID: "e5", Key: "a", EventTime: 300_000, Value: 5},
	}
	cfg := Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 1_000,
		NumShards: 1, Workers: 1, QueueCapacity: 4, CheckpointEvery: 3,
		MaxCommitRetries: 5, CommitRetryBackoff: time.Millisecond,
		Logger: testLogger(t),
	}

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	want := resultIndex(t, base)

	flaky := newRig(t)
	writeEvents(t, flaky.inputPath, events)
	sink := newFlakySink(flaky)
	sink.Fail(2) // 前两次写出失败（覆盖首个窗口提交），第三次成功
	st, err := runEngineWithSink(t, flaky, cfg, sink, context.Background())
	if err != nil {
		t.Fatalf("engine should survive transient sink failures: %v", err)
	}
	t.Logf("stats: retries=%d replay_results=%d fatal=%d checkpoints=%d",
		st.SinkRetries, st.SinkReplayResults, st.SinkFatal, st.Checkpoints)
	if st.SinkRetries < 2 {
		t.Fatalf("expected >=2 sink retries, got %d", st.SinkRetries)
	}
	if st.SinkReplayResults == 0 {
		t.Fatal("expected observable replayed result count")
	}
	if st.SinkFatal != 0 {
		t.Fatalf("transient failures must not be fatal, got fatal=%d", st.SinkFatal)
	}
	if sink.FailCalls() != 2 {
		t.Fatalf("expected 2 failed commit attempts, got %d", sink.FailCalls())
	}
	assertSameResults(t, resultIndex(t, flaky), want)
}

// TestSinkPartialWriteThenRestart：写出一部分后失败（残留完整/残缺行），
// 紧接着继续跑（同代重试）必须补回且不重复；残缺尾行被剔除。
func TestSinkPartialWriteReplayIdempotent(t *testing.T) {
	events := []Event{
		{ID: "e1", Key: "a", EventTime: 10_000, Value: 1},
		{ID: "e2", Key: "a", EventTime: 20_000, Value: 2},
		{ID: "e3", Key: "a", EventTime: 30_000, Value: 3},
		{ID: "e4", Key: "a", EventTime: 200_000, Value: 4},
		{ID: "e5", Key: "a", EventTime: 300_000, Value: 5},
	}
	cfg := Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 1_000,
		NumShards: 1, Workers: 1, QueueCapacity: 4, CheckpointEvery: 3,
		MaxCommitRetries: 5, CommitRetryBackoff: time.Millisecond,
		Logger: testLogger(t),
	}
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	want := resultIndex(t, base)

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	sink := newFlakySink(rig)
	sink.FailPartial(1) // 第一次提交：留下残缺行后失败；随后同代重试成功
	if _, err := runEngineWithSink(t, rig, cfg, sink, context.Background()); err != nil {
		t.Fatalf("engine should recover from partial write: %v", err)
	}
	got := resultIndex(t, rig)
	assertSameResults(t, got, want)
	// 残留的伪造 count=99 窗口绝不能出现在最终结果里。
	for k, r := range got {
		if r.Count == 99 {
			t.Fatalf("torn/partial record survived replay: %s=%+v", k, r)
		}
	}
	// 结果文件中也不能再有残缺行。
	data, err := os.ReadFile(rig.resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if i := indexUnparsableTail(data); i >= 0 {
		t.Fatalf("results file still contains torn trailing bytes at %d", i)
	}
	t.Log("partial write replayed idempotently; torn tail discarded")
}

func indexUnparsableTail(data []byte) int {
	// 校验最后一行必须能解析为 WindowResult（否则即残缺尾行残留）。
	if len(data) == 0 {
		return -1
	}
	i := len(data) - 1
	for i >= 0 && data[i] != '\n' {
		i--
	}
	line := data[i+1:]
	if len(line) == 0 {
		return -1
	}
	var r WindowResult
	if json.Unmarshal(line, &r) != nil {
		return i + 1
	}
	return -1
}

// killSink 在第 killAt 次 Commit 时“写完输出后永久停住”：底层文件已写入并 fsync，
// 但检查点尚未保存。测试用它精确制造 commit/checkpoint 之间的“被杀”时刻；
// 阻塞期间不持有文件句柄或引擎锁，测试随后直接以新引擎模拟新进程恢复。
type killSink struct {
	inner  *FileSink
	calls  atomic.Int32
	killAt int32
	killed chan struct{}
	once   sync.Once
}

func newKillSink(rig testRig, killAt int32) *killSink {
	return &killSink{
		inner:  NewFileSink(rig.resultsPath, rig.latePath),
		killAt: killAt, killed: make(chan struct{}),
	}
}

func (k *killSink) Commit(cpID uint64, results []WindowResult, lates []LateRecord) error {
	if err := k.inner.Commit(cpID, results, lates); err != nil {
		return err
	}
	if k.calls.Add(1) == k.killAt {
		k.once.Do(func() { close(k.killed) })
		select {} // 模拟进程被杀：永不返回，检查点因此永不保存
	}
	return nil
}

func (k *killSink) TruncateTo(cpID uint64) error { return k.inner.TruncateTo(cpID) }

// TestCrashAfterCommitReplaysUnconfirmed：写出成功但检查点落盘前被杀，
// 重启后未确认输出被回滚并重放，最终结果与基线一致、无重复。
func TestCrashAfterCommitReplaysUnconfirmed(t *testing.T) {
	var events []Event
	keys := []string{"a", "b"}
	for i := 0; i < 120; i++ {
		events = append(events, Event{
			ID:        fmt.Sprintf("k-%03d", i),
			Key:       keys[i%2],
			EventTime: int64(i)*1_000 + int64((i*13)%10_000) - 5_000,
			Value:     float64(i%7) + 0.25,
		})
	}
	cfg := Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 5_000,
		NumShards: 1, Workers: 1, QueueCapacity: 8, CheckpointEvery: 20,
		MaxCommitRetries: 1, // 不做同进程重试：直接当“崩溃”，靠重启重放
		Logger:           testLogger(t),
	}
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	want := resultIndex(t, base)

	// 崩溃运行：不设周期性检查点，使第一次 sink.Commit 恰好就是源耗尽后的最终提交，
	// kill 点之前磁盘上不会有任何已保存代数。
	crashCfg := cfg
	crashCfg.CheckpointEvery = 1 << 30
	crash := newRig(t)
	writeEvents(t, crash.inputPath, events)
	sink := newKillSink(crash, 1)
	go func() {
		_, _ = runEngineWithSink(t, crash, crashCfg, sink, context.Background())
	}()
	select {
	case <-sink.killed:
		// 此刻：最终输出已落盘、检查点尚未保存。
	case <-time.After(5 * time.Second):
		t.Fatal("kill point never reached")
	}
	// 崩溃现场断言：sink 里已有未确认输出，而检查点存储没有任何代数。
	if n := len(crash.results(t)); n == 0 {
		t.Fatal("expected uncommitted sink output present at crash site")
	} else {
		t.Logf("uncommitted results present at kill point: %d (checkpoint not yet saved)", n)
	}
	if ids, _ := NewCheckpointStore(crash.cpPath).List(); len(ids) != 0 {
		t.Fatalf("no checkpoint generation should exist at kill point, got %v", ids)
	}

	// 重启（新进程）：恢复 -> 找不到检查点则清空未确认输出从头跑；
	// 若已有更早检查点则 TruncateTo 回滚未确认输出并从位点重放。
	st2, err := crash.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatalf("restart failed: %v", err)
	}
	t.Logf("restarted run: checkpoints=%d", st2.Checkpoints)
	assertSameResults(t, resultIndex(t, crash), want)
	if len(crash.lates(t)) != len(base.lates(t)) {
		t.Fatalf("late records mismatch after replay: crash=%d base=%d",
			len(crash.lates(t)), len(base.lates(t)))
	}
}

// TestLateRecordsReplayedOnSinkFailure：含迟到记录的检查点写出失败时，
// 迟到记录随同代重放，最终恰好一份且原因完整。
func TestLateRecordsReplayedOnSinkFailure(t *testing.T) {
	events := []Event{
		{ID: "l1", Key: "a", EventTime: 30_000, Value: 1},
		{ID: "l2", Key: "a", EventTime: 100_000, Value: 2}, // 水位推进
		{ID: "l3", Key: "a", EventTime: 40_000, Value: 9},  // 迟到
		{ID: "l4", Key: "a", EventTime: 50_000, Value: 8},  // 迟到
	}
	cfg := Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 5_000,
		NumShards: 1, Workers: 1, QueueCapacity: 4, CheckpointEvery: 1,
		MaxCommitRetries: 4, CommitRetryBackoff: time.Millisecond,
		Logger: testLogger(t),
	}
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	sink := newFlakySink(rig)
	sink.FailWhenLates() // 携带迟到记录的那次提交失败，随后同代重试成功
	st, err := runEngineWithSink(t, rig, cfg, sink, context.Background())
	if err != nil {
		t.Fatalf("transient failure should be retried: %v", err)
	}
	t.Logf("retries=%d replay_lates=%d", st.SinkRetries, st.SinkReplayLates)
	if st.SinkReplayLates == 0 || st.SinkRetries == 0 {
		t.Fatalf("late batch retry should be observable: retries=%d replay_lates=%d",
			st.SinkRetries, st.SinkReplayLates)
	}
	gotLates := rig.lates(t)
	if len(gotLates) != len(base.lates(t)) {
		t.Fatalf("late count: got=%d want=%d", len(gotLates), len(base.lates(t)))
	}
	seen := map[string]int{}
	for _, l := range gotLates {
		if l.Reason == "" || l.CheckpointID == 0 {
			t.Fatalf("late record missing reason/checkpoint id: %+v", l)
		}
		seen[l.Event.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("late record %s appeared %d times, want exactly 1", id, n)
		}
	}
	assertSameResults(t, resultIndex(t, rig), resultIndex(t, base))
}

// hardFailSink 永远写出失败，用于演练“写出持续不可用”。
type hardFailSink struct {
	inner *FileSink
}

func newHardFailSink(rig testRig) *hardFailSink {
	return &hardFailSink{inner: NewFileSink(rig.resultsPath, rig.latePath)}
}

func (h *hardFailSink) Commit(uint64, []WindowResult, []LateRecord) error {
	return fmt.Errorf("disk offline (injected permanent failure)")
}
func (h *hardFailSink) TruncateTo(uint64) error { return nil }

func hardLimitEvents(nLate int) []Event {
	// 少量早期事件 -> 一条极晚事件把水位线推高 -> 其后 nLate 条旧时间事件，
	// 全部确定性成为迟到（event_time <= watermark），从而稳定打满 pendingLates。
	events := []Event{
		{ID: "h-early-1", Key: "a", EventTime: 100, Value: 1},
		{ID: "h-early-2", Key: "a", EventTime: 200, Value: 1},
		{ID: "h-far", Key: "a", EventTime: 1_000_000, Value: 1},
	}
	for j := 0; j < nLate; j++ {
		events = append(events, Event{
			ID: fmt.Sprintf("h-late-%02d", j), Key: "a",
			EventTime: int64(j) * 100, Value: 1,
		})
	}
	return events
}

// TestPendingLimitRejectsWhenSinkDown：写出持续失败时，待写出积压到上限后
// 引擎必须有可见的上限计数，并明确拒绝（返回 ErrSinkUnavailable 停机），
// 不能让 pending 无限增长。
func TestPendingLimitRejectsWhenSinkDown(t *testing.T) {
	events := hardLimitEvents(24)
	cfg := Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 1,
		NumShards: 1, Workers: 1, QueueCapacity: 2, CheckpointEvery: 2,
		MaxCheckpoints:  2,
		MaxPendingLates: 4, MaxPendingResults: 4,
		MaxCommitRetries:   2,
		CommitRetryBackoff: time.Millisecond,
		Logger:             testLogger(t),
	}
	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	sink := newHardFailSink(rig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := runEngineWithSink(t, rig, cfg, sink, ctx)
	if err == nil {
		t.Fatal("expected engine to stop explicitly when sink stays down")
	}
	if !errors.Is(err, ErrSinkUnavailable) {
		t.Fatalf("expected ErrSinkUnavailable, got %v", err)
	}
	t.Logf("rejected with stats: pending_hits=%d fatal=%d retries=%d processed=%d late=%d",
		st.PendingLimitHits, st.SinkFatal, st.SinkRetries, st.Processed, st.Late)
	if st.SinkFatal != 1 {
		t.Fatalf("expected exactly 1 sink-fatal mark, got %d", st.SinkFatal)
	}
	if st.PendingLimitHits == 0 {
		t.Fatal("expected observable pending-limit-hit counter")
	}
	// 上限触发后引擎应很快停机：不可能把全部输入处理完。
	if st.Processed+st.Late >= uint64(len(events)) {
		t.Fatalf("engine should stop before draining input after limit: processed=%d late=%d",
			st.Processed, st.Late)
	}
}

// TestPendingLimitFlushesAndRecovers：上限触发的强制写出在 sink 恢复后成功，
// 引擎继续跑完且结果正确，上限计数可见。
func TestPendingLimitFlushesAndRecovers(t *testing.T) {
	events := hardLimitEvents(24)
	cfg := Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 1,
		NumShards: 1, Workers: 1, QueueCapacity: 2, CheckpointEvery: 4,
		MaxCheckpoints:  3,
		MaxPendingLates: 5, MaxPendingResults: 1000,
		MaxCommitRetries:   3,
		CommitRetryBackoff: time.Millisecond,
		Logger:             testLogger(t),
	}
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(base.lates(t)); n != 24 {
		t.Fatalf("baseline should mark 24 late, got %d", n)
	}
	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	sink := newFlakySink(rig)
	sink.Fail(2) // 前两次写出失败 -> 积压 -> 撞上限强制 flush -> 恢复后成功
	st, err := runEngineWithSink(t, rig, cfg, sink, context.Background())
	if err != nil {
		t.Fatalf("engine should complete after sink recovers: %v", err)
	}
	t.Logf("stats: pending_hits=%d retries=%d replay_lates=%d fatal=%d",
		st.PendingLimitHits, st.SinkRetries, st.SinkReplayLates, st.SinkFatal)
	if st.SinkFatal != 0 {
		t.Fatalf("transient failures must not be fatal, fatal=%d", st.SinkFatal)
	}
	if st.PendingLimitHits == 0 {
		t.Fatal("expected the pending cap to force a flush at least once")
	}
	if len(rig.lates(t)) != len(base.lates(t)) {
		t.Fatalf("late count: got=%d want=%d", len(rig.lates(t)), len(base.lates(t)))
	}
	assertSameResults(t, resultIndex(t, rig), resultIndex(t, base))
}
