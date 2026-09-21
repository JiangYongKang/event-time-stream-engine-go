package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/checkpoint"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/sink"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/source"
)

func et(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

func ev(id, key string, sec int64, val int64, partition int) model.Event {
	return model.Event{ID: id, Key: key, EventTime: et(sec), Value: val, Partition: partition}
}

func baseCfg() Config {
	return Config{
		WindowSize:      time.Minute,
		OutOfOrderness:  0,
		AllowedLateness: 30 * time.Second,
		BatchSize:       2,
		PendingQueueCap: 4,
		CheckpointEvery: 15 * time.Millisecond,
	}
}

func runEngine(t *testing.T, dir string, src source.Source, snk sink.Sink, cfg Config) *Engine {
	t.Helper()
	eng := New(cfg, src, snk, checkpoint.NewManager(dir))
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("engine run: %v", err)
	}
	return eng
}

func findResult(rs []model.Result, startSec int64, key string) (model.Result, bool) {
	for _, r := range rs {
		if r.Key == key && r.Start.Unix() == startSec {
			return r, true
		}
	}
	return model.Result{}, false
}

// TestOutOfOrderWindowAssignment 乱序到达不改变窗口归属，允许迟到内事件更新已定稿前的窗口。
func TestOutOfOrderWindowAssignment(t *testing.T) {
	t.Log("== 乱序：事件按 2,65,5 到达，均落在各自事件时间窗口，w0 不被 wm 提前定稿 ==")
	dir := t.TempDir()
	events := []model.Event{
		ev("a", "k", 2, 1, 0),
		ev("b", "k", 65, 10, 0),
		ev("c", "k", 5, 100, 0), // 乱序：wm=65，5+30=35<65 -> 超范围（这里用允许迟到 60s 改为接受）
	}
	cfg := baseCfg()
	cfg.AllowedLateness = 60 * time.Second // 5+60>=65，w0(end60) 在 end+60=120 才定稿
	src := source.NewMemorySource(map[int][]model.Event{0: events})
	snk := sink.NewMemorySink()
	runEngine(t, dir, src, snk, cfg)

	r0, ok := findResult(snk.Results(), 0, "k")
	if !ok || r0.Count != 2 || r0.Sum != 101 {
		t.Fatalf("window[0,60) must include a+c regardless of arrival order: %+v results=%+v", r0, snk.Results())
	}
	r1, ok := findResult(snk.Results(), 60, "k")
	if !ok || r1.Count != 1 || r1.Sum != 10 {
		t.Fatalf("window[60,120) must be b: %+v", r1)
	}
	t.Logf("window0 count=%d sum=%d; window1 count=%d sum=%d", r0.Count, r0.Sum, r1.Count, r1.Sum)
}

// TestDuplicateNotCountedTwice 同 ID 重复（含跨批次、迟到形式）绝不重复计入。
func TestDuplicateNotCountedTwice(t *testing.T) {
	t.Log("== 重复：同 ID 事件以乱序、重复发送形式多次到达 ==")
	dir := t.TempDir()
	events := []model.Event{
		ev("a", "k", 2, 1, 0),
		ev("a", "k", 2, 1, 0),
		ev("b", "k", 10, 2, 0),
	}
	src := source.NewMemorySource(map[int][]model.Event{0: events})
	snk := sink.NewMemorySink()
	eng := runEngine(t, dir, src, snk, baseCfg())
	r, _ := findResult(snk.Results(), 0, "k")
	if r.Count != 2 || r.Sum != 3 {
		t.Fatalf("duplicate a must count once: %+v", r)
	}
	m := eng.SnapshotMetrics()
	if m.DuplicateDropped < 1 {
		t.Fatalf("duplicate metric must be >=1: %+v", m)
	}
	t.Logf("window count=%d sum=%d duplicateDropped=%d", r.Count, r.Sum, m.DuplicateDropped)
}

// TestLateRecordsSideOutput 超允许迟到范围的记录必须明确旁路，且已提交窗口不被回改。
func TestLateRecordsSideOutput(t *testing.T) {
	t.Log("== 迟到：wm 越过窗口关闭线后到达的事件进入 side output，原因可区分 ==")
	dir := t.TempDir()
	events := []model.Event{
		ev("a", "k", 2, 1, 0),
		ev("b", "k", 95, 10, 0), // wm=95：w0(end60+30=90) 定稿
		ev("c", "k", 5, 100, 0), // 5+30=35<95 -> 迟到，且窗口已提交
	}
	cfg := baseCfg()
	cfg.BatchSize = 1 // 让 b 先把 wm 推上去
	src := source.NewMemorySource(map[int][]model.Event{0: events})
	snk := sink.NewMemorySink()
	runEngine(t, dir, src, snk, cfg)

	late := snk.Late()
	if len(late) != 1 || late[0].Event.ID != "c" {
		t.Fatalf("exactly c must be side-output: %+v", late)
	}
	if late[0].Reason != model.LateWindowAlreadyCommitted && late[0].Reason != model.LateExceedsAllowedLateness {
		t.Fatalf("late reason must be explicit, got %s", late[0].Reason)
	}
	t.Logf("side-output event=%s reason=%s watermark=%s", late[0].Event.ID, late[0].Reason, late[0].Watermark)
	// 已提交窗口结果不得被回改。
	r, _ := findResult(snk.Results(), 0, "k")
	if r.Sum != 1 || r.Count != 1 {
		t.Fatalf("committed window must not change: %+v", r)
	}
}

// TestCrashReplayExactlyOnce 崩溃于“批次已处理但未提交检查点”，恢复后重放，结果恰好一次。
func TestCrashReplayExactlyOnce(t *testing.T) {
	t.Log("== 崩溃重放：第 1 批次处理后崩溃（位点未持久化），重启后从位点 0 重放 ==")
	dir := t.TempDir()
	events := []model.Event{
		ev("a", "k", 2, 1, 0),
		ev("b", "k", 8, 2, 0),
		ev("c", "k", 200, 3, 0),
	}
	src := source.NewMemorySource(map[int][]model.Event{0: events})
	snk := sink.NewMemorySink()

	cfg1 := baseCfg()
	cfg1.CrashAfterBatches = 1
	cfg1.CheckpointEvery = time.Hour // 崩溃前不落检查点
	eng1 := New(cfg1, src, snk, checkpoint.NewManager(dir))
	err := eng1.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected simulated crash, got %v", err)
	}
	t.Logf("stage1 metrics: %+v; sink results so far=%d", eng1.SnapshotMetrics(), len(snk.Results()))
	if len(snk.Results()) != 0 {
		t.Fatal("nothing must be emitted before a committed checkpoint")
	}

	// 全新引擎从空检查点恢复，从头重放；同 ID 去重保证幂等。
	eng2 := runEngine(t, dir, source.NewMemorySource(map[int][]model.Event{0: events}), snk, baseCfg())
	r0, _ := findResult(snk.Results(), 0, "k")
	if r0.Count != 2 || r0.Sum != 3 {
		t.Fatalf("replayed window must count a,b once: %+v all=%+v", r0, snk.Results())
	}
	// 结果 ID 幂等：sink 中每个窗口结果只有一条。
	ids := map[string]int{}
	for _, r := range snk.Results() {
		ids[r.ID]++
	}
	for id, n := range ids {
		if n != 1 {
			t.Fatalf("result %s emitted %d times, want exactly once", id, n)
		}
	}
	t.Logf("stage2 metrics: %+v; exactly-once verified (%d distinct results)", eng2.SnapshotMetrics(), len(ids))
}

// TestRecoverFromNonEmptyCheckpoint 干净停机落“中途检查点”（窗口活动、位点未到末尾），
// 新引擎据此恢复，从检查点位点继续，不重复计入已恢复的窗口状态。
func TestRecoverFromNonEmptyCheckpoint(t *testing.T) {
	t.Log("== 非空检查点恢复：处理 1 批后干净停机（窗口活动、已落检查点），再恢复续跑 ==")
	dir := t.TempDir()
	// 同一完整流：前 2 条事件时间很小（窗口0 活动、未定稿），后续推进水位。
	events := []model.Event{
		ev("a", "k", 2, 1, 0),
		ev("b", "k", 8, 2, 0),
		ev("c", "k", 10, 4, 0),
		ev("d", "k", 200, 9, 0),
	}
	snk := sink.NewMemorySink()

	cfg1 := baseCfg()
	cfg1.StopAfterBatches = 1 // 处理 [a,b] 后干净停机并强制落一个状态检查点
	eng1 := New(cfg1, source.NewMemorySource(map[int][]model.Event{0: events}), snk, checkpoint.NewManager(dir))
	if err := eng1.Run(context.Background()); err != nil {
		t.Fatalf("clean stop must return nil, got %v", err)
	}
	if id := eng1.SnapshotMetrics().RestoredFrom; id != "" {
		t.Fatalf("first run must be fresh, got %s", id)
	}
	if eng1.SnapshotMetrics().Checkpoints < 1 || len(snk.Results()) != 0 {
		t.Fatalf("active-window checkpoint must exist and no window emitted yet: cp=%d results=%d",
			eng1.SnapshotMetrics().Checkpoints, len(snk.Results()))
	}
	// 直接校验磁盘检查点：位点=2、含活动窗口0（sum=3）、无已提交结果。
	cp, err := checkpoint.NewManager(dir).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cp.Offsets[0] != 2 || len(cp.Windows) != 1 || cp.Windows[0].Sum != 3 {
		t.Fatalf("mid-stream checkpoint mismatch: offsets=%v windows=%+v", cp.Offsets, cp.Windows)
	}
	t.Logf("mid-stream checkpoint id=%s offsets=%v activeWindow.sum=%d", cp.ID, cp.Offsets, cp.Windows[0].Sum)

	// 新引擎恢复：应报告恢复依据，并从位点 2 继续处理 c,d（不重放 a,b）。
	eng2 := runEngine(t, dir, source.NewMemorySource(map[int][]model.Event{0: events}), snk, baseCfg())
	m2 := eng2.SnapshotMetrics()
	if m2.RestoredFrom != cp.ID {
		t.Fatalf("second run must restore from %s, got %q", cp.ID, m2.RestoredFrom)
	}
	r0, _ := findResult(snk.Results(), 0, "k")
	if r0.Count != 3 || r0.Sum != 7 {
		t.Fatalf("window0 after recovery must be a+b+c exactly once: %+v", r0)
	}
	t.Logf("restored from=%s and continued from offset; window0 count=%d sum=%d (a+b+c)",
		m2.RestoredFrom, r0.Count, r0.Sum)
}

// TestMultiPartitionConcurrency 多分区并行：同窗口状态更新无竞态、结果确定。
func TestMultiPartitionConcurrency(t *testing.T) {
	t.Log("== 多分区并发：4 分区、同键同窗口事件并行汇聚，结果必须精确 ==")
	dir := t.TempDir()
	const partitions = 4
	const perPartition = 50
	data := map[int][]model.Event{}
	var expectCount int64
	var expectSum int64
	for p := 0; p < partitions; p++ {
		for i := 0; i < perPartition; i++ {
			id := fmt.Sprintf("p%d-e%d", p, i)
			// 所有事件使用相同事件时间（同窗口、同水位），隔离验证“并发更新同一窗口”
			// 不存在半更新/窗口错位；乱序与迟到语义由其他用例覆盖。
			v := int64(p*100 + i)
			data[p] = append(data[p], ev(id, "k", 10, v, p))
			expectCount++
			expectSum += v
		}
	}
	cfg := baseCfg()
	cfg.BatchSize = 3
	cfg.PendingQueueCap = 2
	cfg.ProcessDelay = 100 * time.Microsecond
	cfg.OutOfOrderness = 0
	cfg.AllowedLateness = 0
	snk := sink.NewMemorySink()
	eng := runEngine(t, dir, source.NewMemorySource(data), snk, cfg)

	// 窗口0在所有分区时间都超过 90 后不会发生（事件时间<=58），因此靠排空定稿。
	r, ok := findResult(snk.Results(), 0, "k")
	if !ok {
		t.Fatalf("window0 missing: %+v", snk.Results())
	}
	if r.Count != expectCount || r.Sum != expectSum {
		t.Fatalf("concurrent aggregation mismatch: count=%d(want %d) sum=%d(want %d)",
			r.Count, expectCount, r.Sum, expectSum)
	}
	t.Logf("concurrent window0 count=%d sum=%d checkpoints=%d backpressure=%d",
		r.Count, r.Sum, eng.SnapshotMetrics().Checkpoints, eng.SnapshotMetrics().BackPressure)
}

// TestBackpressureObservable 处理慢于读取时必须阻塞并有可观测计数，且内存不无限堆积。
func TestBackpressureObservable(t *testing.T) {
	t.Log("== 背压：小队列 + 处理延迟 -> feeder 阻塞，BackPressure 指标 > 0 ==")
	dir := t.TempDir()
	var events []model.Event
	for i := 0; i < 100; i++ {
		events = append(events, ev(fmt.Sprintf("e%d", i), "k", int64(1+i%58), 1, 0))
	}
	cfg := baseCfg()
	cfg.BatchSize = 5
	cfg.PendingQueueCap = 1
	cfg.ProcessDelay = 2 * time.Millisecond
	snk := sink.NewMemorySink()
	eng := runEngine(t, dir, source.NewMemorySource(map[int][]model.Event{0: events}), snk, cfg)
	if eng.SnapshotMetrics().BackPressure == 0 {
		t.Fatalf("backpressure must be observable: %+v", eng.SnapshotMetrics())
	}
	t.Logf("backpressure events=%d, processed=%d", eng.SnapshotMetrics().BackPressure, eng.SnapshotMetrics().Processed)
}

// TestRefuseCorruptCheckpoint 检查点损坏时必须拒绝启动并给出可区分原因。
func TestRefuseCorruptCheckpoint(t *testing.T) {
	t.Log("== 检查点损坏：篡改数据文件，引擎必须拒绝从中间状态静默继续 ==")
	dir := t.TempDir()
	events := []model.Event{ev("a", "k", 2, 1, 0), ev("b", "k", 90, 2, 0)}
	snk := sink.NewMemorySink()
	runEngine(t, dir, source.NewMemorySource(map[int][]model.Event{0: events}), snk, baseCfg())

	// 篡改清单指向的数据文件。
	mf, err := checkpoint.ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, mf.DataFile)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[10] ^= 0xFF
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}

	eng := New(baseCfg(), source.NewMemorySource(map[int][]model.Event{0: events}), sink.NewMemorySink(),
		checkpoint.NewManager(dir))
	err = eng.Run(context.Background())
	if !errors.Is(err, checkpoint.ErrCorruptData) {
		t.Fatalf("must refuse corrupt checkpoint with ErrCorruptData, got %v", err)
	}
	t.Logf("engine refused to start: %v", err)
}

// TestDeterminismAcrossRuns 相同输入在“一次运行”和“崩溃重放”两条路径下结果完全一致。
func TestDeterminismAcrossRuns(t *testing.T) {
	t.Log("== 确定性：一次跑完 vs 中途崩溃重放，最终窗口结果必须一致 ==")
	events := []model.Event{
		ev("a", "k", 2, 1, 0),
		ev("b", "k", 9, 2, 0),
		ev("c", "k", 70, 4, 0),
		ev("a", "k", 2, 1, 0), // 重复
		ev("d", "k", 5, 50, 0),
	}
	runOnce := func(crashBatches int64) []model.Result {
		dir := t.TempDir()
		snk := sink.NewMemorySink()
		cfg := baseCfg()
		cfg.CrashAfterBatches = crashBatches
		if crashBatches > 0 {
			cfg.CheckpointEvery = time.Hour
		}
		eng := New(cfg, source.NewMemorySource(map[int][]model.Event{0: events}), snk, checkpoint.NewManager(dir))
		err := eng.Run(context.Background())
		if crashBatches > 0 && !errors.Is(err, context.Canceled) {
			t.Fatalf("want crash, got %v", err)
		}
		if crashBatches > 0 {
			cfg2 := baseCfg()
			eng2 := New(cfg2, source.NewMemorySource(map[int][]model.Event{0: events}), snk, checkpoint.NewManager(dir))
			if err := eng2.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		return snk.Results()
	}

	once := runOnce(0)
	crashed := runOnce(2)
	if len(once) != len(crashed) {
		t.Fatalf("result count differs: %d vs %d", len(once), len(crashed))
	}
	for i := range once {
		if once[i].ID != crashed[i].ID || once[i].Count != crashed[i].Count || once[i].Sum != crashed[i].Sum {
			t.Fatalf("result %d differs:\n once=%+v\n crash=%+v", i, once[i], crashed[i])
		}
	}
	t.Logf("both paths produced %d identical windows", len(once))
}

var _ = sync.WaitGroup{}
