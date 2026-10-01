package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// writeEvents 把事件写成 JSONL 输入文件。
func writeEvents(t *testing.T, path string, events []Event) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			t.Fatal(err)
		}
	}
}

type testRig struct {
	inputPath   string
	resultsPath string
	latePath    string
	cpPath      string
	sink        *FileSink // 非 nil 时复用同一个 sink（用于故障注入）
}

func newRig(t *testing.T) testRig {
	t.Helper()
	dir := t.TempDir()
	return testRig{
		inputPath:   filepath.Join(dir, "input.jsonl"),
		resultsPath: filepath.Join(dir, "results.jsonl"),
		latePath:    filepath.Join(dir, "late.jsonl"),
		cpPath:      filepath.Join(dir, "checkpoint.json"),
	}
}

func (r *testRig) useSink(s *FileSink) { r.sink = s }

func (r testRig) runOnce(t *testing.T, cfg Config, ctx context.Context) (Stats, error) {
	t.Helper()
	src := NewFileSource(r.inputPath)
	defer src.Close()
	sink := r.sink
	if sink == nil {
		sink = NewFileSink(r.resultsPath, r.latePath)
	}
	store := NewCheckpointStore(r.cpPath)
	e := NewEngine(cfg, src, sink, store)
	err := e.Run(ctx)
	return e.Stats(), err
}

func (r testRig) results(t *testing.T) []WindowResult {
	t.Helper()
	res, err := NewFileSink(r.resultsPath, r.latePath).ReadResults()
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (r testRig) lates(t *testing.T) []LateRecord {
	t.Helper()
	l, err := NewFileSink(r.resultsPath, r.latePath).ReadLates()
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// modelRun 用独立的简单模型重放事件，作为引擎输出的参照。
// 仅适用于 NumShards=1 / Workers=1 的确定性场景。
func modelRun(events []Event, cfg Config) (map[string]WindowResult, []string) {
	results := map[string]WindowResult{}
	var lateIDs []string
	seen := map[string]bool{}
	maxSeen := int64(math.MinInt64)
	for _, ev := range events {
		if seen[ev.ID] {
			continue
		}
		seen[ev.ID] = true
		if ev.EventTime > maxSeen {
			maxSeen = ev.EventTime
		}
		wm := int64(math.MinInt64)
		if maxSeen != math.MinInt64 {
			wm = maxSeen - cfg.AllowedLatenessMs
		}
		if ev.EventTime <= wm {
			lateIDs = append(lateIDs, ev.ID)
			continue
		}
		w := NewAssigner(cfg.WindowSizeMs).Assign(ev.EventTime)
		key := fmt.Sprintf("%s@%d", ev.Key, w.Start)
		r := results[key]
		r.Key = ev.Key
		r.WindowStart = w.Start
		r.WindowEnd = w.End
		r.Count++
		r.Sum += ev.Value
		results[key] = r
	}
	return results, lateIDs
}

func TestOutOfOrderDuplicatesLate(t *testing.T) {
	rig := newRig(t)
	cfg := Config{
		WindowSizeMs:      60_000,
		AllowedLatenessMs: 30_000,
		NumShards:         1,
		Workers:           1,
		QueueCapacity:     8,
		CheckpointEvery:   3,
		Logger:            testLogger(t),
	}
	events := []Event{
		{ID: "e1", Key: "a", EventTime: 10_000, Value: 1},
		{ID: "e2", Key: "a", EventTime: 25_000, Value: 2},
		{ID: "e3", Key: "a", EventTime: 15_000, Value: 3}, // 乱序：仍在窗口 [0,60s)
		{ID: "e2", Key: "a", EventTime: 25_000, Value: 2}, // 重复：必须去重
		{ID: "e4", Key: "b", EventTime: 5_000, Value: 4},
		{ID: "e5", Key: "a", EventTime: 70_000, Value: 5},   // 窗口 [60s,120s)
		{ID: "e6", Key: "a", EventTime: 120_000, Value: 6},  // 推进水位线 -> 90000
		{ID: "e7", Key: "a", EventTime: 55_000, Value: 7},   // 迟到：55000 <= 90000，旁路
		{ID: "e8", Key: "b", EventTime: 65_000, Value: 8},   // 迟到：65000 <= 90000
		{ID: "e9", Key: "b", EventTime: 200_000, Value: 9},  // 推进水位线，提交更多窗口
		{ID: "e10", Key: "b", EventTime: 100_000, Value: 1}, // 迟到
	}
	writeEvents(t, rig.inputPath, events)

	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stats: %+v", st)

	got := map[string]WindowResult{}
	for _, r := range rig.results(t) {
		key := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
		if _, dup := got[key]; dup {
			t.Fatalf("duplicate output for window %s", key)
		}
		got[key] = r
	}
	want, wantLate := modelRun(events, cfg)
	if len(got) != len(want) {
		t.Fatalf("result windows mismatch: got %d want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for k, w := range want {
		g := got[k]
		if g.Count != w.Count || math.Abs(g.Sum-w.Sum) > 1e-9 {
			t.Fatalf("window %s: got count=%d sum=%.2f, want count=%d sum=%.2f",
				k, g.Count, g.Sum, w.Count, w.Sum)
		}
		t.Logf("window %s OK: count=%d sum=%.2f", k, g.Count, g.Sum)
	}

	// 迟到记录：必须显式旁路且带原因，不得静默丢弃。
	lates := rig.lates(t)
	if len(lates) != len(wantLate) {
		t.Fatalf("late records: got %d want %d (%v)", len(lates), len(wantLate), wantLate)
	}
	for _, l := range lates {
		if l.Reason == "" {
			t.Fatalf("late record missing reason: %+v", l)
		}
		t.Logf("late record: id=%s reason=%q watermark=%d", l.Event.ID, l.Reason, l.Watermark)
	}

	// 已知事实断言：窗口 [0,60s) 的 key=a 恰为 e1+e2+e3（e2 去重、e7 迟到不计入）。
	w := got["a@0"]
	if w.Count != 3 || w.Sum != 6 {
		t.Fatalf("window a@[0,60s): got count=%d sum=%.2f, want count=3 sum=6", w.Count, w.Sum)
	}
	if st.Duplicates != 1 {
		t.Fatalf("duplicates: got %d want 1", st.Duplicates)
	}
	if st.Late != 3 {
		t.Fatalf("late: got %d want 3 (e7, e8, e10)", st.Late)
	}
}

func TestLateEventDoesNotModifyCommittedWindow(t *testing.T) {
	rig := newRig(t)
	cfg := Config{
		WindowSizeMs:      60_000,
		AllowedLatenessMs: 10_000,
		NumShards:         1,
		Workers:           1,
		QueueCapacity:     4,
		CheckpointEvery:   2,
		Logger:            testLogger(t),
	}
	events := []Event{
		{ID: "g1", Key: "k", EventTime: 30_000, Value: 10}, // 窗口 [0,60s)
		{ID: "g2", Key: "k", EventTime: 100_000, Value: 1}, // 水位线 -> 90000，提交窗口 [0,60s)
		{ID: "g3", Key: "k", EventTime: 40_000, Value: 99}, // 迟到：属于已提交窗口，必须旁路
	}
	writeEvents(t, rig.inputPath, events)
	if _, err := rig.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, r := range rig.results(t) {
		t.Logf("result: key=%s [%d,%d) count=%d sum=%.2f", r.Key, r.WindowStart, r.WindowEnd, r.Count, r.Sum)
		if r.WindowStart == 0 && (r.Count != 1 || r.Sum != 10) {
			t.Fatalf("committed window [0,60s) was modified by late event: %+v", r)
		}
	}
	lates := rig.lates(t)
	if len(lates) != 1 || lates[0].Event.ID != "g3" {
		t.Fatalf("expected g3 side-output, got %+v", lates)
	}
}

func TestCrashRecovery(t *testing.T) {
	// 构造确定性输入：乱序 + 重复 + 迟到。
	var events []Event
	keys := []string{"a", "b", "c"}
	for i := 0; i < 300; i++ {
		ev := Event{
			ID:        fmt.Sprintf("ev-%03d", i),
			Key:       keys[i%3],
			EventTime: int64(i)*1_000 + int64((i*37)%20_000) - 10_000, // 乱序 ±10s
			Value:     float64(i%10) + 0.5,
		}
		events = append(events, ev)
		if i%40 == 39 {
			events = append(events, events[i-1]) // 原样重发 -> 重复
		}
	}

	cfg := Config{
		WindowSizeMs:      60_000,
		AllowedLatenessMs: 15_000,
		NumShards:         1,
		Workers:           1,
		QueueCapacity:     8,
		CheckpointEvery:   25,
	}

	// 基线：无崩溃一次跑完。
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	cfg.Logger = testLogger(t)
	baseStats, err := base.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// 崩溃运行：处理到第 100 条时取消（模拟崩溃），再重启恢复。
	crash := newRig(t)
	writeEvents(t, crash.inputPath, events)
	cancel := func() {}
	crashCfg := cfg
	crashCfg.ProcessedHook = func(n uint64) {
		if n >= 100 {
			cancel()
		}
	}
	ctx, c := context.WithCancel(context.Background())
	crashCfg.ProcessedHook = func(n uint64) {
		if n >= 100 {
			c()
		}
	}
	_ = cancel
	st1, err := crash.runOnce(t, crashCfg, ctx)
	if err == nil {
		t.Fatal("expected run1 to be canceled (simulated crash)")
	}
	t.Logf("run1 (crashed): processed=%d checkpoints=%d", st1.Processed, st1.Checkpoints)

	// 重启：从检查点恢复，跑完剩余输入。
	st2, err := crash.runOnce(t, crashCfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("run2 (recovered): processed=%d checkpoints=%d", st2.Processed, st2.Checkpoints)

	// 断言 1：崩溃运行与基线产出完全一致的窗口结果（恰好一次）。
	baseRes := map[string]WindowResult{}
	for _, r := range base.results(t) {
		baseRes[fmt.Sprintf("%s@%d", r.Key, r.WindowStart)] = r
	}
	crashRes := map[string]WindowResult{}
	for _, r := range crash.results(t) {
		key := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
		if _, dup := crashRes[key]; dup {
			t.Fatalf("duplicate output after recovery: window %s", key)
		}
		crashRes[key] = r
	}
	if len(baseRes) != len(crashRes) {
		t.Fatalf("window count mismatch: baseline=%d crash-recovered=%d", len(baseRes), len(crashRes))
	}
	for k, w := range baseRes {
		g := crashRes[k]
		if g.Count != w.Count || math.Abs(g.Sum-w.Sum) > 1e-9 {
			t.Fatalf("window %s mismatch after recovery: got %+v want %+v", k, g, w)
		}
	}

	// 断言 2：全局统计自洽（迟到记录数一致，窗口累计一致）。
	var baseCount, crashCount int64
	for _, r := range baseRes {
		baseCount += r.Count
	}
	for _, r := range crashRes {
		crashCount += r.Count
	}
	if baseCount != crashCount {
		t.Fatalf("global count mismatch: baseline=%d crash=%d", baseCount, crashCount)
	}
	if len(base.lates(t)) != len(crash.lates(t)) {
		t.Fatalf("late count mismatch: baseline=%d crash=%d", len(base.lates(t)), len(crash.lates(t)))
	}
	t.Logf("recovery verified: %d windows, total count=%d, lates=%d",
		len(crashRes), crashCount, len(crash.lates(t)))
	t.Logf("baseline stats=%+v", baseStats)
}

func TestSinkTruncateRollback(t *testing.T) {
	dir := t.TempDir()
	sink := NewFileSink(filepath.Join(dir, "r.jsonl"), filepath.Join(dir, "l.jsonl"))
	// 模拟：检查点 1 已确认，检查点 2 的输出已写但检查点未保存（崩溃）。
	if err := sink.Commit(1, []WindowResult{{Key: "a", Count: 1, CheckpointID: 1}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sink.Commit(2, []WindowResult{{Key: "a", Count: 2, CheckpointID: 2}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.TruncateTo(1); err != nil {
		t.Fatal(err)
	}
	res, err := sink.ReadResults()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].CheckpointID != 1 {
		t.Fatalf("rollback failed: %+v", res)
	}
	t.Logf("rollback ok: kept %d record(s) from confirmed checkpoint", len(res))
}

func TestBackpressureObservable(t *testing.T) {
	rig := newRig(t)
	var events []Event
	for i := 0; i < 200; i++ {
		events = append(events, Event{ID: fmt.Sprintf("b-%d", i), Key: "k", EventTime: int64(i) * 1000, Value: 1})
	}
	writeEvents(t, rig.inputPath, events)
	cfg := Config{
		WindowSizeMs:      60_000,
		AllowedLatenessMs: 5_000,
		NumShards:         1,
		Workers:           1,
		QueueCapacity:     1, // 极小队列：必然触发背压
		CheckpointEvery:   50,
		Logger:            testLogger(t),
		ProcessedHook: func(uint64) {
			time.Sleep(time.Millisecond) // 模拟慢消费者
		},
	}
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("backpressure blocks=%d processed=%d", st.BackpressureBlocks, st.Processed)
	if st.BackpressureBlocks == 0 {
		t.Fatal("expected observable backpressure with queue capacity 1")
	}
	// 背压下结果仍须正确：所有事件恰好计入一次。
	var total int64
	for _, r := range rig.results(t) {
		total += r.Count
	}
	if total != 200 {
		t.Fatalf("total count=%d want 200", total)
	}
}

func TestConcurrentShardsConsistency(t *testing.T) {
	rig := newRig(t)
	var events []Event
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for i := 0; i < 2000; i++ {
		events = append(events, Event{
			ID:        fmt.Sprintf("c-%d", i),
			Key:       keys[i%len(keys)],
			EventTime: int64(i) * 500,
			Value:     1,
		})
		if i%97 == 0 && i > 0 {
			events = append(events, events[i-1]) // 重复
		}
	}
	writeEvents(t, rig.inputPath, events)
	cfg := Config{
		WindowSizeMs:      10_000,
		AllowedLatenessMs: 2_000,
		NumShards:         8,
		Workers:           8,
		QueueCapacity:     16,
		CheckpointEvery:   100,
		Logger:            testLogger(t),
	}
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("concurrent stats: %+v", st)

	// 不变量 1：无重复窗口输出。
	seenWin := map[string]bool{}
	var totalCount int64
	for _, r := range rig.results(t) {
		key := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
		if seenWin[key] {
			t.Fatalf("duplicate window output: %s", key)
		}
		seenWin[key] = true
		if r.WindowStart%cfg.WindowSizeMs != 0 {
			t.Fatalf("misaligned window: %+v", r)
		}
		totalCount += r.Count
	}
	// 不变量 2：窗口计数 + 迟到数 == 去重后事件数。
	uniqueIDs := map[string]bool{}
	for _, ev := range events {
		uniqueIDs[ev.ID] = true
	}
	lateCount := int64(len(rig.lates(t)))
	if totalCount+lateCount != int64(len(uniqueIDs)) {
		t.Fatalf("consistency violated: results=%d late=%d unique=%d",
			totalCount, lateCount, len(uniqueIDs))
	}
	// 不变量 3：重复事件全部被去重。
	if st.Duplicates == 0 {
		t.Fatal("expected duplicates to be detected")
	}
	t.Logf("consistency ok: windows=%d total=%d late=%d dup=%d",
		len(seenWin), totalCount, lateCount, st.Duplicates)
}

func TestRecoverFromCorruptCheckpointFails(t *testing.T) {
	rig := newRig(t)
	writeEvents(t, rig.inputPath, []Event{{ID: "x", Key: "k", EventTime: 1_000, Value: 1}})
	cfg := Config{WindowSizeMs: 60_000, AllowedLatenessMs: 1_000, NumShards: 1, Workers: 1, Logger: testLogger(t)}
	// 先正常跑出检查点。
	if _, err := rig.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	// 破坏实际加载的最新代检查点文件（不是老路径指针副本）。
	store := NewCheckpointStore(rig.cpPath)
	ids, err := store.Generations()
	if err != nil || len(ids) == 0 {
		t.Fatalf("expected checkpoint generations: %v", err)
	}
	latestPath := filepath.Join(store.dir, genName(ids[len(ids)-1]))
	data, _ := os.ReadFile(latestPath)
	data[len(data)/3] ^= 0xFF
	os.WriteFile(latestPath, data, 0o644)
	// 恢复必须明确失败，而不是从中间静默继续。
	_, err = rig.runOnce(t, cfg, context.Background())
	if err == nil {
		t.Fatal("expected recovery failure on corrupt checkpoint")
	}
	if !contains(err.Error(), "corrupt") {
		t.Fatalf("error should identify corruption, got: %v", err)
	}
	t.Logf("corrupt checkpoint rejected: %v", err)
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func TestResultOrderingDeterministic(t *testing.T) {
	// 排序辅助：保证比较时结果有序（文档化结果可解释性）。
	rig := newRig(t)
	events := []Event{
		{ID: "s2", Key: "a", EventTime: 1_000, Value: 2},
		{ID: "s1", Key: "a", EventTime: 61_000, Value: 1},
	}
	writeEvents(t, rig.inputPath, events)
	cfg := Config{WindowSizeMs: 60_000, AllowedLatenessMs: 5_000, NumShards: 1, Workers: 1, Logger: testLogger(t)}
	if _, err := rig.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	res := rig.results(t)
	sort.Slice(res, func(i, j int) bool { return res[i].WindowStart < res[j].WindowStart })
	for _, r := range res {
		t.Logf("window [%d,%d) key=%s count=%d sum=%.2f", r.WindowStart, r.WindowEnd, r.Key, r.Count, r.Sum)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(res))
	}
}
