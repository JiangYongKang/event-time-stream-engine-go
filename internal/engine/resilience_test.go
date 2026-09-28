package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func timeAfter(d time.Duration) <-chan time.Time { return time.After(d) }

// resEvents 构造确定性乱序 + 重复 + 迟到输入。
func resEvents(n int) []Event {
	var events []Event
	keys := []string{"a", "b", "c"}
	for i := 0; i < n; i++ {
		ev := Event{
			ID:        fmt.Sprintf("ev-%03d", i),
			Key:       keys[i%3],
			EventTime: int64(i)*1_000 + int64((i*37)%20_000) - 10_000,
			Value:     float64(i%10) + 0.5,
		}
		events = append(events, ev)
		if i%40 == 39 {
			events = append(events, events[i]) // 重复
		}
	}
	return events
}

func resCfg(t *testing.T) Config {
	return Config{
		WindowSizeMs:      60_000,
		AllowedLatenessMs: 15_000,
		NumShards:         1,
		Workers:           1,
		QueueCapacity:     8,
		CheckpointEvery:   25,
		Logger:            testLogger(t),
		MaxPendingOutput:  100000,
	}
}

func indexResults(t *testing.T, rs []WindowResult) map[string]WindowResult {
	t.Helper()
	m := map[string]WindowResult{}
	for _, r := range rs {
		k := fmt.Sprintf("%s@%d", r.Key, r.WindowStart)
		if _, dup := m[k]; dup {
			t.Fatalf("duplicate output window %s", k)
		}
		m[k] = r
	}
	return m
}

func assertSameWindows(t *testing.T, want, got map[string]WindowResult) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("window count: want %d got %d", len(want), len(got))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing window %s", k)
		}
		if g.Count != w.Count || math.Abs(g.Sum-w.Sum) > 1e-9 {
			t.Fatalf("window %s: got count=%d sum=%.2f want count=%d sum=%.2f",
				k, g.Count, g.Sum, w.Count, w.Sum)
		}
	}
}

// TestSinkFaultInRunResume：写出阶段多次失败，引擎在进程内自动续写，
// 最终窗口结果与全程无故障基线完全一致（归属/计数/求和），且能看到重试/重放计数。
func TestSinkFaultInRunResume(t *testing.T) {
	events := resEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	sink := NewFileSink(rig.resultsPath, rig.latePath)
	// 让前 3 次提交各在写出 1 条后失败（磁盘写一半），第 4 次起恢复正常。
	sink.FailNextCommitsAfter(3, 1)
	rig.useSink(sink)

	st, err := rig.runOnce(t, resCfg(t), context.Background())
	if err != nil {
		t.Fatalf("run with sink faults: %v", err)
	}
	t.Logf("stats: retries=%d replays=%d checkpoints=%d", st.SinkRetries, st.SinkReplays, st.Checkpoints)
	if st.SinkRetries < 3 {
		t.Fatalf("expected >=3 sink resumes, got %d", st.SinkRetries)
	}
	if st.SinkReplays < 3 {
		t.Fatalf("expected >=3 replayed(skipped) records, got %d", st.SinkReplays)
	}
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
	if len(base.lates(t)) != len(rig.lates(t)) {
		t.Fatalf("late count: base=%d got=%d", len(base.lates(t)), len(rig.lates(t)))
	}
}

// TestSinkFaultThenCrash：写出失败、部分落盘后进程被杀；
// 重启后经“回滚 + 检查点批次重放”，结果与基线一致，不丢不重。
func TestSinkFaultThenCrash(t *testing.T) {
	events := resEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	// 第一轮：在第一次提交时写出 2 条后令 sink 永久失败，
	// 同时处理到第 80 条取消进程（模拟在写出异常期间被杀）。
	sink := NewFileSink(rig.resultsPath, rig.latePath)
	sink.FailNextCommitsAfter(100000, 2)
	rig.useSink(sink)
	cfg := resCfg(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.ProcessedHook = func(n uint64) {
		if n >= 80 {
			cancel()
		}
	}
	if _, err := rig.runOnce(t, cfg, ctx); err == nil {
		t.Fatal("expected first run to end with error (crash/fault)")
	}
	// 重启：sink 恢复正常（新实例），从最近检查点恢复并重放未确认批次。
	rig.sink = nil
	st, err := rig.runOnce(t, resCfg(t), context.Background())
	if err != nil {
		t.Fatalf("recovered run: %v", err)
	}
	t.Logf("recovered stats: checkpoints=%d processed=%d", st.Checkpoints, st.Processed)
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
	if len(base.lates(t)) != len(rig.lates(t)) {
		t.Fatalf("late count: base=%d got=%d", len(base.lates(t)), len(rig.lates(t)))
	}
}

// writeV1LegacyCheckpoint 把最新代检查点转成 v1 老格式单文件布局：
// 删除全部代文件，仅保留老路径指针（版本 1、无新字段），模拟升级前留下的检查点。
func writeV1LegacyCheckpoint(t *testing.T, rig testRig) {
	t.Helper()
	store := NewCheckpointStore(rig.cpPath)
	ids, err := store.Generations()
	if err != nil || len(ids) == 0 {
		t.Fatalf("no generations: %v", err)
	}
	cp, err := store.LoadGeneration(ids[len(ids)-1])
	if err != nil {
		t.Fatal(err)
	}
	cp.Version = 1
	cp.PendingResults = nil
	cp.PendingLates = nil
	cp.CreatedAtUnix = 0
	payload, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	file := checkpointFile{Payload: payload, Checksum: crc32.ChecksumIEEE(payload)}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := os.Remove(store.genPath(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(rig.cpPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote legacy v1 checkpoint id=%d at %s (no generation files)", cp.ID, rig.cpPath)
}

// TestRecoverFromSpecificGeneration：先完整跑出多代；再从中间某代恢复，
// 该代之后的“未来代”被清理，重放后的最终结果与全程无中断基线一致。
func TestRecoverFromSpecificGeneration(t *testing.T) {
	events := resEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	if _, err := rig.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}
	store := NewCheckpointStore(rig.cpPath)
	ids, err := store.Generations()
	if err != nil || len(ids) < 3 {
		t.Fatalf("expected >=3 generations, got %v (err=%v)", ids, err)
	}
	target := ids[len(ids)/2]
	t.Logf("generations=%v, forcing recovery from %d", ids, target)

	// 指定不存在的代：必须明确报 ErrCheckpointNotFound，而不是静默从最新代硬接。
	if _, err := store.LoadGeneration(target + 9999); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("missing generation: want ErrCheckpointNotFound, got %v", err)
	}

	cfg := resCfg(t)
	cfg.RecoverFromGeneration = target
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatalf("run from generation %d: %v", target, err)
	}
	t.Logf("recovered-from-gen stats: checkpoints=%d fork_prunes=%d", st.Checkpoints, st.ForkPrunes)
	if st.ForkPrunes == 0 {
		t.Fatal("expected fork pruning to be observable when recovering from older generation")
	}
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
	if len(base.lates(t)) != len(rig.lates(t)) {
		t.Fatalf("late count: base=%d got=%d", len(base.lates(t)), len(rig.lates(t)))
	}
}

// TestLegacyV1CheckpointCompatible：老版本（v1 单文件）检查点必须可读、可接着用。
func TestLegacyV1CheckpointCompatible(t *testing.T) {
	events := resEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	// 跑到一半“崩溃”，制造一份真实中段状态的检查点。
	cfg := resCfg(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.ProcessedHook = func(n uint64) {
		if n >= 120 {
			cancel()
		}
	}
	if _, err := rig.runOnce(t, cfg, ctx); err == nil {
		t.Fatal("expected crash run to be canceled")
	}
	// 把检查点降级成 v1 单文件老格式（模拟升级前磁盘上已有文件）。
	writeV1LegacyCheckpoint(t, rig)

	// 升级后的引擎必须能读、能恢复、能跑完，结果与基线一致。
	st, err := rig.runOnce(t, resCfg(t), context.Background())
	if err != nil {
		t.Fatalf("recover from legacy v1: %v", err)
	}
	t.Logf("legacy v1 recovery ok, checkpoints=%d", st.Checkpoints)
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
	if len(base.lates(t)) != len(rig.lates(t)) {
		t.Fatalf("late count: base=%d got=%d", len(base.lates(t)), len(rig.lates(t)))
	}
}

// TestCheckpointRetentionLimit：保留代数有上限，超出部分按 ID 删除并计数。
func TestCheckpointRetentionLimit(t *testing.T) {
	events := resEvents(300)
	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	cfg := resCfg(t)
	cfg.RetainCheckpoints = 2
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store := NewCheckpointStore(rig.cpPath).SetRetain(2)
	ids, err := store.Generations()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("remaining generations=%v prunes=%d", ids, st.CheckpointPrunes)
	if len(ids) != 2 {
		t.Fatalf("retain=2: want exactly 2 generations on disk, got %v", ids)
	}
	if st.CheckpointPrunes == 0 {
		t.Fatal("expected CheckpointPrunes counter to be observable")
	}
	if store.Retain() != 2 {
		t.Fatalf("store retain=%d want 2", store.Retain())
	}
}

// TestPendingOutputCapPolicies：待写出上限触发时的拒绝/阻塞策略确定性行为。
func TestPendingOutputCapPolicies(t *testing.T) {
	rig := newRig(t)
	writeEvents(t, rig.inputPath, []Event{{ID: "x", Key: "k", EventTime: 1, Value: 1}})

	newEngine := func(policy LateOverflowPolicy, cap int) *Engine {
		src := NewFileSource(rig.inputPath)
		t.Cleanup(func() { src.Close() })
		sink := NewFileSink(rig.resultsPath+"."+string(rune('a'+cap)), rig.latePath)
		store := NewCheckpointStore(filepath.Join(t.TempDir(), "cp.json"))
		cfg := Config{
			WindowSizeMs: 60_000, AllowedLatenessMs: 0, NumShards: 1, Workers: 1,
			QueueCapacity: 4, MaxPendingOutput: cap, LateOverflow: policy,
		}
		return NewEngine(cfg, src, sink, store)
	}

	// Reject：填满后新迟到记录被明确拒绝（返回 false），并可观察到 FlushTriggers。
	e := newEngine(LateOverflowReject, 2)
	e.pendingMu.Lock()
	e.pendingLates = []LateRecord{{Event: Event{ID: "x1"}}, {Event: Event{ID: "x2"}}}
	e.pendingMu.Unlock()
	rec := LateRecord{Event: Event{ID: "late-x"}, Reason: "r", Watermark: 1}
	if e.enqueueLate(context.Background(), rec) {
		t.Fatal("reject policy: enqueueLate must return false at cap")
	}
	if e.Stats().FlushTriggers != 1 {
		t.Fatalf("reject policy: FlushTriggers=%d want 1", e.Stats().FlushTriggers)
	}
	t.Logf("reject policy observable: FlushTriggers=%d (cap=2)", e.Stats().FlushTriggers)

	// Block：填满后阻塞，直到提交腾出空间才放行，且期间不丢失、不超容量。
	e2 := newEngine(LateOverflowBlock, 2)
	e2.pendingMu.Lock()
	e2.pendingLates = []LateRecord{{Event: Event{ID: "y1"}}, {Event: Event{ID: "y2"}}}
	e2.pendingMu.Unlock()
	done := make(chan bool, 1)
	go func() { done <- e2.enqueueLate(context.Background(), rec) }()
	select {
	case <-done:
		t.Fatal("block policy: enqueueLate should block while at cap")
	case <-timeAfter(50 * time.Millisecond):
	}
	// 模拟一次成功提交腾出空间并广播。
	e2.pendingMu.Lock()
	e2.pendingLates = nil
	e2.pendingCond.Broadcast()
	e2.pendingMu.Unlock()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("block policy: enqueue should succeed after space freed")
		}
	case <-timeAfter(2 * time.Second):
		t.Fatal("block policy: enqueue did not resume after broadcast")
	}
	if n := len(e2.pendingLates); n != 1 {
		t.Fatalf("block policy: pending lates=%d want 1", n)
	}
	t.Logf("block policy observable: blocked then accepted after commit, pending=%d", len(e2.pendingLates))
}
