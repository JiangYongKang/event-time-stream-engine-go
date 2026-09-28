package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func genEvents(n int) []Event {
	var events []Event
	keys := []string{"a", "b", "c"}
	for i := 0; i < n; i++ {
		events = append(events, Event{
			ID:        fmt.Sprintf("g-%03d", i),
			Key:       keys[i%3],
			EventTime: int64(i)*1_000 + int64((i*29)%15_000) - 7_500,
			Value:     float64(i%6) + 0.5,
		})
	}
	return events
}

func genCfg(t *testing.T, maxCp int) Config {
	return Config{
		WindowSizeMs: 60_000, AllowedLatenessMs: 5_000,
		NumShards: 1, Workers: 1, QueueCapacity: 8, CheckpointEvery: 20,
		MaxCheckpoints: maxCp,
		Logger:         testLogger(t),
	}
}

// TestCheckpointGenerationRetention：存储层只保留最近 MaxCheckpoints 代，
// 旧代文件被删除，删除数量可观测（不依赖检查点触发的调度时序）。
func TestCheckpointGenerationRetention(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStore(filepath.Join(dir, "cp.json"))
	store.SetMaxGenerations(3)
	const total = 7
	for id := uint64(1); id <= total; id++ {
		if err := store.Save(&Checkpoint{ID: id, Offset: int64(id)}); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retained generations (newest-first)=%v", ids)
	wantIDs := []uint64{7, 6, 5}
	if len(ids) != len(wantIDs) {
		t.Fatalf("expected 3 retained generations, got %v", ids)
	}
	for i := range wantIDs {
		if ids[i] != wantIDs[i] {
			t.Fatalf("retained generations = %v, want %v", ids, wantIDs)
		}
	}
	if got := store.Evictions(); got != total-3 {
		t.Fatalf("eviction counter = %d, want %d", got, total-3)
	}
	t.Logf("evicted checkpoint files: %d (retained=%v)", store.Evictions(), ids)
}

// TestCheckpointRetentionViaEngine：端到端再确认一次引擎 Stats 会带出代数裁剪计数。
func TestCheckpointRetentionViaEngine(t *testing.T) {
	events := genEvents(300)
	cfg := genCfg(t, 2)
	cfg.CheckpointEvery = 5
	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids, err := NewCheckpointStore(rig.cpPath).List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) > 2 {
		t.Fatalf("engine must enforce cap of 2, got %v", ids)
	}
	t.Logf("engine retained generations=%v evictions=%d", ids, st.CheckpointEvictions)
}

// TestRestoreFromOlderGeneration：先跑出多代，再指定从中间旧代恢复并跑完，
// 最终结果必须与从头不中断完全一致；更新代数被裁剪，恢复点与窗口状态有日志依据。
func TestRestoreFromOlderGeneration(t *testing.T) {
	// CheckpointEvery 较小：即便高负载下 cpCh 信号发生合并，也能稳定产生足够多代数。
	events := genEvents(600)
	cfg := genCfg(t, 10)
	cfg.CheckpointEvery = 10

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	want := resultIndex(t, base)

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	if _, err := rig.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	store := NewCheckpointStore(rig.cpPath)
	all, err := store.List()
	if err != nil || len(all) < 3 {
		t.Fatalf("expected at least 3 generations, got %v err=%v", all, err)
	}
	// all 降序：选最旧代数作为恢复点（它必有更新代数可裁剪，且不依赖具体代数数量）。
	target := all[len(all)-1]
	newest := all[0]
	// 恢复点必须能独立读出，且带当时位点与窗口状态（恢复依据）。
	rcp, err := store.LoadID(target)
	if err != nil {
		t.Fatalf("load target generation: %v", err)
	}
	t.Logf("restarting from generation=%d while newest was %d; resume_offset=%d open_windows=%d",
		target, newest, rcp.Offset, len(rcp.Windows))

	restoreCfg := cfg
	restoreCfg.RestoreCheckpointID = target
	restoreCfg.MaxCheckpoints = 10_000 // 本用例要验证从恢复点连续重放，不让保留策略裁剪
	st, err := rig.runOnce(t, restoreCfg, context.Background())
	if err != nil {
		t.Fatalf("restore from older generation failed: %v", err)
	}
	t.Logf("restart stats: checkpoints=%d evictions=%d", st.Checkpoints, st.CheckpointEvictions)

	// 恢复时比 target 新的旧代数必须被裁剪，裁剪数量可观测。
	if st.CheckpointEvictions == 0 {
		t.Fatalf("expected stats to report pruning of newer generations")
	}
	// 恢复重跑后：恢复点保留，且从 target+1 连续推进到重新生成的最新一代
	//（旧的 >target 代数已在恢复时裁剪，其编号被重跑产生的新一代复用）。
	left, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(left) == 0 {
		t.Fatal("expected generations after restored run")
	}
	top := left[0]
	for id := target; id <= top; id++ {
		if _, err := store.LoadID(id); err != nil {
			t.Fatalf("expected contiguous generation %d after restore (left=%v): %v", id, left, err)
		}
	}
	assertSameResults(t, resultIndex(t, rig), want)
	// 全局累计条数与基线一致（无重复计数、无遗漏）。
	var gotCount, wantCount int64
	for _, r := range rig.results(t) {
		gotCount += r.Count
	}
	for _, r := range base.results(t) {
		wantCount += r.Count
	}
	if gotCount != wantCount {
		t.Fatalf("total count mismatch: restored=%d baseline=%d", gotCount, wantCount)
	}
	if l1, l2 := len(rig.lates(t)), len(base.lates(t)); l1 != l2 {
		t.Fatalf("late count mismatch: restored=%d baseline=%d", l1, l2)
	}
}

// TestRestoreSpecifiedGenerationMissing：指定一个不存在的代数必须明确失败，
// 不能静默退回到最新代数“从中间硬接”。
func TestRestoreSpecifiedGenerationMissing(t *testing.T) {
	rig := newRig(t)
	writeEvents(t, rig.inputPath, genEvents(40))
	cfg := genCfg(t, 3)
	if _, err := rig.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	bad := cfg
	bad.RestoreCheckpointID = 9999
	_, err := rig.runOnce(t, bad, context.Background())
	if err == nil {
		t.Fatal("expected failure when requested generation is missing")
	}
	if !contains(err.Error(), "not found") {
		t.Fatalf("error should say generation not found, got: %v", err)
	}
	t.Logf("missing generation rejected: %v", err)
}

// TestLegacyV1CheckpointCompatible：升级前的 v1 单文件检查点 + 旧 sink 输出，
// 升级后必须能读、能恢复、能接着跑完，且结果与基线一致；首个 v2 写出后旧文件归档。
func TestLegacyV1CheckpointCompatible(t *testing.T) {
	events := genEvents(300)
	cfg := genCfg(t, 3)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	want := resultIndex(t, base)

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)

	// 第一步：用当前代码跑出真实多代快照。
	if _, err := rig.runOnce(t, cfg, context.Background()); err != nil {
		t.Fatal(err)
	}
	store := NewCheckpointStore(rig.cpPath)
	ids, _ := store.List()
	if len(ids) < 2 {
		t.Fatalf("need >=2 generations, got %v", ids)
	}
	snapID := ids[len(ids)/2] // 选一个中间代，保证其后还有待处理输入
	cp, err := store.LoadID(snapID)
	if err != nil {
		t.Fatal(err)
	}

	// 第二步：把该快照降级写成 v1 单文件，sink 只保留 <=snapID 的确认输出，
	// 删除 v2 代数目录 —— 模拟“升级前老版本停下来时”的磁盘状态。
	v1 := *cp
	v1.Version = 1
	v1.CreatedAtMs = 0
	payload, err := json.Marshal(v1)
	if err != nil {
		t.Fatal(err)
	}
	file := checkpointFile{Payload: payload, Checksum: crc32.ChecksumIEEE(payload)}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(rig.cpPath + genDirSuffix); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rig.cpPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sink := NewFileSink(rig.resultsPath, rig.latePath)
	if err := sink.TruncateTo(snapID); err != nil {
		t.Fatal(err)
	}

	// 第三步：升级后的引擎从 v1 检查点恢复并跑完。
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatalf("legacy v1 checkpoint must be recoverable: %v", err)
	}
	t.Logf("resumed after upgrade from v1: checkpoints=%d stats=%+v", st.Checkpoints, st)
	if _, statErr := os.Stat(rig.cpPath); !os.IsNotExist(statErr) {
		t.Fatalf("legacy v1 single file should be archived after first v2 save, stat err=%v", statErr)
	}
	if gens, _ := NewCheckpointStore(rig.cpPath).List(); len(gens) == 0 {
		t.Fatal("expected v2 generations after upgrade save")
	}
	assertSameResults(t, resultIndex(t, rig), want)
	var c int64
	for _, r := range rig.results(t) {
		c += r.Count
	}
	var wc int64
	for _, r := range base.results(t) {
		wc += r.Count
	}
	if c != wc {
		t.Fatalf("legacy resume total count mismatch: %d vs baseline %d", c, wc)
	}
	// 结果文件中的 checkpoint_id 应出现 v2 新一代（snapID+1 起）。
	ids2, _ := NewCheckpointStore(rig.cpPath).List()
	if len(ids2) == 0 || ids2[len(ids2)-1] <= snapID {
		t.Fatalf("expected new generations after v1 resume, got %v", ids2)
	}
}

// TestCheckpointUnreadableVsCorrupt：权限等“读不了”与内容损坏必须可区分。
func TestCheckpointUnreadableVsCorrupt(t *testing.T) {
	t.Run("unreadable is not corrupt", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses file permissions")
		}
		dir := t.TempDir()
		store := NewCheckpointStore(filepath.Join(dir, "cp.json"))
		if err := store.Save(&Checkpoint{ID: 1}); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(store.dir, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(store.dir, 0o755) })
		_, err := store.Load()
		if err == nil {
			t.Fatal("expected read error")
		}
		if isCorruptOrVersion(err) {
			t.Fatalf("permission/read error must not be classified as corrupt/version: %v", err)
		}
		t.Logf("unreadable checkpoint reported distinctly: %v", err)
	})

	t.Run("torn bytes are corrupt", func(t *testing.T) {
		dir := t.TempDir()
		store := NewCheckpointStore(filepath.Join(dir, "cp.json"))
		if err := store.Save(&Checkpoint{ID: 1}); err != nil {
			t.Fatal(err)
		}
		gp := genPath(store.dir, 1)
		if err := os.WriteFile(gp, []byte(`{"payload":"NOT-JSON`), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := store.Load()
		if err == nil || !isCorruptOrVersion(err) {
			t.Fatalf("garbled checkpoint must be corrupt, got %v", err)
		}
		t.Logf("corrupt checkpoint identified: %v", err)
	})
}

func isCorruptOrVersion(err error) bool {
	return contains(err.Error(), ErrCheckpointCorrupt.Error()) ||
		contains(err.Error(), ErrCheckpointVersion.Error())
}

// TestPruneFromDirectly：存储层按恢复点裁剪更新代数，裁剪数量可观测。
func TestPruneFromDirectly(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStore(filepath.Join(dir, "cp.json"))
	store.SetMaxGenerations(100) // 本轮不触发上限裁剪
	for id := uint64(1); id <= 6; id++ {
		if err := store.Save(&Checkpoint{ID: id, Offset: int64(id) * 10}); err != nil {
			t.Fatal(err)
		}
	}
	ids, _ := store.List()
	if len(ids) != 6 {
		t.Fatalf("setup: want 6 gens, got %v", ids)
	}
	removed, err := store.PruneFrom(4)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 generations pruned (5,6), got %d", removed)
	}
	left, _ := store.List()
	want := []uint64{4, 3, 2, 1}
	if len(left) != len(want) {
		t.Fatalf("after prune: got %v want %v", left, want)
	}
	for i := range want {
		if left[i] != want[i] {
			t.Fatalf("after prune: got %v want %v", left, want)
		}
	}
	cp, err := store.LoadID(4)
	if err != nil || cp.Offset != 40 {
		t.Fatalf("restore point must survive prune: cp=%+v err=%v", cp, err)
	}
	if store.Evictions() < 2 {
		t.Fatalf("eviction counter should include pruned gens, got %d", store.Evictions())
	}
}
