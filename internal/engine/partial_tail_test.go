package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// appendRaw 以 append 方式往文件追加裸字节（文件不存在则创建）。
func appendRaw(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
}

// fragmentOf 把一条业务记录编码成当前封装行后，从中间截断成“没写完的半截”
// （无换行结束），精确模拟进程在 write() 中途被强杀后文件末尾的残行。
func fragmentOf[T any](t *testing.T, rec T) []byte {
	t.Helper()
	full, err := encodeRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	cut := len(full) * 2 / 5
	if cut < 1 {
		cut = 1
	}
	return full[:cut]
}

func mustLatestCheckpoint(t *testing.T, rig testRig) *Checkpoint {
	t.Helper()
	cp, err := NewCheckpointStore(rig.cpPath).Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	return cp
}

func sortedLateIDs(lates []LateRecord) []string {
	out := make([]string, 0, len(lates))
	for _, l := range lates {
		out = append(out, l.Event.ID)
	}
	sort.Strings(out)
	return out
}

func assertSameLates(t *testing.T, want, got []LateRecord) {
	t.Helper()
	w, g := sortedLateIDs(want), sortedLateIDs(got)
	if len(w) != len(g) {
		t.Fatalf("late records count: want %d got %d", len(w), len(g))
	}
	for i := range w {
		if w[i] != g[i] {
			t.Fatalf("late records differ at %d: want %v got %v", i, w, g)
		}
	}
}

// crashHookAt 返回一个 ProcessedHook：processed>=n 时取消 ctx（模拟强杀）。
func crashHookAt(n uint64, cancel context.CancelFunc) func(uint64) {
	return func(seen uint64) {
		if seen >= n {
			cancel()
		}
	}
}

// partialCfg 与 resCfg 相同，但允许迟到只有 2s：配合 partialEvents
// 的大幅度乱序回跳，会稳定产生迟到记录，便于覆盖 late.jsonl 的残行场景。
func partialCfg(t *testing.T) Config {
	cfg := resCfg(t)
	cfg.AllowedLatenessMs = 2_000
	return cfg
}

// partialEvents 构造确定性输入：整体单调推进、每 19 条插入一条大幅回跳的
// 旧事件（相对当前水位确定迟到），并保留少量重复事件。
func partialEvents(n int) []Event {
	var events []Event
	keys := []string{"a", "b", "c"}
	for i := 0; i < n; i++ {
		ev := Event{
			ID:        fmt.Sprintf("pe-%03d", i),
			Key:       keys[i%3],
			EventTime: int64(i) * 1_000,
			Value:     float64(i%10) + 0.5,
		}
		events = append(events, ev)
		if i%19 == 18 {
			// 回跳到 30s 前：此前水位约 i*1000-2000，该事件确定迟到。
			events = append(events, Event{
				ID:        fmt.Sprintf("pe-late-%03d", i),
				Key:       keys[i%3],
				EventTime: int64(i)*1_000 - 30_000,
				Value:     1.25,
			})
		}
		if i%40 == 39 {
			events = append(events, events[0]) // 重复（会被去重）
		}
	}
	return events
}

// TestRecoverPartialTailAfterKill：写出一批窗口结果/迟到记录中途被强杀，
// 输出文件末尾留下半截残行。重启必须认出残行并丢弃、从最近一致检查点补齐，
// 然后再整体重跑一次也不得重复写；窗口归属/计数/求和、迟到记录与无故障基线逐窗口一致。
// 结果文件、迟到文件、两个文件同时留残行三种情形分别覆盖。
func TestRecoverPartialTailAfterKill(t *testing.T) {
	events := partialEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, partialCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}
	baseResults := indexResults(t, base.results(t))
	baseLates := base.lates(t)
	if len(baseLates) == 0 {
		t.Fatal("test setup: baseline produced no late records; adjust input")
	}

	cases := []struct {
		name         string
		inResults    bool
		inLates      bool
		fragmentCpID func(cp *Checkpoint) uint64 // 残行所属批次（未确认=cp+1）
	}{
		{name: "results_only", inResults: true, fragmentCpID: func(cp *Checkpoint) uint64 { return cp.ID + 1 }},
		{name: "lates_only", inLates: true, fragmentCpID: func(cp *Checkpoint) uint64 { return cp.ID + 1 }},
		{name: "both_files", inResults: true, inLates: true, fragmentCpID: func(cp *Checkpoint) uint64 { return cp.ID + 1 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRig(t)
			writeEvents(t, rig.inputPath, events)

			// 第一轮：跑到中段强杀，留下至少一份一致检查点。
			cfg := partialCfg(t)
			ctx, cancel := context.WithCancel(context.Background())
			cfg.ProcessedHook = crashHookAt(120, cancel)
			if _, err := rig.runOnce(t, cfg, ctx); err == nil {
				t.Fatal("expected killed first run to end with error")
			}

			cp := mustLatestCheckpoint(t, rig)
			fragCp := tc.fragmentCpID(cp)

			// 直接在输出文件末尾追加“写到一半、无换行”的残行（确定性，不靠时序）。
			if tc.inResults {
				frag := fragmentOf(t, WindowResult{
					Key: "z-killed", WindowStart: 123_456_000, WindowEnd: 123_516_000,
					Count: 7, Sum: 77, CheckpointID: fragCp,
				})
				appendRaw(t, rig.resultsPath, frag)
			}
			if tc.inLates {
				frag := fragmentOf(t, LateRecord{
					Event:        Event{ID: "z-killed-late", Key: "a", EventTime: 1, Value: 1},
					Reason:       "killed mid-write",
					Watermark:    2,
					CheckpointID: fragCp,
				})
				appendRaw(t, rig.latePath, frag)
			}

			// 第二轮（=“重启”）：不得一启动就失败，必须自动修补残行并补齐。
			var logs bytes.Buffer
			r2cfg := partialCfg(t)
			r2cfg.Logger = log.New(&logs, "", 0)
			st2, err := rig.runOnce(t, r2cfg, context.Background())
			if err != nil {
				t.Fatalf("restart with partial tail must recover, got error: %v", err)
			}
			if !strings.Contains(logs.String(), "SINK TAIL REPAIR") {
				t.Fatalf("logs must show partial-tail repair; logs:\n%s", logs.String())
			}
			if strings.Contains(logs.String(), "SINK CORRUPT") {
				t.Fatalf("repairable tail must not be reported as corrupt; logs:\n%s", logs.String())
			}
			if st2.TailRepairs == 0 {
				t.Fatal("TailRepairs metric should be >0 after repairing a killed mid-write tail")
			}
			assertSameWindows(t, baseResults, indexResults(t, rig.results(t)))
			assertSameLates(t, baseLates, rig.lates(t))

			// 残行里那条“未确认”的假数据绝不能残留在最终结果里。
			for _, r := range rig.results(t) {
				if r.Key == "z-killed" {
					t.Fatal("partial-tail record leaked into results")
				}
			}
			for _, l := range rig.lates(t) {
				if l.Event.ID == "z-killed-late" {
					t.Fatal("partial-tail record leaked into late output")
				}
			}

			// 第三轮：补完后整体重跑，任何一批都不得重复写。
			st3, err := rig.runOnce(t, partialCfg(t), context.Background())
			if err != nil {
				t.Fatalf("re-run after recovery: %v", err)
			}
			if st3.TailRepairs != 0 {
				t.Fatalf("no partial tail should remain for a third run, got repairs=%d", st3.TailRepairs)
			}
			assertSameWindows(t, baseResults, indexResults(t, rig.results(t)))
			assertSameLates(t, baseLates, rig.lates(t))
		})
	}
}

// TestRecoverPartialTailFirstRunNoCheckpoint：第一次运行、还没存下任何检查点
// 就在写出途中被杀，输出文件只有残行；重启不得报错，清空未确认输出后从头跑齐。
func TestRecoverPartialTailFirstRunNoCheckpoint(t *testing.T) {
	events := partialEvents(200)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	// 没有任何检查点文件，只有两个输出文件末尾的残行。
	appendRaw(t, rig.resultsPath, fragmentOf(t, WindowResult{
		Key: "ghost", WindowStart: 1, WindowEnd: 2, Count: 9, Sum: 9, CheckpointID: 1,
	}))
	appendRaw(t, rig.latePath, fragmentOf(t, LateRecord{
		Event: Event{ID: "ghost-late"}, Reason: "ghost", Watermark: 1, CheckpointID: 1,
	}))

	var logs bytes.Buffer
	cfg := resCfg(t)
	cfg.Logger = log.New(&logs, "", 0)
	st, err := rig.runOnce(t, cfg, context.Background())
	if err != nil {
		t.Fatalf("fresh start over a partial tail must succeed, got: %v", err)
	}
	if st.TailRepairs == 0 {
		t.Fatal("expected TailRepairs>0 when cleaning first-run partial output")
	}
	if !strings.Contains(logs.String(), "SINK TAIL REPAIR") {
		t.Fatalf("logs must show tail repair; logs:\n%s", logs.String())
	}
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
	assertSameLates(t, base.lates(t), rig.lates(t))
	for _, r := range rig.results(t) {
		if r.Key == "ghost" {
			t.Fatal("ghost partial record survived fresh start")
		}
	}
}

// TestRecoverPartialTailOfConfirmedBatch：最近检查点记录的已确认批次在
// 上一次恢复重放时又被杀（残行属于该检查点 ID 本身）。重启回滚不删这批、
// 修补残行后由批次重放补齐缺失记录，不丢不重。
func TestRecoverPartialTailOfConfirmedBatch(t *testing.T) {
	events := partialEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, partialCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := partialCfg(t)
	cfg.CheckpointEvery = 20
	// 确定性触发：在“携带已收割结果批次”的那一代检查点成功保存之后，
	// 由引擎在检查点屏障内同步取消。被杀时点精确锚定在该代之后，
	// 不依赖处理条数、队列调度或任何时序。
	cfg.CheckpointHook = func(cp Checkpoint) {
		if len(cp.PendingResults) > 0 {
			cancel()
		}
	}
	if _, err := rig.runOnce(t, cfg, ctx); err == nil {
		t.Fatal("expected killed run to fail")
	}

	cp := mustLatestCheckpoint(t, rig)
	if len(cp.PendingResults) == 0 {
		t.Fatal("test setup: latest checkpoint carries no committed result batch")
	}
	// 残行伪装成该确认批次中某条记录写到一半（属于 cp.ID，而非 cp.ID+1）。
	target := cp.PendingResults[0]
	target.CheckpointID = cp.ID
	appendRaw(t, rig.resultsPath, fragmentOf(t, target))

	st, err := rig.runOnce(t, partialCfg(t), context.Background())
	if err != nil {
		t.Fatalf("restart must repair confirmed-batch partial tail and replay: %v", err)
	}
	if st.TailRepairs == 0 {
		t.Fatal("expected tail repair for killed replay")
	}
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
	assertSameLates(t, base.lates(t), rig.lates(t))

	// 再跑一次：幂等，不重复。
	if _, err := rig.runOnce(t, partialCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}
	assertSameWindows(t, indexResults(t, base.results(t)), indexResults(t, rig.results(t)))
}

// TestSinkPartialTailVsTrueCorruption：严格区分“可修补残行”和“真正损坏”。
//   - 末尾无换行的半截（哪怕恰好是合法 JSON 片段）-> 自动裁掉；
//   - 一条以换行结束的完整记录无法解析 / CRC 对不上 -> ErrSinkCorrupt，绝不硬接。
func TestSinkPartialTailVsTrueCorruption(t *testing.T) {
	dir := t.TempDir()
	rp := filepath.Join(dir, "r.jsonl")
	lp := filepath.Join(dir, "l.jsonl")

	good, err := encodeRecord(WindowResult{Key: "a", WindowStart: 0, WindowEnd: 1, Count: 1, Sum: 1, CheckpointID: 1})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("unterminated_json_object_is_repaired", func(t *testing.T) {
		sink := NewFileSink(rp, lp)
		// 残行恰好是一个完整合法 JSON、但没有换行（写了记录没来得及写换行就被杀）。
		legitButUnterminated := []byte(`{"key":"tail","window_start":2,"window_end":3,"count":1,"sum":1,"checkpoint_id":2}`)
		if err := os.WriteFile(rp, append(append(good, '\n'), legitButUnterminated...), 0o644); err != nil {
			t.Fatal(err)
		}
		rep, err := sink.CommitBatch(3, nil, nil)
		if err != nil {
			t.Fatalf("unterminated tail must be repaired, got: %v", err)
		}
		if !rep.RepairedTail {
			t.Fatal("RepairedTail should be true")
		}
		got, err := sink.ReadResults()
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Key != "a" {
			t.Fatalf("only the confirmed record should remain, got %+v", got)
		}
	})

	t.Run("complete_line_bad_json_is_corrupt", func(t *testing.T) {
		sink := NewFileSink(filepath.Join(dir, "badjson.jsonl"), lp)
		data := append(append(good, '\n'), []byte("{this is not json\n")...)
		if err := os.WriteFile(filepath.Join(dir, "badjson.jsonl"), data, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := sink.CommitBatch(3, nil, nil); !errors.Is(err, ErrSinkCorrupt) {
			t.Fatalf("complete unparseable line: want ErrSinkCorrupt, got %v", err)
		}
	})

	t.Run("complete_line_crc_mismatch_is_corrupt", func(t *testing.T) {
		p := filepath.Join(dir, "badcrc.jsonl")
		env, err := encodeRecord(WindowResult{Key: "b", WindowStart: 0, WindowEnd: 1, Count: 2, Sum: 2, CheckpointID: 1})
		if err != nil {
			t.Fatal(err)
		}
		// 翻转封装行中间一个字节：行完整（有换行），但 CRC 必然对不上。
		env[len(env)/2] ^= 0xFF
		data := append(append(append(good, '\n'), env...), '\n')
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		sink := NewFileSink(p, lp)
		if _, err := sink.CommitBatch(3, nil, nil); !errors.Is(err, ErrSinkCorrupt) {
			t.Fatalf("checksum mismatch on a complete line: want ErrSinkCorrupt, got %v", err)
		}
		// 真正损坏时文件不得被裁剪（坏行及其后数据不能被静默丢弃）。
		raw, _ := os.ReadFile(p)
		if !bytes.Contains(raw, env) {
			t.Fatal("corrupt line must not be truncated away")
		}
		if _, err := sink.TruncateTo(9); !errors.Is(err, ErrSinkCorrupt) {
			t.Fatalf("TruncateTo on corrupt file: want ErrSinkCorrupt, got %v", err)
		}
	})

	t.Run("read_distinguishes_corrupt_and_partial", func(t *testing.T) {
		// 只读路径：残尾 -> ErrSinkPartialTail；完整坏行 -> ErrSinkCorrupt。
		p1 := filepath.Join(dir, "readtail.jsonl")
		if err := os.WriteFile(p1, append(good, []byte(`{"key":"x"`)...), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readAll[WindowResult](p1); !errors.Is(err, ErrSinkPartialTail) {
			t.Fatalf("read partial tail: want ErrSinkPartialTail, got %v", err)
		}
		p2 := filepath.Join(dir, "readbad.jsonl")
		if err := os.WriteFile(p2, append(append(good, '\n'), []byte("###notjson###\n")...), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readAll[WindowResult](p2); !errors.Is(err, ErrSinkCorrupt) {
			t.Fatalf("read corrupt line: want ErrSinkCorrupt, got %v", err)
		}
	})
}

// TestRecoverRejectsTrueCorruptSinkFile：引擎重启遇到“完整记录已损坏”时，
// 必须明确失败退出、日志能看出 CORRUPT 判定，且不能当成残行静默修掉。
func TestRecoverRejectsTrueCorruptSinkFile(t *testing.T) {
	events := partialEvents(200)

	makeRig := func(t *testing.T) (testRig, Config, *Checkpoint) {
		rig := newRig(t)
		writeEvents(t, rig.inputPath, events)
		ctx, cancel := context.WithCancel(context.Background())
		cfg := resCfg(t)
		cfg.ProcessedHook = crashHookAt(100, cancel)
		if _, err := rig.runOnce(t, cfg, ctx); err == nil {
			t.Fatal("expected killed run")
		}
		return rig, resCfg(t), mustLatestCheckpoint(t, rig)
	}

	t.Run("corrupt_results", func(t *testing.T) {
		rig, _, cp := makeRig(t)
		// 在 results 文件里插入一条“完整换行结束但 CRC 错误”的记录。
		bad, err := encodeRecord(WindowResult{Key: "evil", WindowStart: 1, WindowEnd: 2, CheckpointID: cp.ID + 1})
		if err != nil {
			t.Fatal(err)
		}
		bad[len(bad)/2] ^= 0xFF
		appendRaw(t, rig.resultsPath, append(bad, '\n'))

		var logs bytes.Buffer
		cfg := resCfg(t)
		cfg.Logger = log.New(&logs, "", 0)
		if _, err := rig.runOnce(t, cfg, context.Background()); !errors.Is(err, ErrSinkCorrupt) {
			t.Fatalf("want ErrSinkCorrupt on damaged results file, got %v", err)
		}
		if !strings.Contains(logs.String(), "SINK CORRUPT") {
			t.Fatalf("logs must identify true corruption; logs:\n%s", logs.String())
		}
	})

	t.Run("corrupt_lates", func(t *testing.T) {
		rig, _, cp := makeRig(t)
		bad, err := encodeRecord(LateRecord{
			Event: Event{ID: "evil-late"}, Reason: "x", CheckpointID: cp.ID + 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		bad[len(bad)/2] ^= 0xFF
		appendRaw(t, rig.latePath, append(bad, '\n'))

		var logs bytes.Buffer
		cfg := resCfg(t)
		cfg.Logger = log.New(&logs, "", 0)
		if _, err := rig.runOnce(t, cfg, context.Background()); !errors.Is(err, ErrSinkCorrupt) {
			t.Fatalf("want ErrSinkCorrupt on damaged late file, got %v", err)
		}
		if !strings.Contains(logs.String(), "SINK CORRUPT") {
			t.Fatalf("logs must identify true corruption; logs:\n%s", logs.String())
		}
	})
}

// TestEnvelopeRoundTripAndLegacyRead：新封装（带 CRC）往返正确，
// 旧版本裸 JSON 行仍可正常读取（兼容老结果文件）。
func TestEnvelopeRoundTripAndLegacyRead(t *testing.T) {
	dir := t.TempDir()
	rp := filepath.Join(dir, "r.jsonl")
	lp := filepath.Join(dir, "l.jsonl")
	sink := NewFileSink(rp, lp)
	results := []WindowResult{
		{Key: "a", WindowStart: 0, WindowEnd: 60_000, Count: 3, Sum: 4.5, CheckpointID: 5},
	}
	lates := []LateRecord{{Event: Event{ID: "l1"}, Reason: "late", Watermark: 9, CheckpointID: 5}}
	if err := sink.Commit(5, results, lates); err != nil {
		t.Fatal(err)
	}
	got, err := sink.ReadResults()
	if err != nil || len(got) != 1 || got[0].Key != "a" || got[0].CheckpointID != 5 {
		t.Fatalf("envelope round trip results: %+v err=%v", got, err)
	}
	gl, err := sink.ReadLates()
	if err != nil || len(gl) != 1 || gl[0].Event.ID != "l1" {
		t.Fatalf("envelope round trip lates: %+v err=%v", gl, err)
	}

	// 追加一行旧版本裸 JSON：必须能读，也必须能在续写时被识别为已存在。
	legacy := []byte(fmt.Sprintf(`{"key":"old","window_start":0,"window_end":1,"count":1,"sum":1,"checkpoint_id":6}` + "\n"))
	appendRaw(t, rp, legacy)
	got, err = sink.ReadResults()
	if err != nil {
		t.Fatalf("legacy line must remain readable: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 results incl. legacy, got %d", len(got))
	}
	rep, err := sink.CommitBatch(6, []WindowResult{
		{Key: "old", WindowStart: 0, WindowEnd: 1, Count: 1, Sum: 1, CheckpointID: 6},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReplayedResults != 1 || rep.WrittenResults != 0 {
		t.Fatalf("legacy record should be recognized and skipped on resume: %+v", rep)
	}
}
