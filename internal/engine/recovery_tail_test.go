package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

// tornResultLine 构造一条窗口结果封装记录的“半截”字节（无换行），
// 模拟进程在写出该行中途被杀。
func tornResultLine(t *testing.T, r WindowResult) []byte {
	t.Helper()
	full, err := encodeSinkLine(r)
	if err != nil {
		t.Fatal(err)
	}
	h := len(full) / 2
	if h < 1 {
		h = 1
	}
	return full[:h]
}

func tornLateLine(t *testing.T, r LateRecord) []byte {
	t.Helper()
	full, err := encodeSinkLine(r)
	if err != nil {
		t.Fatal(err)
	}
	h := len(full) / 2
	if h < 1 {
		h = 1
	}
	return full[:h]
}

func appendFile(t *testing.T, path string, raw []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(raw); err != nil {
		t.Fatal(err)
	}
}

// corruptLastResultLine 读入结果文件，把最后一条完整记录重写为“有换行但 CRC 错误”的坏行。
func corruptLastResultLine(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("read %s: %v", path, err)
	}
	// 直接追加一条完整（有换行）但校验错误的封装行：内容完整却确实损坏。
	p, err := json.Marshal(WindowResult{Key: "ghost", WindowStart: 1, WindowEnd: 2, CheckpointID: 1})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := json.Marshal(sinkEnvelope{Version: sinkLineVersion, CRC: 42, Payload: p})
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, append(bad, '\n'))
}

// crashAt 跑一轮并在处理到第 n 条窗口事件时取消（模拟进程被杀）。
func crashAt(t *testing.T, rig testRig, n uint64) {
	t.Helper()
	cfg := resCfg(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.ProcessedHook = func(seen uint64) {
		if seen >= n {
			cancel()
		}
	}
	if _, err := rig.runOnce(t, cfg, ctx); err == nil {
		t.Fatal("expected crash run to be canceled")
	}
}

// runRecoveryScenarios 覆盖：结果文件残行 / 迟到文件残行 / 两个文件同时残行，
// 重启后从最近一致检查点补齐（不丢不重），再整体重跑一次也不重复写。
func TestRecoveryAfterPartialTornLines(t *testing.T) {
	events := resEvents(300)

	// 无故障基线。
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}
	baseWin := indexResults(t, base.results(t))
	baseLate := len(base.lates(t))

	cases := []struct {
		name     string
		tornRes  bool
		tornLate bool
	}{
		{name: "results partial", tornRes: true},
		{name: "lates partial", tornLate: true},
		{name: "both partial", tornRes: true, tornLate: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRig(t)
			writeEvents(t, rig.inputPath, events)

			// 第一轮：正常跑出若干检查点后“被杀”。
			crashAt(t, rig, 120)

			// 在输出文件末尾制造写到一半的残行（重启前的现场）。
			if tc.tornRes {
				appendFile(t, rig.resultsPath, tornResultLine(t,
					WindowResult{Key: "z", WindowStart: 42, WindowEnd: 102, Count: 9, Sum: 9, CheckpointID: 999}))
			}
			if tc.tornLate {
				appendFile(t, rig.latePath, tornLateLine(t,
					LateRecord{Event: Event{ID: "z-torn"}, Reason: "x", Watermark: 1, CheckpointID: 999}))
			}

			// 重启：必须认出残行、丢掉并从最近检查点补齐，而不是报错退出。
			st, err := rig.runOnce(t, resCfg(t), context.Background())
			if err != nil {
				t.Fatalf("restart after partial tail must succeed, got: %v", err)
			}
			t.Logf("%s recovered: checkpoints=%d processed=%d", tc.name, st.Checkpoints, st.Processed)

			// 逐窗口与无故障基线一致（归属、计数、求和）；迟到条数一致（不丢不重）。
			assertSameWindows(t, baseWin, indexResults(t, rig.results(t)))
			if got := len(rig.lates(t)); got != baseLate {
				t.Fatalf("late count: got %d want %d", got, baseLate)
			}
			// 残行绝不能以幽灵记录形式残留。
			for _, r := range rig.results(t) {
				if r.Key == "z" {
					t.Fatalf("torn ghost record survived repair: %+v", r)
				}
			}
			for _, l := range rig.lates(t) {
				if l.Event.ID == "z-torn" {
					t.Fatal("torn ghost late record survived repair")
				}
			}

			// 再整体重跑一次：同一批结果必须全部跳过，不重复写。
			st2, err := rig.runOnce(t, resCfg(t), context.Background())
			if err != nil {
				t.Fatalf("second run: %v", err)
			}
			assertSameWindows(t, baseWin, indexResults(t, rig.results(t)))
			if got := len(rig.lates(t)); got != baseLate {
				t.Fatalf("late count after rerun: got %d want %d", got, baseLate)
			}
			t.Logf("%s rerun stable: checkpoints=%d", tc.name, st2.Checkpoints)
		})
	}
}

// TestTornWriteFaultAcrossRestart：用真实的“撕裂写”故障（半截字节无换行落盘后失败）
// 驱动第一批写出，随后重启补齐，结果与基线一致。
func TestTornWriteFaultAcrossRestart(t *testing.T) {
	events := resEvents(300)

	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}
	baseWin := indexResults(t, base.results(t))
	baseLate := len(base.lates(t))

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)

	// 第一轮：第一次提交在写出 1 条完整结果后撕裂下一条，制造真实残行；
	// 同时处理到 90 条时取消进程（崩溃发生在写出异常期间）。
	sink := NewFileSink(rig.resultsPath, rig.latePath)
	sink.FailNextTornWrites(100000, 1, 0)
	rig.useSink(sink)
	cfg := resCfg(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.ProcessedHook = func(n uint64) {
		if n >= 90 {
			cancel()
		}
	}
	if _, err := rig.runOnce(t, cfg, ctx); err == nil {
		t.Fatal("expected first run to end with error (torn write / crash)")
	}

	// 重启：换正常 sink，残行被识别裁掉并按检查点批次补齐。
	rig.sink = nil
	if _, err := rig.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatalf("recover after torn write: %v", err)
	}
	assertSameWindows(t, baseWin, indexResults(t, rig.results(t)))
	if got := len(rig.lates(t)); got != baseLate {
		t.Fatalf("late count: got %d want %d", got, baseLate)
	}
}

// TestRecoveryFirstRunNoCheckpointPartialTail：首次运行、还没存下任何检查点就被杀，
// 输出文件留下残行时，重启也要能处理（裁掉未确认残行，从头重放）。
func TestRecoveryFirstRunNoCheckpointPartialTail(t *testing.T) {
	events := resEvents(300)
	base := newRig(t)
	writeEvents(t, base.inputPath, events)
	if _, err := base.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatal(err)
	}
	baseWin := indexResults(t, base.results(t))
	baseLate := len(base.lates(t))

	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)

	// 用撕裂写故障 + 极早取消，制造“还没成功保存检查点”就崩溃的现场。
	sink := NewFileSink(rig.resultsPath, rig.latePath)
	sink.FailNextTornWrites(1, 0, 0) // 第一批第一条就撕裂
	rig.useSink(sink)
	cfg := resCfg(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.ProcessedHook = func(n uint64) {
		if n >= 5 {
			cancel()
		}
	}
	_, _ = rig.runOnce(t, cfg, ctx) // 预期失败/取消，忽略错误

	// 关键断言：此时不应存在任何检查点代。
	store := NewCheckpointStore(rig.cpPath)
	if ids, _ := store.Generations(); len(ids) != 0 {
		t.Fatalf("expected no checkpoint generations, got %v", ids)
	}

	// 重启（sink 恢复正常）：即使有残行也必须从头恢复成功。
	rig.sink = nil
	if _, err := rig.runOnce(t, resCfg(t), context.Background()); err != nil {
		t.Fatalf("fresh restart with torn tail must recover, got: %v", err)
	}
	assertSameWindows(t, baseWin, indexResults(t, rig.results(t)))
	if got := len(rig.lates(t)); got != baseLate {
		t.Fatalf("late count: got %d want %d", got, baseLate)
	}
}

// TestRecoveryRejectsGenuinelyCorruptOutput：完整（有换行）但校验失败的输出行
// 必须被明确判定为损坏并报错，不能当成半截残行裁掉硬接。
func TestRecoveryRejectsGenuinelyCorruptOutput(t *testing.T) {
	events := resEvents(300)
	rig := newRig(t)
	writeEvents(t, rig.inputPath, events)
	crashAt(t, rig, 120)

	corruptLastResultLine(t, rig.resultsPath)

	_, err := rig.runOnce(t, resCfg(t), context.Background())
	if err == nil {
		t.Fatal("genuinely corrupt complete line must fail recovery")
	}
	if !errors.Is(err, ErrSinkCorrupt) {
		t.Fatalf("want ErrSinkCorrupt, got: %v", err)
	}
	t.Logf("corrupt output rejected distinctly from partial tail: %v", err)
}
