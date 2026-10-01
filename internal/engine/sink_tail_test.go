package engine

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// writeLine 以当前封装格式写一条完整记录行。
func writeLine[T any](t *testing.T, path string, rec T) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := encodeSinkLine(rec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
}

// appendRaw 原样追加字节（用于制造残行 / 损坏行）。
func appendRaw(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(raw); err != nil {
		t.Fatal(err)
	}
}

// TestRepairTailsTrimsPartialLastLine：无换行的半截内容是残行，必须被裁掉。
func TestRepairTailsTrimsPartialLastLine(t *testing.T) {
	dir := t.TempDir()
	rp := filepath.Join(dir, "r.jsonl")
	lp := filepath.Join(dir, "l.jsonl")
	sink := NewFileSink(rp, lp)

	writeLine(t, rp, WindowResult{Key: "a", WindowStart: 0, WindowEnd: 60_000, Count: 1, Sum: 1, CheckpointID: 1})
	appendRaw(t, rp, []byte(`{"key":"b","window_start":1`)) // 半截、无换行

	rep, err := sink.RepairTails()
	if err != nil {
		t.Fatalf("partial tail must be repairable, got: %v", err)
	}
	if !rep.ResultsTrim || rep.LateTrim {
		t.Fatalf("report mismatch: %+v", rep)
	}
	got, err := sink.ReadResults()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("want only the complete record, got %+v", got)
	}

	// 再跑一次：已经没有残行，报告应为 no-op。
	rep2, err := sink.RepairTails()
	if err != nil || rep2.Trimmed() {
		t.Fatalf("second repair should be no-op: rep=%+v err=%v", rep2, err)
	}
}

// TestRepairTailsRejectsGenuinelyCorruptCompleteLine：
// 完整（有换行）但内容损坏的行必须报 ErrSinkCorrupt，绝不能当残行裁掉。
func TestRepairTailsRejectsGenuinelyCorruptCompleteLine(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{
			name: "unparseable full line",
			raw:  []byte("{not-json\n"),
		},
		{
			name: "full line with wrong crc",
			raw: func() []byte {
				b, _ := json.Marshal(sinkEnvelope{
					Version: sinkLineVersion,
					CRC:     12345, // 故意错误
					Payload: json.RawMessage(`{"key":"a","window_start":0,"window_end":60000,"count":1,"sum":1,"checkpoint_id":1}`),
				})
				return append(b, '\n')
			}(),
		},
		{
			name: "full line with unknown version",
			raw: func() []byte {
				p := json.RawMessage(`{}`)
				b, _ := json.Marshal(sinkEnvelope{Version: 99, CRC: crc32Of(p), Payload: p})
				return append(b, '\n')
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			rp := filepath.Join(dir, "r.jsonl")
			sink := NewFileSink(rp, filepath.Join(dir, "l.jsonl"))
			writeLine(t, rp, WindowResult{Key: "a", CheckpointID: 1})
			appendRaw(t, rp, tc.raw)

			_, err := sink.RepairTails()
			if !errors.Is(err, ErrSinkCorrupt) {
				t.Fatalf("want ErrSinkCorrupt, got %v", err)
			}
			// 真损坏：文件必须原样保留，不得静默裁掉“完整”的坏行。
			data, rerr := os.ReadFile(rp)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if len(data) == 0 || data[len(data)-1] != '\n' {
				t.Fatal("corrupt file must be left untouched")
			}
			t.Logf("corruption rejected distinctly: %v", err)
		})
	}
}

// TestRepairTailsAcceptsLegacyBareLines：老版本写出的裸记录行（无 CRC 封装）
// 依旧可读；只有其后的残行才会被裁掉。
func TestRepairTailsAcceptsLegacyBareLines(t *testing.T) {
	dir := t.TempDir()
	rp := filepath.Join(dir, "r.jsonl")
	sink := NewFileSink(rp, filepath.Join(dir, "l.jsonl"))
	legacy := []byte(`{"key":"old","window_start":0,"window_end":60000,"count":7,"sum":7.5,"checkpoint_id":1}` + "\n")
	appendRaw(t, rp, legacy)
	appendRaw(t, rp, []byte(`{"key":"torn"`)) // 残行

	rep, err := sink.RepairTails()
	if err != nil {
		t.Fatalf("legacy line + partial tail: %v", err)
	}
	if !rep.ResultsTrim {
		t.Fatal("expected torn bytes trimmed")
	}
	got, err := sink.ReadResults()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "old" || got[0].Count != 7 {
		t.Fatalf("legacy record must remain readable: %+v", got)
	}

	// 老格式裸行但内容本身是坏 JSON 且有换行：仍然算真损坏。
	lp := filepath.Join(dir, "l2.jsonl")
	appendRaw(t, lp, []byte("{garbage\n"))
	if _, err := NewFileSink(filepath.Join(dir, "r2.jsonl"), lp).RepairTails(); !errors.Is(err, ErrSinkCorrupt) {
		t.Fatalf("complete garbage line must be ErrSinkCorrupt, got %v", err)
	}
}

func crc32Of(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}

// TestTornWriteFaultLeavesRepairableHalfLine：撕裂写故障在文件末尾留下
// 无换行的半截封装行；RepairTails 能裁掉，随后同批补齐、不丢不重。
func TestTornWriteFaultLeavesRepairableHalfLine(t *testing.T) {
	dir := t.TempDir()
	sink := NewFileSink(filepath.Join(dir, "r.jsonl"), filepath.Join(dir, "l.jsonl"))
	results := []WindowResult{
		{Key: "a", WindowStart: 0, WindowEnd: 60_000, Count: 1, Sum: 1},
		{Key: "b", WindowStart: 0, WindowEnd: 60_000, Count: 2, Sum: 2},
		{Key: "c", WindowStart: 0, WindowEnd: 60_000, Count: 3, Sum: 3},
	}
	// 第 1 条完整写出后，第 2 条撕裂（落盘一半字节、无换行）。
	sink.FailNextTornWrites(1, 1, 0)
	if _, err := sink.CommitBatch(5, results, nil); !errors.Is(err, ErrSinkFault) {
		t.Fatalf("want ErrSinkFault, got %v", err)
	}

	// 撕裂现场：末尾无换行，直接 ReadResults 必须报损坏（读到残行）。
	data, err := os.ReadFile(filepath.Join(dir, "r.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if data[len(data)-1] == '\n' {
		t.Fatal("torn write must leave file without trailing newline")
	}
	if _, err := sink.ReadResults(); err == nil {
		t.Fatal("reading a file with a torn tail must fail before repair")
	}

	// 重启等价路径：修复 + 同批续写。
	rep, err := sink.CommitBatch(5, results, nil)
	if err != nil {
		t.Fatalf("resume after torn write: %v", err)
	}
	if !rep.RepairedTail || rep.ReplayedResults != 1 || rep.WrittenResults != 2 {
		t.Fatalf("resume report mismatch: %+v", rep)
	}
	got, err := sink.ReadResults()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 results, got %d", len(got))
	}
	// 再提交一次：全部跳过，幂等。
	rep2, err := sink.CommitBatch(5, results, nil)
	if err != nil || rep2.WrittenResults != 0 || rep2.ReplayedResults != 3 {
		t.Fatalf("idempotent: %+v err=%v", rep2, err)
	}
}
