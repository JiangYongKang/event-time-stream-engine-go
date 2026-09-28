package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSinkCommitResumeAfterFault(t *testing.T) {
	dir := t.TempDir()
	sink := NewFileSink(filepath.Join(dir, "r.jsonl"), filepath.Join(dir, "l.jsonl"))

	results := []WindowResult{
		{Key: "a", WindowStart: 0, WindowEnd: 60_000, Count: 1, Sum: 1},
		{Key: "a", WindowStart: 60_000, WindowEnd: 120_000, Count: 2, Sum: 5},
		{Key: "b", WindowStart: 0, WindowEnd: 60_000, Count: 3, Sum: 9},
	}
	lates := []LateRecord{
		{Event: Event{ID: "late-1", Key: "a", EventTime: 1}, Reason: "r1", Watermark: 2},
		{Event: Event{ID: "late-2", Key: "a", EventTime: 3}, Reason: "r2", Watermark: 4},
	}

	// 第一次提交：写出 1 条窗口结果后被注入故障（模拟磁盘写失败）。
	sink.FailNextCommitsAfter(1, 1)
	_, err := sink.CommitBatch(7, results, lates)
	if !errors.Is(err, ErrSinkFault) {
		t.Fatalf("want ErrSinkFault, got %v", err)
	}
	got, _ := sink.ReadResults()
	if len(got) != 1 {
		t.Fatalf("after fault: want 1 persisted result, got %d", len(got))
	}
	t.Logf("after fault: %d result(s) persisted (partial batch)", len(got))

	// 第二次提交：续写同一批。已落盘的 1 条必须跳过，只补另外 2 条结果 + 2 条迟到。
	rep, err := sink.CommitBatch(7, results, lates)
	if err != nil {
		t.Fatalf("resume commit: %v", err)
	}
	if rep.ReplayedResults != 1 || rep.WrittenResults != 2 || rep.WrittenLates != 2 {
		t.Fatalf("resume report mismatch: %+v", rep)
	}
	if !rep.Resume {
		t.Fatal("resume report should mark Resume=true")
	}
	t.Logf("resume commit report: %+v", rep)

	// 第三次：重复提交同一批必须全部跳过（幂等），文件不增长。
	rep2, err := sink.CommitBatch(7, results, lates)
	if err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
	if rep2.WrittenResults != 0 || rep2.WrittenLates != 0 ||
		rep2.ReplayedResults != 3 || rep2.ReplayedLates != 2 {
		t.Fatalf("idempotent report mismatch: %+v", rep2)
	}
	got, _ = sink.ReadResults()
	gotL, _ := sink.ReadLates()
	if len(got) != 3 || len(gotL) != 2 {
		t.Fatalf("final files: results=%d want 3, lates=%d want 2", len(got), len(gotL))
	}
}

func TestSinkRepairPartialTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "r.jsonl")
	// 写入一条完整记录 + 一段写到一半的残行（模拟进程在 write 中途被杀）。
	full := []byte(`{"key":"a","window_start":0,"window_end":60000,"count":1,"sum":1,"checkpoint_id":1}` + "\n")
	partial := []byte(`{"key":"b","window_start":0,"window_end":60000`) // 无换行、未闭合
	if err := writeAll(path, append(full, partial...)); err != nil {
		t.Fatal(err)
	}
	sink := NewFileSink(path, filepath.Join(dir, "l.jsonl"))
	rep, err := sink.CommitBatch(2, []WindowResult{
		{Key: "b", WindowStart: 0, WindowEnd: 60_000, Count: 2, Sum: 2},
	}, nil)
	if err != nil {
		t.Fatalf("commit after partial tail: %v", err)
	}
	if !rep.RepairedTail {
		t.Fatal("commit report should mark RepairedTail=true")
	}
	got, _ := sink.ReadResults()
	if len(got) != 2 {
		t.Fatalf("want 2 results after tail repair, got %d", len(got))
	}
	for _, r := range got {
		t.Logf("result: key=%s count=%d cp=%d", r.Key, r.Count, r.CheckpointID)
	}
}

func writeAll(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}
