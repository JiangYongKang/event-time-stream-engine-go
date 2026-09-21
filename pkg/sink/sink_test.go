package sink

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

func mkResult(id string, sum int64) model.Result {
	return model.Result{ID: id, Key: "k", Start: time.Unix(0, 0).UTC(), End: time.Unix(60, 0).UTC(), Count: 1, Sum: sum}
}

func TestMemorySinkIdempotent(t *testing.T) {
	s := NewMemorySink()
	ctx := context.Background()
	if existed, _ := s.WriteResult(ctx, mkResult("w1", 5)); existed {
		t.Fatal("first write must not be duplicate")
	}
	if existed, _ := s.WriteResult(ctx, mkResult("w1", 5)); !existed {
		t.Fatal("second write with same id must be idempotent (existed=true)")
	}
	if got := s.Results(); len(got) != 1 || got[0].Sum != 5 {
		t.Fatalf("result must appear exactly once: %+v", got)
	}
}

func TestFileSinkReopenDedup(t *testing.T) {
	dir := t.TempDir()
	rp := filepath.Join(dir, "results.jsonl")
	lp := filepath.Join(dir, "late.jsonl")

	s1, err := NewFileSink(rp, lp)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s1.WriteResult(ctx, mkResult("w1", 5)); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟新进程重新打开：历史结果必须被重建为幂等索引。
	s2, err := NewFileSink(rp, lp)
	if err != nil {
		t.Fatal(err)
	}
	existed, err := s2.WriteResult(ctx, mkResult("w1", 5))
	if err != nil || !existed {
		t.Fatalf("reopen must dedup historical result: existed=%v err=%v", existed, err)
	}
	if got := s2.Results(); len(got) != 1 {
		t.Fatalf("reopened sink must have exactly 1 result, got %d", len(got))
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开核对：文件中物理行必须只有一行。
	s3, _ := NewFileSink(rp, lp)
	defer s3.Close()
	if len(s3.Results()) != 1 {
		t.Fatalf("physical file must contain one result line, got %d", len(s3.Results()))
	}
	t.Log("file sink rebuilds idempotent index after reopen; no duplicate physical line")
}
