package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileSourceOffsetReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p0.jsonl")
	content := `{"id":"a","key":"k","event_time":"2026-09-22T10:00:02Z","value":1,"seq":1}
{"id":"b","key":"k","event_time":"2026-09-22T10:00:08Z","value":2,"seq":2}
{"id":"c","key":"k","event_time":"2026-09-22T10:00:50Z","value":3,"seq":3}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileSource(map[int]string{0: path})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	batch1, off1, err := src.Read(ctx, 0, 0, 2)
	if err != nil || len(batch1) != 2 {
		t.Fatalf("first read: %d events err=%v", len(batch1), err)
	}
	if batch1[0].ID != "a" || batch1[1].ID != "b" ||
		!batch1[0].EventTime.Equal(time.Date(2026, 9, 22, 10, 0, 2, 0, time.UTC)) {
		t.Fatalf("batch1 parse wrong: %+v", batch1)
	}
	// 从检查点位点 off1 精确回放：应得到 c。
	batch2, off2, err := src.Read(ctx, 0, off1, 2)
	if err != nil || len(batch2) != 1 || batch2[0].ID != "c" {
		t.Fatalf("resume from offset %d must yield only c: %+v err=%v", off1, batch2, err)
	}
	// 再读越界 -> 空、位点不变。
	batch3, off3, err := src.Read(ctx, 0, off2, 2)
	if err != nil || len(batch3) != 0 || off3 != off2 {
		t.Fatalf("read past end must be empty/stable: %+v off=%d", batch3, off3)
	}
	// 从位 0 再读，得到完全相同的 a（确定性回放）。
	replay, _, err := src.Read(ctx, 0, 0, 1)
	if err != nil || len(replay) != 1 || replay[0].ID != "a" {
		t.Fatalf("replay from 0 must return a: %+v", replay)
	}
	t.Logf("offset boundary: after a,b -> %d; resumed c, then stable at %d", off1, off2)
}
