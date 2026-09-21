package engine

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t}, "", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", string(p))
	return len(p), nil
}

func TestWindowAssign(t *testing.T) {
	a := NewAssigner(60_000)
	cases := []struct {
		ts         int64
		start, end int64
	}{
		{0, 0, 60_000},
		{59_999, 0, 60_000},
		{60_000, 60_000, 120_000},
		{123_456, 120_000, 180_000},
		{-1, -60_000, 0},
	}
	for _, c := range cases {
		w := a.Assign(c.ts)
		t.Logf("assign ts=%d -> [%d,%d)", c.ts, w.Start, w.End)
		if w.Start != c.start || w.End != c.end {
			t.Fatalf("ts=%d: got [%d,%d), want [%d,%d)", c.ts, w.Start, w.End, c.start, c.end)
		}
	}
}

func TestWatermark(t *testing.T) {
	w := NewWatermarkTracker(2, 10_000)
	if got := w.Watermark(); got != -9223372036854775808 {
		t.Fatalf("initial watermark should be MinInt64, got %d", got)
	}
	w.Observe(0, 100_000)
	if got := w.Watermark(); got != -9223372036854775808 {
		t.Fatalf("partition 1 silent: watermark should stay MinInt64, got %d", got)
	}
	w.Observe(1, 50_000)
	if got := w.Watermark(); got != 40_000 {
		t.Fatalf("watermark = min(100000,50000)-10000 = 40000, got %d", got)
	}
	// 水位线只随分区最大事件时间前进。
	w.Observe(0, 90_000) // 小于已见的 100000，不改变
	if got := w.Watermark(); got != 40_000 {
		t.Fatalf("watermark regressed: got %d", got)
	}
	t.Logf("final watermark=%d snapshot=%v", w.Watermark(), w.Snapshot())
}

func TestCheckpointStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStore(filepath.Join(dir, "cp.json"))

	if _, err := store.Load(); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("empty store: want ErrCheckpointNotFound, got %v", err)
	}

	cp := &Checkpoint{
		ID:        3,
		Offset:    1234,
		Watermark: map[int]int64{0: 100, 1: 200},
		Windows:   []WindowState{{Key: "a", Window: Window{Start: 0, End: 60_000}, Count: 5, Sum: 12.5}},
		SeenIDs:   []string{"ev-1", "ev-2"},
	}
	if err := store.Save(cp); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("restored checkpoint id=%d offset=%d windows=%+v", got.ID, got.Offset, got.Windows)
	if got.ID != 3 || got.Offset != 1234 || len(got.Windows) != 1 || got.Windows[0].Count != 5 || len(got.SeenIDs) != 2 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestCheckpointCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	store := NewCheckpointStore(path)
	if err := store.Save(&Checkpoint{ID: 1}); err != nil {
		t.Fatal(err)
	}
	// 篡改文件内容，校验和不再匹配。
	data, _ := os.ReadFile(path)
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := store.Load()
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("want ErrCheckpointCorrupt, got %v", err)
	}
	t.Logf("corrupt checkpoint detected: %v", err)
}

func TestCheckpointVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	store := NewCheckpointStore(path)
	if err := store.Save(&Checkpoint{ID: 1}); err != nil {
		t.Fatal(err)
	}
	// 用未来版本号写一个校验和合法的检查点文件。
	payload, _ := json.Marshal(Checkpoint{ID: 1, Version: checkpointVersion + 1})
	file := checkpointFile{Payload: payload, Checksum: crc32.ChecksumIEEE(payload)}
	data, _ := json.Marshal(file)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := store.Load()
	if !errors.Is(err, ErrCheckpointVersion) {
		t.Fatalf("want ErrCheckpointVersion, got %v", err)
	}
	t.Logf("version mismatch detected: %v", err)
}
