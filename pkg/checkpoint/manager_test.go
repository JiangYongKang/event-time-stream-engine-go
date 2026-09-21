package checkpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sampleState() *State {
	return &State{
		ID: "cp-1", CreatedAt: time.Now().UTC(),
		Watermark:      time.Unix(100, 0).UTC(),
		PartitionTimes: map[int]time.Time{0: time.Unix(100, 0).UTC()},
		Offsets:        map[int]int64{0: 42},
		Windows:        []WindowState{{WindowID: "w1", Key: "k", Start: time.Unix(0, 0).UTC(), End: time.Unix(60, 0).UTC(), Count: 3, Sum: 9}},
		SeenIDs:        map[string][]string{"w1": {"e1", "e2", "e3"}},
		AckedResultIDs: []string{"old-result"},
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	st := sampleState()
	if err := m.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	got, err := m.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "cp-1" || got.Offsets[0] != 42 || len(got.Windows) != 1 ||
		got.Windows[0].Count != 3 || got.Windows[0].Sum != 9 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	t.Logf("checkpoint loaded: id=%s offsets=%v windows=%+v", got.ID, got.Offsets, got.Windows)
}

func TestLoadEmptyDirIsNoCheckpoint(t *testing.T) {
	m := NewManager(t.TempDir())
	_, err := m.Load(context.Background())
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
}

func TestCorruptDataChecksum(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	if err := m.Save(context.Background(), sampleState()); err != nil {
		t.Fatal(err)
	}
	// 翻转数据文件中部的一个字节（避开结尾空白）。
	mfInfo, merr := ReadManifest(dir)
	if merr != nil {
		t.Fatal(merr)
	}
	p := filepath.Join(dir, mfInfo.DataFile)
	b, _ := os.ReadFile(p)
	if len(b) < 16 {
		t.Fatalf("data file too short: %d", len(b))
	}
	b[10] ^= 0xFF
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := m.Load(context.Background())
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("want ErrCorruptData, got %v", err)
	}
	t.Logf("corrupt data rejected distinctly: %v", err)
}

func TestCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	if err := m.Save(context.Background(), sampleState()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := m.Load(context.Background())
	if !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("want ErrCorruptManifest, got %v", err)
	}
	t.Logf("corrupt manifest rejected distinctly: %v", err)
}

func TestDanglingDataWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data-orphan.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(dir)
	_, err := m.Load(context.Background())
	if !errors.Is(err, ErrDanglingCheckpoint) {
		t.Fatalf("want ErrDanglingCheckpoint, got %v", err)
	}
}

func TestManifestMissingData(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	if err := m.Save(context.Background(), sampleState()); err != nil {
		t.Fatal(err)
	}
	// 删除清单指向的数据文件。
	mfInfo, merr := ReadManifest(dir)
	if merr != nil {
		t.Fatal(merr)
	}
	if err := os.Remove(filepath.Join(dir, mfInfo.DataFile)); err != nil {
		t.Fatal(err)
	}
	_, err := m.Load(context.Background())
	if !errors.Is(err, ErrInconsistent) {
		t.Fatalf("want ErrInconsistent, got %v", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	st := sampleState()
	st.FormatVersion = FormatVersion + 1 // 未来/不兼容版本
	// 直接落盘并伪造清单，绕过 Save 的版本归一。
	if err := writeFutureVersion(dir, st); err != nil {
		t.Fatal(err)
	}
	_, err := m.Load(context.Background())
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error should explain version problem: %v", err)
	}
	t.Logf("stale/incompatible version rejected distinctly: %v", err)
}
