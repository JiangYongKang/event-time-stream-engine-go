package watermark

import (
	"testing"
	"time"
)

func sec(n int64) time.Time { return time.Unix(n, 0).UTC() }

func TestMinAcrossPartitionsAndMonotonic(t *testing.T) {
	tr := NewTracker(5*time.Second, 10*time.Second)
	tr.RegisterPartitions(0, 1)
	tr.Observe(0, sec(100)) // wm=95
	tr.Observe(1, sec(50))  // min=50 -> wm=45
	if got := tr.Watermark(); !got.Equal(sec(45)) {
		t.Fatalf("watermark = %v, want 45", got)
	}
	// 慢分区推进 -> wm 前进
	tr.Observe(1, sec(120)) // min=100 -> wm=95
	if got := tr.Watermark(); !got.Equal(sec(95)) {
		t.Fatalf("watermark = %v, want 95", got)
	}
	// 乱序（更早）事件不得让水位后退
	tr.Observe(0, sec(60))
	if got := tr.Watermark(); !got.Equal(sec(95)) {
		t.Fatalf("watermark regressed to %v", got)
	}
}

func TestLatenessDecision(t *testing.T) {
	tr := NewTracker(0, 10*time.Second)
	tr.RegisterPartitions(0)
	tr.Observe(0, sec(100)) // wm=100
	// 90+10=100 不在 wm 之后 -> 边界：不算迟到（< 才迟到）
	if late, _ := tr.IsLate(sec(90)); late {
		t.Fatal("ts=90 with allowed=10 and wm=100 must NOT be late (boundary inclusive)")
	}
	// 89+10=99 < 100 -> 迟到
	if late, wm := tr.IsLate(sec(89)); !late {
		t.Fatalf("ts=89 must be late, wm=%v", wm)
	}
	// 水位线之前但在允许范围内 = 乱序而非迟到
	if !tr.IsBeyondWatermark(sec(95)) {
		t.Fatal("ts=95 is behind watermark 100")
	}
	if late, _ := tr.IsLate(sec(95)); late {
		t.Fatal("ts=95 within allowed lateness must be accepted")
	}
}

func TestRestoreRecomputesWatermark(t *testing.T) {
	tr := NewTracker(0, 0)
	tr.RegisterPartitions(0, 1)
	tr.Restore(map[int]time.Time{0: sec(100), 1: sec(80)})
	if got := tr.Watermark(); !got.Equal(sec(80)) {
		t.Fatalf("restored watermark = %v, want 80", got)
	}
}
