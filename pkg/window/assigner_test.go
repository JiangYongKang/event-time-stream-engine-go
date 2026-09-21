package window

import (
	"testing"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

func TestAssignDeterministic(t *testing.T) {
	a := NewAssigner(time.Minute)
	base := time.Unix(0, 0).UTC()
	cases := []struct {
		tsSec    int64
		startSec int64
	}{
		{0, 0}, {1, 0}, {59, 0}, {60, 60}, {61, 60}, {125, 120},
	}
	for _, c := range cases {
		w := a.Assign("k", time.Unix(c.tsSec, 0).UTC())
		if w.Start != base.Add(time.Duration(c.startSec)*time.Second) ||
			!w.End.Equal(w.Start.Add(time.Minute)) {
			t.Fatalf("ts=%d got window [%v,%v), want start %d", c.tsSec, w.Start.Unix(), w.End.Unix(), c.startSec)
		}
	}
	// 乱序到达不得改变窗口归属：同一事件时间多次分配，窗口 ID 恒定。
	w1 := a.Assign("k", time.Unix(73, 0).UTC())
	w2 := a.Assign("k", time.Unix(73, 0).UTC())
	if WindowID(w1) != WindowID(w2) || WindowID(w1) != "k@60000000000/120000000000" {
		t.Fatalf("window assignment not deterministic: %s vs %s", WindowID(w1), WindowID(w2))
	}
	// 不同键同时间 -> 不同窗口。
	if WindowID(a.Assign("a", base)) == WindowID(a.Assign("b", base)) {
		t.Fatal("windows of different keys must differ")
	}
	// 边界：左闭右开。
	w := a.Assign("k", base)
	if !w.Contains(base) || w.Contains(w.End) {
		t.Fatal("window must be left-closed right-open")
	}
	var _ = model.Window{}
}
