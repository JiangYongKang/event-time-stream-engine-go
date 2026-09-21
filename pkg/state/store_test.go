package state

import (
	"testing"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/window"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func TestAggregateDedupFinalize(t *testing.T) {
	s := NewStore()
	a := window.NewAssigner(time.Minute)
	w0 := a.Assign("k", ts(5))
	w1 := a.Assign("k", ts(65))

	ev := func(id string, sec int, v int64) model.Event {
		return model.Event{ID: id, Key: "k", EventTime: ts(sec), Value: v}
	}

	for _, e := range []model.Event{ev("a", 5, 1), ev("b", 9, 2), ev("a", 5, 1), ev("c", 65, 10)} {
		w := a.Assign("k", e.EventTime)
		_, dropped := s.Upsert(e, w)
		if e.ID == "a" && !dropped {
			// 第二次 a 必须被判重
		}
	}
	// 重复一次 a
	if _, dropped := s.Upsert(ev("a", 5, 1), w0); !dropped {
		t.Fatal("duplicate event must be dropped")
	}
	got, ok := s.Window(window.WindowID(w0))
	if !ok || got.Count != 2 || got.Sum != 3 {
		t.Fatalf("w0 aggregate wrong: %+v ok=%v", got, ok)
	}

	// wm=90, allowedLateness=0 -> w0(end60) 定稿；w1(end120) 保留。
	finalized := s.FinalizeWindows(ts(90), 0)
	if len(finalized) != 1 || finalized[0].WindowID != window.WindowID(w0) {
		t.Fatalf("expected exactly w0 finalized, got %+v", finalized)
	}
	if !s.IsCommitted(window.WindowID(w0)) {
		t.Fatal("finalized window must be marked committed")
	}
	// 已提交窗口不得被回改：迟到更新直接判重丢弃。
	if _, dropped := s.Upsert(ev("late", 5, 100), w0); !dropped {
		t.Fatal("update to committed window must be rejected")
	}
	got1, _ := s.Window(window.WindowID(w1))
	if got1.Count != 1 || got1.Sum != 10 {
		t.Fatalf("w1 should remain active: %+v", got1)
	}
	if st := s.Stats(); st.DuplicateDropped != 3 {
		t.Fatalf("duplicate metric wrong (loop-dup + extra-dup + committed-window reject): %+v", st)
	}
}

func TestAllowedLatenessKeepsWindowOpen(t *testing.T) {
	s := NewStore()
	a := window.NewAssigner(time.Minute)
	w0 := a.Assign("k", ts(5))
	s.Upsert(model.Event{ID: "a", Key: "k", EventTime: ts(5), Value: 1}, w0)
	// wm=70, allowedLateness=30 -> end60+30=90 > 70，窗口不得定稿。
	if f := s.FinalizeWindows(ts(70), 30*time.Second); len(f) != 0 {
		t.Fatalf("window within allowed lateness must stay open, got %+v", f)
	}
	// wm=90 -> end60+30=90 <= 90，定稿。
	if f := s.FinalizeWindows(ts(90), 30*time.Second); len(f) != 1 {
		t.Fatal("window must finalize once wm reaches end+allowedLateness")
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := NewStore()
	a := window.NewAssigner(time.Minute)
	w0 := a.Assign("k", ts(5))
	s.Upsert(model.Event{ID: "a", Key: "k", EventTime: ts(5), Value: 7}, w0)
	wins, seen, committed := s.Snapshot()

	s2 := NewStore()
	s2.Restore(wins, seen, committed)
	got, ok := s2.Window(window.WindowID(w0))
	if !ok || got.Sum != 7 {
		t.Fatalf("restore aggregate wrong: %+v", got)
	}
	// 恢复后去重集合仍然有效：重复 a 被丢弃。
	if _, dropped := s2.Upsert(model.Event{ID: "a", Key: "k", EventTime: ts(5), Value: 7}, w0); !dropped {
		t.Fatal("dedup set must survive restore")
	}
}
