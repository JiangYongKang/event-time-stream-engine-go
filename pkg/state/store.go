package state

import (
	"sync"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/checkpoint"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
	"github.com/highcumontoa/event-time-stream-engine-go/pkg/window"
)

type windowAgg struct {
	st   checkpoint.WindowState
	seen map[string]struct{}
}

// Store 持有全部窗口聚合状态、去重集合与已提交窗口集合。
// 所有方法在同一把锁下完成“去重判定 + 聚合更新”，杜绝半更新与竞态。
type Store struct {
	mu               sync.RWMutex
	windows          map[string]*windowAgg
	committed        map[string]struct{}
	duplicateDropped int64
}

// NewStore 创建空状态存储。
func NewStore() *Store {
	return &Store{
		windows:   make(map[string]*windowAgg),
		committed: make(map[string]struct{}),
	}
}

// Upsert 在窗口中累加事件。
// dropped=true 表示重复事件（ID 已计入或窗口已提交），不得重复计入。
func (s *Store) Upsert(ev model.Event, w model.Window) (updated bool, dropped bool) {
	id := window.WindowID(w)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.committed[id]; ok {
		s.duplicateDropped++
		return false, true
	}
	agg, ok := s.windows[id]
	if !ok {
		agg = &windowAgg{
			st: checkpoint.WindowState{
				WindowID: id,
				Key:      w.Key,
				Start:    w.Start,
				End:      w.End,
			},
			seen: make(map[string]struct{}),
		}
		s.windows[id] = agg
	}
	if _, dup := agg.seen[ev.ID]; dup {
		s.duplicateDropped++
		return false, true
	}
	agg.seen[ev.ID] = struct{}{}
	agg.st.Count++
	agg.st.Sum += ev.Value
	return true, false
}

// Window 读取窗口当前聚合值与是否存在。
func (s *Store) Window(id string) (checkpoint.WindowState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	agg, ok := s.windows[id]
	if !ok {
		return checkpoint.WindowState{}, false
	}
	return agg.st, true
}

// MarkCommitted 标记窗口已提交（恢复时使用）。
func (s *Store) MarkCommitted(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committed[id] = struct{}{}
}

// IsCommitted 判断窗口是否已提交；已提交窗口结果绝不回改。
func (s *Store) IsCommitted(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.committed[id]
	return ok
}

// IsKnownEvent 判断事件 ID 是否曾被计入（含已关闭窗口 GC 前的活动窗口）。
// 用于在迟到判定之前先识别重复，保证"迟到的重复事件"仍按重复处理。
func (s *Store) IsKnownEvent(evID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, agg := range s.windows {
		if _, ok := agg.seen[evID]; ok {
			return true
		}
	}
	return false
}

// FinalizeWindows 返回所有满足 windowEnd + allowedLateness <= wm 的窗口快照，
// 并把它们原子地标记为已提交、从活动状态中移除（连同去重集合一起 GC）。
func (s *Store) FinalizeWindows(wm time.Time, allowedLateness time.Duration) []checkpoint.WindowState {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []checkpoint.WindowState
	for id, agg := range s.windows {
		if agg.st.End.Add(allowedLateness).After(wm) {
			continue
		}
		out = append(out, agg.st)
		s.committed[id] = struct{}{}
		delete(s.windows, id) // 窗口关闭后状态不再接受更新
	}
	return out
}

// Snapshot 导出可恢复状态（深拷贝）。
func (s *Store) Snapshot() ([]checkpoint.WindowState, map[string][]string, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wins := make([]checkpoint.WindowState, 0, len(s.windows))
	seen := make(map[string][]string, len(s.windows))
	for id, agg := range s.windows {
		wins = append(wins, agg.st)
		ids := make([]string, 0, len(agg.seen))
		for eid := range agg.seen {
			ids = append(ids, eid)
		}
		seen[id] = ids
	}
	committed := make([]string, 0, len(s.committed))
	for id := range s.committed {
		committed = append(committed, id)
	}
	return wins, seen, committed
}

// Restore 从检查点导入状态（先清空当前状态）。
func (s *Store) Restore(windows []checkpoint.WindowState, seen map[string][]string, committed []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows = make(map[string]*windowAgg, len(windows))
	s.committed = make(map[string]struct{}, len(committed))
	s.duplicateDropped = 0
	for _, st := range windows {
		agg := &windowAgg{st: st, seen: make(map[string]struct{})}
		for _, id := range seen[st.WindowID] {
			agg.seen[id] = struct{}{}
		}
		s.windows[st.WindowID] = agg
	}
	for _, id := range committed {
		s.committed[id] = struct{}{}
	}
}

// Stats 是状态存储的观测指标。
type Stats struct {
	ActiveWindows    int
	CommittedWindows int
	TrackedEventIDs  int
	DuplicateDropped int64
}

// Stats 返回当前指标快照。
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := 0
	for _, agg := range s.windows {
		ids += len(agg.seen)
	}
	return Stats{
		ActiveWindows:    len(s.windows),
		CommittedWindows: len(s.committed),
		TrackedEventIDs:  ids,
		DuplicateDropped: s.duplicateDropped,
	}
}
