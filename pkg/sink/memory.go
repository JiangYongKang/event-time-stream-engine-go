package sink

import (
	"context"
	"sort"
	"sync"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// MemorySink 是保存在内存中的幂等 sink，用于本地测试与确定性验证。
type MemorySink struct {
	mu      sync.Mutex
	res     map[string]model.Result
	late    map[string]model.LateRecord
	resSeq  []string
	lateSeq []string
}

// NewMemorySink 创建空内存 sink。
func NewMemorySink() *MemorySink {
	return &MemorySink{
		res:  make(map[string]model.Result),
		late: make(map[string]model.LateRecord),
	}
}

// WriteResult 幂等写出窗口结果。
func (s *MemorySink) WriteResult(ctx context.Context, r model.Result) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.res[r.ID]; ok {
		return true, nil
	}
	s.res[r.ID] = r
	s.resSeq = append(s.resSeq, r.ID)
	return false, nil
}

// WriteLate 幂等旁路一条迟到记录。
func (s *MemorySink) WriteLate(ctx context.Context, r model.LateRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.late[r.Event.ID]; ok {
		return true, nil
	}
	s.late[r.Event.ID] = r
	s.lateSeq = append(s.lateSeq, r.Event.ID)
	return false, nil
}

// Results 返回按窗口开始时间、键排序的已提交结果。
func (s *MemorySink) Results() []model.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Result, 0, len(s.resSeq))
	for _, id := range s.resSeq {
		out = append(out, s.res[id])
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Late 返回全部迟到旁路记录。
func (s *MemorySink) Late() []model.LateRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.LateRecord, 0, len(s.lateSeq))
	for _, id := range s.lateSeq {
		out = append(out, s.late[id])
	}
	return out
}
