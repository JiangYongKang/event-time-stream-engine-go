package source

import (
	"context"
	"sort"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// MemorySource 是按分区保存事件切片的本地数据源，支持任意位点回放，
// 位点语义为“下一条待处理事件的切片下标”。
type MemorySource struct {
	partitions map[int][]model.Event
}

// NewMemorySource 以 分区->事件列表 构造内存数据源。
func NewMemorySource(partitions map[int][]model.Event) *MemorySource {
	cp := make(map[int][]model.Event, len(partitions))
	for p, evs := range partitions {
		dup := make([]model.Event, len(evs))
		copy(dup, evs)
		cp[p] = dup
	}
	return &MemorySource{partitions: cp}
}

// Partitions 返回分区编号列表（升序）。
func (s *MemorySource) Partitions() []int {
	ps := make([]int, 0, len(s.partitions))
	for p := range s.partitions {
		ps = append(ps, p)
	}
	sort.Ints(ps)
	return ps
}

// Read 从 partition 的 offset 下标读取最多 maxBatch 条事件，
// 返回事件与新位点；到分区末尾时返回空切片与原位点。
func (s *MemorySource) Read(ctx context.Context, partition int, offset int64, maxBatch int) ([]model.Event, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, offset, err
	}
	if maxBatch <= 0 {
		maxBatch = 1
	}
	evs := s.partitions[partition]
	if offset >= int64(len(evs)) {
		return nil, offset, nil
	}
	end := offset + int64(maxBatch)
	if end > int64(len(evs)) {
		end = int64(len(evs))
	}
	out := make([]model.Event, end-offset)
	copy(out, evs[offset:end])
	return out, end, nil
}
