package engine

import (
	"math"
	"sync"
)

// WatermarkTracker 跟踪各分区已观察到的最大事件时间，
// 全局水位线 = min(各分区最大事件时间) - 允许迟到时长。
// 水位线语义：事件时间 <= 水位线 的事件视为迟到。
type WatermarkTracker struct {
	mu                sync.Mutex
	allowedLatenessMs int64
	maxSeen           map[int]int64
}

func NewWatermarkTracker(partitions int, allowedLatenessMs int64) *WatermarkTracker {
	w := &WatermarkTracker{allowedLatenessMs: allowedLatenessMs, maxSeen: make(map[int]int64)}
	for i := 0; i < partitions; i++ {
		w.maxSeen[i] = math.MinInt64
	}
	return w
}

// Observe 记录分区观察到的事件时间（只增不减）。
func (w *WatermarkTracker) Observe(partition int, eventTimeMs int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if eventTimeMs > w.maxSeen[partition] {
		w.maxSeen[partition] = eventTimeMs
	}
}

// Watermark 返回当前全局水位线；任何分区未产生数据前水位线为最小值。
func (w *WatermarkTracker) Watermark() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	min := int64(math.MaxInt64)
	for _, v := range w.maxSeen {
		if v < min {
			min = v
		}
	}
	if min == math.MinInt64 {
		return math.MinInt64
	}
	return min - w.allowedLatenessMs
}

// Close 在输入耗尽时调用：所有分区水位推到正无穷，
// 使剩余窗口在最终检查点全部完成。
func (w *WatermarkTracker) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k := range w.maxSeen {
		w.maxSeen[k] = math.MaxInt64
	}
}

// Snapshot 导出各分区最大事件时间，用于检查点。
func (w *WatermarkTracker) Snapshot() map[int]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[int]int64, len(w.maxSeen))
	for k, v := range w.maxSeen {
		out[k] = v
	}
	return out
}

// Restore 从检查点恢复各分区最大事件时间。
func (w *WatermarkTracker) Restore(snap map[int]int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k, v := range snap {
		w.maxSeen[k] = v
	}
}
