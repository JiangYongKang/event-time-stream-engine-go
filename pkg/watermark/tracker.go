package watermark

import (
	"sync"
	"time"
)

// Tracker 跟踪各分区事件时间并计算全局水位线。
// 全局水位线 = min(各分区最大事件时间) - outOfOrderness，单调不减。
type Tracker struct {
	mu              sync.RWMutex
	outOfOrder      time.Duration
	allowedLateness time.Duration
	lastEventTime   map[int]time.Time
	registered      map[int]struct{}
	cached          time.Time
	initialized     bool
}

// NewTracker 创建水位线跟踪器。
// outOfOrderness: 生成水位线时预留的乱序余量。
// allowedLateness: 超出水位线多少范围内的迟到事件仍可进入对应窗口。
func NewTracker(outOfOrder, allowedLateness time.Duration) *Tracker {
	if outOfOrder < 0 || allowedLateness < 0 {
		panic("watermark: durations must be non-negative")
	}
	return &Tracker{
		outOfOrder:      outOfOrder,
		allowedLateness: allowedLateness,
		lastEventTime:   make(map[int]time.Time),
		registered:      make(map[int]struct{}),
	}
}

// RegisterPartitions 声明参与全局水位线计算的分区；必须在 Observe 前注册，
// 保证“慢分区尚未产生事件”时不会把全局水位错误地拉高。
func (t *Tracker) RegisterPartitions(parts ...int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range parts {
		t.registered[p] = struct{}{}
	}
	t.recomputeLocked()
}

// Observe 记录某分区观察到的事件时间，并推进该分区时间（只取更大值）。
func (t *Tracker) Observe(partition int, ts time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.lastEventTime[partition]; !ok || ts.After(cur) {
		t.lastEventTime[partition] = ts
		t.recomputeLocked()
	}
}

// recomputeLocked 重算全局水位线；水位线只允许前进。
// 已注册但尚无事件的分区按时间起点参与 min，即慢分区会压住全局水位。
func (t *Tracker) recomputeLocked() {
	if len(t.registered) == 0 {
		// 兼容未显式注册的用法：只在已有事件的分区上取 min。
		if len(t.lastEventTime) == 0 {
			return
		}
	}
	min := time.Time{}
	haveMin := false
	for p := range t.registered {
		ts, ok := t.lastEventTime[p]
		if !ok {
			min = time.Time{}
			haveMin = true
			break
		}
		if !haveMin || ts.Before(min) {
			min, haveMin = ts, true
		}
	}
	if !haveMin { // 未注册且无数据
		return
	}
	wm := min.Add(-t.outOfOrder)
	if !t.initialized || wm.After(t.cached) {
		t.cached = wm
		t.initialized = true
	}
}

// Watermark 返回当前全局水位线。
func (t *Tracker) Watermark() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cached
}

// AllowedLateness 返回允许迟到时长。
func (t *Tracker) AllowedLateness() time.Duration { return t.allowedLateness }

// IsLate 判断某事件时间是否已超过允许迟到范围：
// ts + allowedLateness < watermark（其目标窗口在水位线语义下已彻底关闭）。
func (t *Tracker) IsLate(ts time.Time) (late bool, watermark time.Time) {
	t.mu.RLock()
	wm := t.cached
	t.mu.RUnlock()
	if !t.initialized {
		return false, wm
	}
	return ts.Add(t.allowedLateness).Before(wm), wm
}

// IsBeyondWatermark 判断事件时间是否落后于当前水位线（乱序到达，
// 但可能仍在允许迟到范围内而被接受）。
func (t *Tracker) IsBeyondWatermark(ts time.Time) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.initialized && ts.Before(t.cached)
}

// Snapshot 导出各分区事件时间（检查点恢复依据）。
func (t *Tracker) Snapshot() map[int]time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cp := make(map[int]time.Time, len(t.lastEventTime))
	for k, v := range t.lastEventTime {
		cp[k] = v
	}
	return cp
}

// Restore 从检查点恢复各分区时间并重算水位线。
func (t *Tracker) Restore(lastEventTime map[int]time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastEventTime = make(map[int]time.Time, len(lastEventTime))
	t.registered = make(map[int]struct{}, len(lastEventTime))
	for k, v := range lastEventTime {
		t.lastEventTime[k] = v
		t.registered[k] = struct{}{}
	}
	t.initialized = false
	t.cached = time.Time{}
	t.recomputeLocked()
}

// farFuture 是输入彻底结束后使用的"无穷大"水位线，用于关闭所有剩余窗口。
// 取可被 JSON 序列化的最大年份内时间。
var farFuture = time.Date(9999, 12, 30, 0, 0, 0, 0, time.UTC)

// MarkComplete 在所有分区输入结束后把水位线推进到无穷大（单调前进）。
// 此后 FinalizeWindows 将关闭全部尚未提交的窗口。
func (t *Tracker) MarkComplete() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cached = farFuture
	t.initialized = true
}
