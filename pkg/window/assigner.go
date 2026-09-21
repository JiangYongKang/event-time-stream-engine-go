package window

import (
	"fmt"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// Assigner 把事件时间映射到固定滚动窗口（左闭右开 [Start, End)）。
// 窗口归属只由事件自带时间决定，与到达/处理时刻无关。
type Assigner struct {
	size time.Duration
}

// NewAssigner 创建固定窗口分配器，size 必须为正。
func NewAssigner(size time.Duration) *Assigner {
	if size <= 0 {
		panic("window: size must be positive")
	}
	return &Assigner{size: size}
}

// Size 返回窗口长度。
func (a *Assigner) Size() time.Duration { return a.size }

// Assign 返回 ts 所属的窗口。以 Unix 零时刻为基准对齐，保证乱序到达时
// 同一事件时间始终落入同一窗口。
func (a *Assigner) Assign(key string, ts time.Time) model.Window {
	startNS := ts.UnixNano()
	if step := int64(a.size); step > 0 {
		startNS -= (startNS%step + step) % step // 兼容负数时间的向下取整
	}
	start := time.Unix(0, startNS).In(ts.Location())
	return model.Window{Key: key, Start: start, End: start.Add(a.size)}
}

// WindowID 返回窗口的确定性标识（同一键同一时间区间始终得到同一 ID）。
func WindowID(w model.Window) string {
	return fmt.Sprintf("%s@%d/%d", w.Key, w.Start.UnixNano(), w.End.UnixNano())
}
