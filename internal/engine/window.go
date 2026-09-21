package engine

// Assigner 负责把事件时间映射到固定滚动窗口。
// 窗口归属只由事件自带时间决定，与处理时刻无关。
type Assigner struct {
	SizeMs int64
}

func NewAssigner(sizeMs int64) *Assigner { return &Assigner{SizeMs: sizeMs} }

// Assign 返回事件时间所属的左闭右开窗口 [start, start+size)。
// 对负时间戳同样成立（向下取整对齐）。
func (a *Assigner) Assign(eventTimeMs int64) Window {
	start := eventTimeMs - mod(eventTimeMs, a.SizeMs)
	return Window{Start: start, End: start + a.SizeMs}
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
