package model

import "time"

// Event 是进入引擎的单条记录。窗口归属完全由 EventTime 决定。
type Event struct {
	ID        string    // 全局唯一 ID，用于去重
	Key       string    // 聚合键
	EventTime time.Time // 事件自带时间（事件时间）
	Value     int64     // 被聚合的值
	Partition int       // 来源分区
	Seq       int64     // 分区内单调序号
}

// Window 表示一个事件时间窗口（左闭右开）。
type Window struct {
	Key   string
	Start time.Time
	End   time.Time
}

func (w Window) Contains(ts time.Time) bool {
	return !ts.Before(w.Start) && ts.Before(w.End)
}

// Result 是某个窗口最终提交的聚合结果。
type Result struct {
	ID        string // 确定性 ID，用于幂等写出
	Key       string
	Start     time.Time
	End       time.Time
	Count     int64
	Sum       int64
	EmittedAt time.Time
}

// LateReason 描述迟到记录被旁路的明确原因。
type LateReason string

const (
	LateExceedsAllowedLateness LateReason = "EXCEEDS_ALLOWED_LATENESS"
	LateWindowAlreadyCommitted LateReason = "WINDOW_ALREADY_COMMITTED"
)

// LateRecord 是被旁路（side output）的迟到记录，绝不静默丢弃。
type LateRecord struct {
	Event           Event
	Reason          LateReason
	Watermark       time.Time
	AllowedLateness time.Duration
	SideOutputAt    time.Time
}
