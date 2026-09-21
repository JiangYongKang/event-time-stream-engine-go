package engine

// Event 是输入数据单元，窗口归属由 EventTime 决定。
type Event struct {
	ID        string  `json:"id"`
	Key       string  `json:"key"`
	EventTime int64   `json:"event_time"` // unix 毫秒
	Value     float64 `json:"value"`
}

// Window 表示一个左闭右开的事件时间窗口 [Start, End)。
type Window struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// WindowState 是窗口的累计状态。
type WindowState struct {
	Key    string  `json:"key"`
	Window Window  `json:"window"`
	Count  int64   `json:"count"`
	Sum    float64 `json:"sum"`
}

// WindowResult 是窗口提交后待写出的结果。
type WindowResult struct {
	Key          string  `json:"key"`
	WindowStart  int64   `json:"window_start"`
	WindowEnd    int64   `json:"window_end"`
	Count        int64   `json:"count"`
	Sum          float64 `json:"sum"`
	CheckpointID uint64  `json:"checkpoint_id"`
}

// LateRecord 是被旁路的迟到事件，必须显式标记而非静默丢弃。
type LateRecord struct {
	Event        Event  `json:"event"`
	Reason       string `json:"reason"`
	Watermark    int64  `json:"watermark"`
	CheckpointID uint64 `json:"checkpoint_id"`
}
