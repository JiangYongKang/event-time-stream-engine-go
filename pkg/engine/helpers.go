package engine

import "github.com/highcumontoa/event-time-stream-engine-go/pkg/checkpoint"

// committedList 返回恢复时需要标记为“已提交”的窗口 ID。
//
// 窗口一旦定稿就会从检查点的活动 Windows 集合中移除，其结果要么已经
// 在 sink 中、要么记录在 PendingResults / AckedResultIDs 中。因此恢复时
// 活动窗口之外的历史窗口无需再作为可变状态导入（它们的结果不可回改）。
func committedList(_ *checkpoint.State) []string { return nil }
