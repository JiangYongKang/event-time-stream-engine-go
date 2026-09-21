package sink

import (
	"context"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// Sink 是恰好一次语义的本地结果存储，按结果 ID 幂等写入。
type Sink interface {
	// WriteResult 幂等写出窗口结果，返回 existed=true 表示该 ID 此前已写出。
	WriteResult(ctx context.Context, r model.Result) (existed bool, err error)
	// WriteLate 幂等旁路一条迟到记录。
	WriteLate(ctx context.Context, r model.LateRecord) (existed bool, err error)
	// Results 返回已写出的全部窗口结果（按窗口开始时间排序）。
	Results() []model.Result
	// Late 返回全部迟到旁路记录。
	Late() []model.LateRecord
}
