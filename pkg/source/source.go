package source

import (
	"context"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// Source 是可按位点回放的本地数据源。
type Source interface {
	// Partitions 返回分区编号列表。
	Partitions() []int
	// Read 从某分区 offset 位置读取一批事件（返回事件与新位点）。
	Read(ctx context.Context, partition int, offset int64, maxBatch int) ([]model.Event, int64, error)
}
