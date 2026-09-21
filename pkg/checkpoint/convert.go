package checkpoint

import "github.com/highcumontoa/event-time-stream-engine-go/pkg/model"

// ToPendingResult / FromPendingResult 在 model.Result 与持久化结构间转换。
func ToPendingResult(r model.Result) pendingResult {
	return pendingResult{
		ID: r.ID, Key: r.Key, Start: r.Start, End: r.End,
		Count: r.Count, Sum: r.Sum, EmittedAt: r.EmittedAt,
	}
}

func FromPendingResult(p pendingResult) model.Result {
	return model.Result{
		ID: p.ID, Key: p.Key, Start: p.Start, End: p.End,
		Count: p.Count, Sum: p.Sum, EmittedAt: p.EmittedAt,
	}
}

// ToPendingLate / FromPendingLate 在 model.LateRecord 与持久化结构间转换。
func ToPendingLate(r model.LateRecord) pendingLate {
	return pendingLate{
		EventID: r.Event.ID, Key: r.Event.Key, EventTime: r.Event.EventTime,
		Value: r.Event.Value, Partition: r.Event.Partition, Seq: r.Event.Seq,
		Reason: string(r.Reason), Watermark: r.Watermark,
		AllowedLateness: int64(r.AllowedLateness), SideOutputAt: r.SideOutputAt,
	}
}

func FromPendingLate(p pendingLate) model.LateRecord {
	return model.LateRecord{
		Event: model.Event{
			ID: p.EventID, Key: p.Key, EventTime: p.EventTime, Value: p.Value,
			Partition: p.Partition, Seq: p.Seq,
		},
		Reason: model.LateReason(p.Reason), Watermark: p.Watermark,
		AllowedLateness: durationOf(p.AllowedLateness), SideOutputAt: p.SideOutputAt,
	}
}

// EncodeState / DecodeState 处理 pending 切片与 model 类型的整体转换。
func (s *State) EncodePending(results []model.Result, lates []model.LateRecord) {
	s.PendingResults = make([]pendingResult, len(results))
	for i, r := range results {
		s.PendingResults[i] = ToPendingResult(r)
	}
	s.PendingLate = make([]pendingLate, len(lates))
	for i, r := range lates {
		s.PendingLate[i] = ToPendingLate(r)
	}
}

func (s *State) DecodePending() ([]model.Result, []model.LateRecord) {
	results := make([]model.Result, len(s.PendingResults))
	for i, p := range s.PendingResults {
		results[i] = FromPendingResult(p)
	}
	lates := make([]model.LateRecord, len(s.PendingLate))
	for i, p := range s.PendingLate {
		lates[i] = FromPendingLate(p)
	}
	return results, lates
}
