package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// FileSink 把窗口结果与迟到记录分别以 JSON Lines 追加写入本地文件。
// 打开时读取历史文件建立 ID 索引，因此重启后按 ID 幂等，重复写入不会重复落盘。
// 每次写入后 fsync，保证“检查点已提交但 sink 未落盘”之外没有灰区。
type FileSink struct {
	mu         sync.Mutex
	resFile    *os.File
	lateFile   *os.File
	resWriter  *bufio.Writer
	lateWriter *bufio.Writer
	resultIDs  map[string]struct{}
	lateIDs    map[string]struct{}
	results    []model.Result
	lates      []model.LateRecord
}

type resultJSON struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Count     int64     `json:"count"`
	Sum       int64     `json:"sum"`
	EmittedAt time.Time `json:"emitted_at"`
}

type lateJSON struct {
	EventID         string    `json:"event_id"`
	Key             string    `json:"key"`
	EventTime       time.Time `json:"event_time"`
	Value           int64     `json:"value"`
	Partition       int       `json:"partition"`
	Seq             int64     `json:"seq"`
	Reason          string    `json:"reason"`
	Watermark       time.Time `json:"watermark"`
	AllowedLateness int64     `json:"allowed_lateness_ns"`
	SideOutputAt    time.Time `json:"side_output_at"`
}

// NewFileSink 创建/打开两个 JSONL 文件并重建幂等索引。
func NewFileSink(resultsPath, latePath string) (*FileSink, error) {
	s := &FileSink{
		resultIDs: make(map[string]struct{}),
		lateIDs:   make(map[string]struct{}),
	}
	rf, err := os.OpenFile(resultsPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(latePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		rf.Close()
		return nil, err
	}
	s.resFile, s.lateFile = rf, lf
	s.resWriter = bufio.NewWriter(rf)
	s.lateWriter = bufio.NewWriter(lf)
	if err := s.reindex(); err != nil {
		rf.Close()
		lf.Close()
		return nil, err
	}
	return s, nil
}

func (s *FileSink) reindex() error {
	if err := readJSONL(s.resFile, func(line []byte) error {
		var rj resultJSON
		if err := json.Unmarshal(line, &rj); err != nil {
			return err
		}
		if _, ok := s.resultIDs[rj.ID]; !ok {
			s.resultIDs[rj.ID] = struct{}{}
			s.results = append(s.results, model.Result{
				ID: rj.ID, Key: rj.Key, Start: rj.Start, End: rj.End,
				Count: rj.Count, Sum: rj.Sum, EmittedAt: rj.EmittedAt,
			})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sink: reindex results: %w", err)
	}
	if err := readJSONL(s.lateFile, func(line []byte) error {
		var lj lateJSON
		if err := json.Unmarshal(line, &lj); err != nil {
			return err
		}
		if _, ok := s.lateIDs[lj.EventID]; !ok {
			s.lateIDs[lj.EventID] = struct{}{}
			s.lates = append(s.lates, decodeLate(lj))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sink: reindex late: %w", err)
	}
	return nil
}

func readJSONL(f *os.File, handle func([]byte) error) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if hErr := handle(line); hErr != nil {
				return hErr
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	_, err := f.Seek(0, io.SeekEnd)
	return err
}

// WriteResult 幂等写出窗口结果。
func (s *FileSink) WriteResult(ctx context.Context, r model.Result) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.resultIDs[r.ID]; ok {
		return true, nil
	}
	rj := resultJSON{
		ID: r.ID, Key: r.Key, Start: r.Start, End: r.End,
		Count: r.Count, Sum: r.Sum, EmittedAt: r.EmittedAt,
	}
	b, err := json.Marshal(rj)
	if err != nil {
		return false, err
	}
	if _, err := s.resWriter.Write(append(b, '\n')); err != nil {
		return false, err
	}
	if err := s.resWriter.Flush(); err != nil {
		return false, err
	}
	if err := s.resFile.Sync(); err != nil {
		return false, err
	}
	s.resultIDs[r.ID] = struct{}{}
	s.results = append(s.results, r)
	return false, nil
}

// WriteLate 幂等旁路一条迟到记录（以事件 ID 去重）。
func (s *FileSink) WriteLate(ctx context.Context, r model.LateRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.lateIDs[r.Event.ID]; ok {
		return true, nil
	}
	lj := encodeLate(r)
	b, err := json.Marshal(lj)
	if err != nil {
		return false, err
	}
	if _, err := s.lateWriter.Write(append(b, '\n')); err != nil {
		return false, err
	}
	if err := s.lateWriter.Flush(); err != nil {
		return false, err
	}
	if err := s.lateFile.Sync(); err != nil {
		return false, err
	}
	s.lateIDs[r.Event.ID] = struct{}{}
	s.lates = append(s.lates, r)
	return false, nil
}

// Results 返回已写出的全部窗口结果（按窗口开始时间、再按键排序）。
func (s *FileSink) Results() []model.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Result, len(s.results))
	copy(out, s.results)
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Late 返回全部迟到旁路记录（按事件时间排序）。
func (s *FileSink) Late() []model.LateRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.LateRecord, len(s.lates))
	copy(out, s.lates)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Event.EventTime.Before(out[j].Event.EventTime)
	})
	return out
}

// Close 刷盘并关闭文件。
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err1, err2 error
	if s.resWriter != nil {
		if e := s.resWriter.Flush(); e == nil {
			err1 = s.resFile.Close()
		} else {
			err1 = e
		}
	}
	if s.lateWriter != nil {
		if e := s.lateWriter.Flush(); e == nil {
			err2 = s.lateFile.Close()
		} else {
			err2 = e
		}
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func encodeLate(r model.LateRecord) lateJSON {
	return lateJSON{
		EventID: r.Event.ID, Key: r.Event.Key, EventTime: r.Event.EventTime,
		Value: r.Event.Value, Partition: r.Event.Partition, Seq: r.Event.Seq,
		Reason: string(r.Reason), Watermark: r.Watermark,
		AllowedLateness: int64(r.AllowedLateness), SideOutputAt: r.SideOutputAt,
	}
}

func decodeLate(lj lateJSON) model.LateRecord {
	return model.LateRecord{
		Event: model.Event{
			ID: lj.EventID, Key: lj.Key, EventTime: lj.EventTime, Value: lj.Value,
			Partition: lj.Partition, Seq: lj.Seq,
		},
		Reason: model.LateReason(lj.Reason), Watermark: lj.Watermark,
		AllowedLateness: time.Duration(lj.AllowedLateness), SideOutputAt: lj.SideOutputAt,
	}
}
