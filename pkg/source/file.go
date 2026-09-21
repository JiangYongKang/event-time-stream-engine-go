package source

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/highcumontoa/event-time-stream-engine-go/pkg/model"
)

// fileEvent 是 JSON Lines 文件中每行的编码。
type fileEvent struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	EventTime time.Time `json:"event_time"`
	Value     int64     `json:"value"`
	Seq       int64     `json:"seq,omitempty"`
}

// FileSource 从本地 JSON Lines 文件读取事件，位点为字节偏移（文件起始的
// 行边界），因此可从任意检查点位点精确回放。
type FileSource struct {
	paths map[int]string
	mu    map[int]*sync.Mutex
}

// NewFileSource 以 分区->文件路径 构造文件数据源。
func NewFileSource(paths map[int]string) (*FileSource, error) {
	for p, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("source: open partition %d: %w", p, err)
		}
		f.Close()
	}
	locks := make(map[int]*sync.Mutex, len(paths))
	for p := range paths {
		locks[p] = &sync.Mutex{}
	}
	return &FileSource{paths: paths, mu: locks}, nil
}

// Partitions 返回分区编号列表（升序）。
func (s *FileSource) Partitions() []int {
	ps := make([]int, 0, len(s.paths))
	for p := range s.paths {
		ps = append(ps, p)
	}
	// 简单排序
	for i := 0; i < len(ps); i++ {
		for j := i + 1; j < len(ps); j++ {
			if ps[j] < ps[i] {
				ps[i], ps[j] = ps[j], ps[i]
			}
		}
	}
	return ps
}

// Read 从 partition 的字节偏移 offset 处读取最多 maxBatch 行。
func (s *FileSource) Read(ctx context.Context, partition int, offset int64, maxBatch int) ([]model.Event, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, offset, err
	}
	if maxBatch <= 0 {
		maxBatch = 1
	}
	path, ok := s.paths[partition]
	if !ok {
		return nil, offset, fmt.Errorf("source: unknown partition %d", partition)
	}
	mu := s.mu[partition]
	mu.Lock()
	defer mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return nil, offset, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	r := bufio.NewReader(f)
	var out []model.Event
	pos := offset
	for len(out) < maxBatch {
		if err := ctx.Err(); err != nil {
			return out, pos, err
		}
		line, err := r.ReadBytes('\n')
		lineLen := len(line)
		if len(line) > 0 {
			var fe fileEvent
			if jerr := json.Unmarshal(line, &fe); jerr == nil && fe.ID != "" {
				out = append(out, model.Event{
					ID:        fe.ID,
					Key:       fe.Key,
					EventTime: fe.EventTime,
					Value:     fe.Value,
					Partition: partition,
					Seq:       fe.Seq,
				})
			}
			pos += int64(lineLen)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, pos, err
		}
	}
	return out, pos, nil
}

var _ = time.Time{}
