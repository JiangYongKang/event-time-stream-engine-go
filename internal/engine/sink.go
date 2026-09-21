package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// FileSink 以本地 JSONL 文件模拟外部写出存储。
//
// 恰好一次语义的实现方式：
//   - 每条写出记录携带产生它的检查点 ID；
//   - 引擎先 Commit(checkpointID, ...) 再持久化该检查点；
//   - 恢复时若检查点未包含某次 Commit 的 ID，则 TruncateTo 将其回滚，
//     因此崩溃不会导致重复写出，也不会丢失已确认检查点的输出。
type FileSink struct {
	resultsPath string
	latePath    string
}

func NewFileSink(resultsPath, latePath string) *FileSink {
	return &FileSink{resultsPath: resultsPath, latePath: latePath}
}

// Commit 随检查点提交窗口结果与迟到记录（追加写 + fsync）。
func (s *FileSink) Commit(checkpointID uint64, results []WindowResult, lates []LateRecord) error {
	if err := appendFile(s.resultsPath, results); err != nil {
		return err
	}
	return appendFile(s.latePath, lates)
}

func appendFile[T any](path string, records []T) error {
	if len(records) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			f.Close()
			return err
		}
		if _, err := w.Write(data); err != nil {
			f.Close()
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// TruncateTo 回滚所有属于 checkpointID 之后（未确认）检查点的写出记录。
// 用于崩溃恢复：检查点保存成功之前的 Commit 视为未提交，必须撤销。
func (s *FileSink) TruncateTo(checkpointID uint64) error {
	if err := truncateFile[WindowResult](s.resultsPath, checkpointID, func(r WindowResult) uint64 { return r.CheckpointID }); err != nil {
		return err
	}
	return truncateFile[LateRecord](s.latePath, checkpointID, func(r LateRecord) uint64 { return r.CheckpointID })
}

func truncateFile[T any](path string, checkpointID uint64, cpOf func(T) uint64) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	var kept []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := trimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec T
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("sink file %s corrupt: %w", path, err)
		}
		if cpOf(rec) <= checkpointID {
			kept = append(kept, rec)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	tmp := path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	for _, rec := range kept {
		data, err := json.Marshal(rec)
		if err != nil {
			out.Close()
			return err
		}
		if _, err := w.Write(data); err != nil {
			out.Close()
			return err
		}
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadResults 读取全部已写出的窗口结果。
func (s *FileSink) ReadResults() ([]WindowResult, error) {
	return readAll[WindowResult](s.resultsPath)
}

// ReadLates 读取全部迟到记录。
func (s *FileSink) ReadLates() ([]LateRecord, error) {
	return readAll[LateRecord](s.latePath)
}

func readAll[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := trimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec T
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("sink file %s corrupt: %w", path, err)
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}
