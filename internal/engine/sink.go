package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// Sink 是结果写出端的抽象，便于注入故障实现做演练。
//
// 实现方必须满足：
//   - Commit 对同一个 checkpointID 具备幂等性：
//     同一代检查点（含相同 results/lates）重复 Commit 不得产生重复记录，
//     上次尝试留下的“部分写出”必须被本次尝试整体替换；
//   - 写出必须落盘（fsync + 原子改名），成功返回后数据不得丢失。
type Sink interface {
	Commit(checkpointID uint64, results []WindowResult, lates []LateRecord) error
	// TruncateTo 回滚所有属于 checkpointID 之后（未确认）检查点的写出记录。
	TruncateTo(checkpointID uint64) error
}

// FileSink 以本地 JSONL 文件模拟外部写出存储。
//
// 恰好一次语义的实现方式：
//   - 每条写出记录携带产生它的检查点 ID；
//   - 引擎先 Commit(checkpointID, ...) 再持久化该检查点；
//   - Commit 按代幂等：先读回已确认的旧代记录，剔除本代及更新代的残留
//     （上次失败尝试可能写出的完整行/残缺行），再与本批合并后整体原子重写，
//     因此“写一半失败后重试 / 进程被杀后重启重放”都不会重复写出；
//   - 恢复时若检查点未包含某次 Commit 的 ID，则 TruncateTo 将其回滚。
type FileSink struct {
	resultsPath string
	latePath    string
	logger      *log.Logger
}

func NewFileSink(resultsPath, latePath string) *FileSink {
	return &FileSink{resultsPath: resultsPath, latePath: latePath}
}

// SetLogger 注入运行日志（残缺尾行剔除等事件需要可见）。nil 表示静默。
func (s *FileSink) SetLogger(l *log.Logger) { s.logger = l }

func (s *FileSink) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

// Commit 随检查点幂等提交窗口结果与迟到记录。
func (s *FileSink) Commit(checkpointID uint64, results []WindowResult, lates []LateRecord) error {
	if err := commitFile(s, s.resultsPath, checkpointID, results,
		func(r WindowResult) uint64 { return r.CheckpointID }); err != nil {
		return err
	}
	return commitFile(s, s.latePath, checkpointID, lates,
		func(r LateRecord) uint64 { return r.CheckpointID })
}

// commitFile 的幂等重写过程：
//  1. 读回现有记录（文件末尾残缺行视为上次失败/被杀的残留，丢弃并记日志）；
//  2. 只保留严格旧代（cpID < 本次）的记录；本代残留与更新代记录一律剔除；
//  3. 追加本批记录，tmp 写入 + fsync + 原子改名整体替换。
func commitFile[T any](s *FileSink, path string, cpID uint64, records []T, cpOf func(T) uint64) error {
	kept, tornBytes, err := readValid(s, path, cpOf)
	if err != nil {
		return err
	}
	if tornBytes > 0 {
		s.logf("SINK REWRITE path=%s cp=%d: discarded %d-byte torn trailing line left by a failed/killed write",
			path, cpID, tornBytes)
	}
	filtered := kept[:0]
	dropped := 0
	for _, rec := range kept {
		if cpOf(rec) < cpID {
			filtered = append(filtered, rec)
		} else {
			dropped++
		}
	}
	if dropped > 0 {
		s.logf("SINK REWRITE path=%s cp=%d: dropped %d stale record(s) from cp>=%d before re-append",
			path, cpID, dropped, cpID)
	}
	filtered = append(filtered, records...)
	return rewriteFile(path, filtered)
}

// TruncateTo 回滚所有属于 checkpointID 之后（未确认）检查点的写出记录。
// 用于崩溃恢复：检查点保存成功之前的 Commit 视为未提交，必须撤销。
// 文件末尾因进程被杀留下的残缺行同样在此清除。
func (s *FileSink) TruncateTo(checkpointID uint64) error {
	if err := truncateFile(s, s.resultsPath, checkpointID, func(r WindowResult) uint64 { return r.CheckpointID }); err != nil {
		return err
	}
	return truncateFile(s, s.latePath, checkpointID, func(r LateRecord) uint64 { return r.CheckpointID })
}

func truncateFile[T any](s *FileSink, path string, checkpointID uint64, cpOf func(T) uint64) error {
	kept, tornBytes, err := readValid(s, path, cpOf)
	if err != nil {
		return err
	}
	if tornBytes > 0 {
		s.logf("SINK ROLLBACK path=%s: discarded %d-byte torn trailing line", path, tornBytes)
	}
	var out []T
	dropped := 0
	for _, rec := range kept {
		if cpOf(rec) <= checkpointID {
			out = append(out, rec)
		} else {
			dropped++
		}
	}
	if dropped > 0 {
		s.logf("SINK ROLLBACK path=%s: removed %d uncommitted record(s) with cp>%d",
			path, dropped, checkpointID)
	}
	return rewriteFile(path, out)
}

// readValid 读回 JSONL 记录。文件末尾的残缺行（进程在写行中途被杀）被丢弃，
// 并通过 tornBytes 报告其字节数；文件中间出现无法解析的行属于真正损坏，返回错误。
func readValid[T any](s *FileSink, path string, cpOf func(T) uint64) (recs []T, tornBytes int, err error) {
	lines, err := readLines(path)
	if err != nil {
		return nil, 0, err
	}
	lastNonEmpty := -1
	for i, ln := range lines {
		if len(ln) > 0 {
			lastNonEmpty = i
		}
	}
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		var rec T
		if jerr := json.Unmarshal(line, &rec); jerr != nil {
			if i == lastNonEmpty {
				// 末尾残缺行：写行途中失败/被杀的典型痕迹，容忍并丢弃。
				return recs, len(line), nil
			}
			return nil, 0, fmt.Errorf("sink file %s corrupt at line %d: %w", path, i+1, jerr)
		}
		recs = append(recs, rec)
	}
	return recs, 0, nil
}

func readLines(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var lines [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := trimSpace(sc.Bytes())
		if len(line) == 0 {
			lines = append(lines, nil)
			continue
		}
		cp := make([]byte, len(line))
		copy(cp, line)
		lines = append(lines, cp)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read sink file %s: %w", path, err)
	}
	return lines, nil
}

// rewriteFile 以 tmp + fsync + 原子改名整体重写 JSONL 文件（空切片即清空/删除）。
func rewriteFile[T any](path string, records []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp-%d", path, os.Getpid())
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			out.Close()
			return err
		}
		if _, err := w.Write(data); err != nil {
			out.Close()
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			out.Close()
			return err
		}
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
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirOf(path)
}

func syncDirOf(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	return err
}

// ReadResults 读取全部已写出的窗口结果（末尾残缺行同样被容忍）。
func (s *FileSink) ReadResults() ([]WindowResult, error) {
	recs, torn, err := readValid(s, s.resultsPath, func(r WindowResult) uint64 { return r.CheckpointID })
	if err != nil {
		return nil, err
	}
	if torn > 0 {
		s.logf("SINK READ path=%s: ignored %d-byte torn trailing line", s.resultsPath, torn)
	}
	return recs, nil
}

// ReadLates 读取全部迟到记录（末尾残缺行同样被容忍）。
func (s *FileSink) ReadLates() ([]LateRecord, error) {
	recs, torn, err := readValid(s, s.latePath, func(r LateRecord) uint64 { return r.CheckpointID })
	if err != nil {
		return nil, err
	}
	if torn > 0 {
		s.logf("SINK READ path=%s: ignored %d-byte torn trailing line", s.latePath, torn)
	}
	return recs, nil
}
