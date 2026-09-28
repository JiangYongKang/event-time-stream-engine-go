package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrSinkFault 是故障注入产生的写出错误，测试用它区分“模拟磁盘失败”与真实 I/O 错误。
var ErrSinkFault = errors.New("simulated sink write fault")

// CommitReport 描述一次提交实际做了什么，用于日志与指标，
// 让“重试/重放了哪些记录”从外部可见。
type CommitReport struct {
	CheckpointID    uint64
	Resume          bool // 本次提交续写了此前失败的半成品提交
	RepairedTail    bool // 是否修补了写到一半的残缺行
	WrittenResults  int  // 本次新追加的窗口结果数
	WrittenLates    int  // 本次新追加的迟到记录数
	ReplayedResults int  // 已存在而被跳过（不重复写）的窗口结果数
	ReplayedLates   int  // 已存在而被跳过（不重复写）的迟到记录数
}

// FileSink 以本地 JSONL 文件模拟外部写出存储。
//
// 恰好一次语义的实现方式：
//   - 每条写出记录携带产生它的检查点 ID 与稳定身份（窗口坐标 / 事件 ID）；
//   - 引擎先 Commit(checkpointID, ...) 再持久化该检查点；
//   - 写一半失败时，下一次提交按身份续写：残缺行被裁掉，已落盘记录被跳过，
//     只补写缺失部分，进程重启或进程内重试都不会重复写出；
//   - 进程在“提交完成、检查点未保存”之间被杀时，恢复端用 TruncateTo
//     回滚该批未确认输出后重算，因此也不会丢、不会重。
type FileSink struct {
	resultsPath string
	latePath    string

	mu sync.Mutex // 同一时刻只允许一个提交在写（续写判定需要串行）

	// 故障注入：faultRemaining>0 时，下一次 CommitBatch 在写出 faultAfter 条后失败。
	faultRemaining int
	faultAfter     int
}

func NewFileSink(resultsPath, latePath string) *FileSink {
	return &FileSink{resultsPath: resultsPath, latePath: latePath}
}

// FailNextCommits 让接下来 n 次提交在写出 1 条记录后返回 ErrSinkFault
// （模拟磁盘写到一半失败 / 进程在写出过程中被杀）。
func (s *FileSink) FailNextCommits(n int) { s.FailNextCommitsAfter(n, 1) }

// FailNextCommitsAfter 让接下来 n 次提交在写出 after 条记录后失败。
func (s *FileSink) FailNextCommitsAfter(n, after int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if after < 1 {
		after = 1
	}
	s.faultRemaining += n
	s.faultAfter = after
}

// Commit 是兼容旧调用的薄包装。
func (s *FileSink) Commit(checkpointID uint64, results []WindowResult, lates []LateRecord) error {
	_, err := s.CommitBatch(checkpointID, results, lates)
	return err
}

// CommitBatch 提交一批窗口结果与迟到记录，支持失败后续写：
//  1. 裁掉两个文件末尾写到一半的残缺行（进程被杀的情形）；
//  2. 收集已存在的本批记录身份（上次写成功的部分）；
//  3. 只追加缺失记录；全部追加并 fsync 后才算提交成功。
//
// 该操作可安全重试任意次数：同一批记录绝不会写出第二份。
func (s *FileSink) CommitBatch(checkpointID uint64, results []WindowResult, lates []LateRecord) (CommitReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rep := CommitReport{CheckpointID: checkpointID}

	rr, err := prepareAppend[WindowResult](s.resultsPath, checkpointID, resultID,
		func(r WindowResult) uint64 { return r.CheckpointID })
	if err != nil {
		return rep, err
	}
	rl, err := prepareAppend[LateRecord](s.latePath, checkpointID, lateID,
		func(r LateRecord) uint64 { return r.CheckpointID })
	if err != nil {
		return rep, err
	}
	rep.RepairedTail = rr.repaired || rl.repaired

	// 本批允许写出多少条后触发故障（跨两个文件共享额度）。
	faultBudget := -1
	if s.faultRemaining > 0 {
		faultBudget = s.faultAfter
	}
	written := 0
	maybeFault := func() error {
		if faultBudget < 0 {
			return nil
		}
		if written >= faultBudget {
			s.faultRemaining--
			faultBudget = -1
			return fmt.Errorf("%w: checkpoint=%d after %d record(s)", ErrSinkFault, checkpointID, written)
		}
		return nil
	}

	for i := range results {
		results[i].CheckpointID = checkpointID
	}
	for i := range lates {
		lates[i].CheckpointID = checkpointID
	}

	rep.ReplayedResults, rep.WrittenResults, err = appendMissing(s.resultsPath, results, rr, resultID, &written, maybeFault)
	if err != nil {
		return rep, err
	}
	rep.ReplayedLates, rep.WrittenLates, err = appendMissing(s.latePath, lates, rl, lateID, &written, maybeFault)
	if err != nil {
		return rep, err
	}
	rep.Resume = rep.ReplayedResults > 0 || rep.ReplayedLates > 0 || rep.RepairedTail
	return rep, nil
}

func resultID(r WindowResult) string {
	return fmt.Sprintf("R|%s|%d|%d", r.Key, r.WindowStart, r.WindowEnd)
}

func lateID(r LateRecord) string { return "L|" + r.Event.ID }

// appendState 是续写前对一个输出文件的扫描结果。
type appendState struct {
	exists   map[string]struct{} // 文件中已存在的本批（同 cpID）记录身份
	repaired bool                // 是否裁掉了末尾残缺行
}

// prepareAppend 修补残缺尾行并收集已存在的本批记录身份。
// 返回的 repaired 标记本次是否实际裁掉过残数据。
func prepareAppend[T any](path string, checkpointID uint64, idOf func(T) string, cpOf func(T) uint64) (appendState, error) {
	st := appendState{exists: map[string]struct{}{}}
	repaired, err := repairPartialTail(path)
	if err != nil {
		return st, err
	}
	st.repaired = repaired
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return st, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := trimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec T
		if err := json.Unmarshal(line, &rec); err != nil {
			return st, fmt.Errorf("sink file %s corrupt: %w", path, err)
		}
		if cpOf(rec) > checkpointID {
			return st, fmt.Errorf("sink file %s contains future checkpoint %d (committing %d): refusing to resume",
				path, cpOf(rec), checkpointID)
		}
		if cpOf(rec) == checkpointID {
			st.exists[idOf(rec)] = struct{}{}
		}
	}
	if err := sc.Err(); err != nil {
		return st, err
	}
	return st, nil
}

// repairPartialTail 裁掉文件末尾写到一半的残数据，返回是否实际裁剪过。
// 判定方式：逐行扫描，保留“以换行结束且可解析”的最长合法前缀，
// 其后的任何字节（无换行的半截行，或有换行但解析失败的最后一行）都裁掉。
func repairPartialTail(path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := info.Size()
	if size == 0 {
		return false, nil
	}
	reader := bufio.NewReader(f)
	var validLen int64
	var offset int64
	for {
		line, rerr := reader.ReadBytes('\n')
		n := int64(len(line))
		if rerr == nil {
			// 完整行：可解析才算合法前缀。
			var raw map[string]any
			if json.Unmarshal(trimSpace(line), &raw) == nil {
				validLen = offset + n
			}
		} else {
			// 读到 EOF（或其它错误）且没有以换行结束 -> 残行，停止。
			break
		}
		offset += n
	}
	if validLen == size {
		return false, nil
	}
	if err := f.Truncate(validLen); err != nil {
		return false, err
	}
	return true, f.Sync()
}

// appendMissing 追加文件中尚不存在的本批记录，返回（已存在跳过数, 新追加数）。
// 每条记录立即 flush，使“写一半失败”后已写部分真实可见，续写才能演示去重。
func appendMissing[T any](path string, records []T, st appendState, idOf func(T) string,
	written *int, maybeFault func() error) (skipped, added int, err error) {
	if len(records) == 0 {
		return 0, 0, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, 0, err
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return 0, 0, err
	}
	defer out.Close()
	w := bufio.NewWriter(out)
	for _, rec := range records {
		if _, ok := st.exists[idOf(rec)]; ok {
			skipped++
			continue
		}
		if ferr := maybeFault(); ferr != nil {
			w.Flush()
			return skipped, added, ferr
		}
		data, merr := json.Marshal(rec)
		if merr != nil {
			w.Flush()
			return skipped, added, merr
		}
		if _, werr := w.Write(data); werr != nil {
			w.Flush()
			return skipped, added, werr
		}
		if werr := w.WriteByte('\n'); werr != nil {
			w.Flush()
			return skipped, added, werr
		}
		// 每条都 flush：保证故障点之前的记录对后续续写可见。
		if ferr := w.Flush(); ferr != nil {
			return skipped, added, ferr
		}
		added++
		*written++
	}
	if err := w.Flush(); err != nil {
		return skipped, added, err
	}
	if err := out.Sync(); err != nil {
		return skipped, added, err
	}
	return skipped, added, nil
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
