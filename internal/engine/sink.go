package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrSinkFault 是故障注入产生的写出错误，测试用它区分“模拟磁盘失败”与真实 I/O 错误。
var ErrSinkFault = errors.New("simulated sink write fault")

// ErrSinkCorrupt 表示输出文件存在“内容完整但确实损坏”的行：
// 行以换行结束（一次完整落盘），却无法解析或 CRC 校验不匹配。
// 这种情况不能当成半截残行硬接，调用方必须明确失败。
// 注意：写到一半、没有换行结尾的残行不属于此错误，由修复流程裁掉。
var ErrSinkCorrupt = errors.New("sink file corrupt")

// sinkLineVersion 是输出文件每行封装格式的当前版本。
const sinkLineVersion = 1

// sinkEnvelope 是结果/迟到文件每一行的磁盘封装：业务记录 + CRC32 校验。
// 封装让“整行已完整落盘但内容被破坏”可以被明确识别（校验不过），
// 从而与“写到一半、没有换行的残行”区分开。
type sinkEnvelope struct {
	Version int             `json:"v"`
	CRC     uint32          `json:"crc"`
	Payload json.RawMessage `json:"p"`
}

// encodeSinkLine 把一条业务记录编码为带 CRC 的完整行（不含换行符）。
func encodeSinkLine(rec any) ([]byte, error) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(sinkEnvelope{
		Version: sinkLineVersion,
		CRC:     crc32.ChecksumIEEE(payload),
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// lineKind 标识一行的磁盘格式。
type lineKind int

const (
	lineEnvelope lineKind = iota // 当前格式：CRC 封装，校验已通过
	lineLegacy                   // 老格式裸记录：可读，但没有 CRC 可校验
)

// decodeSinkLine 解析一行完整记录（调用方已确保该行以换行结束）。
// 返回 ErrSinkCorrupt 的情况：封装版本未知、CRC 不匹配或内容无法解析——
// 这些都是“完整但确实坏掉”的行，绝不允许被当作残行静默处理。
func decodeSinkLine[T any](raw []byte) (T, lineKind, error) {
	var rec T
	var probe map[string]json.RawMessage
	isEnvelope := false
	if err := json.Unmarshal(raw, &probe); err == nil {
		_, hasP := probe["p"]
		_, hasCRC := probe["crc"]
		isEnvelope = hasP && hasCRC
	}
	if !isEnvelope {
		// 老版本写出的裸记录行：无 CRC，按原样接受以兼容既有文件。
		if err := json.Unmarshal(raw, &rec); err != nil {
			return rec, lineLegacy, fmt.Errorf("%w: %v", ErrSinkCorrupt, err)
		}
		return rec, lineLegacy, nil
	}
	var env sinkEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return rec, lineEnvelope, fmt.Errorf("%w: envelope: %v", ErrSinkCorrupt, err)
	}
	if env.Version != sinkLineVersion {
		return rec, lineEnvelope, fmt.Errorf("%w: unsupported line version %d", ErrSinkCorrupt, env.Version)
	}
	if crc32.ChecksumIEEE(env.Payload) != env.CRC {
		return rec, lineEnvelope, fmt.Errorf("%w: checksum mismatch", ErrSinkCorrupt)
	}
	if err := json.Unmarshal(env.Payload, &rec); err != nil {
		return rec, lineEnvelope, fmt.Errorf("%w: payload: %v", ErrSinkCorrupt, err)
	}
	return rec, lineEnvelope, nil
}

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
// 每行磁盘格式为带 CRC32 的封装（{v,crc,p}）；旧版本写出的裸记录行仍可直接读取。
//
// 恰好一次语义的实现方式：
//   - 每条写出记录携带产生它的检查点 ID 与稳定身份（窗口坐标 / 事件 ID）；
//   - 引擎先 Commit(checkpointID, ...) 再持久化该检查点；
//   - 写一半失败时，下一次提交按身份续写：残缺行被裁掉，已落盘记录被跳过，
//     只补写缺失部分，进程重启或进程内重试都不会重复写出；
//   - 进程在“提交完成、检查点未保存”之间被杀时，恢复端用 TruncateTo
//     回滚该批未确认输出后重算，因此也不会丢、不会重。
//
// 残行与真损坏的区分（见 RepairTails）：
//   - 末尾没有换行的半截字节只可能来自写入中途被杀，裁掉后幂等补齐；
//   - 完整行无法解析或 CRC 不匹配则返回 ErrSinkCorrupt，绝不硬接。
type FileSink struct {
	resultsPath string
	latePath    string

	mu sync.Mutex // 同一时刻只允许一个提交在写（续写判定需要串行）

	// 故障注入：faultRemaining>0 时，下一次 CommitBatch 在写出 faultAfter 条后失败。
	faultRemaining int
	faultAfter     int

	// 撕裂写故障注入：tornRemaining>0 时，下一次提交在写出 tornAfter 条完整记录后，
	// 把下一条记录的前 tornHalf 个字节（不含换行）落盘并 flush，然后返回 ErrSinkFault。
	// 这确定性地制造“进程在 write 中途被杀、文件末尾留下半截残行”的现场。
	tornRemaining int
	tornAfter     int
	tornHalf      int
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

// FailNextTornWrites 让接下来 n 次提交在写出 after 条完整记录后，
// 把下一条记录只落盘前 half 个字节（无换行）再返回 ErrSinkFault，
// 从而在文件末尾留下一截真实的半截残行（half<1 时取该行长度的一半）。
func (s *FileSink) FailNextTornWrites(n, after, half int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if after < 0 {
		after = 0
	}
	s.tornRemaining += n
	s.tornAfter = after
	s.tornHalf = half
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
	// tornBudget>=0 时，命中的那一条记录只落盘前若干字节（制造半截残行）。
	tornBudget := -1
	if s.tornRemaining > 0 {
		tornBudget = s.tornAfter
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
	torn := func(data []byte) int {
		if tornBudget < 0 || written < tornBudget {
			return 0
		}
		s.tornRemaining--
		tornBudget = -1
		h := s.tornHalf
		if h < 1 || h >= len(data) {
			h = len(data) / 2
		}
		if h < 1 {
			h = 1
		}
		return h
	}

	for i := range results {
		results[i].CheckpointID = checkpointID
	}
	for i := range lates {
		lates[i].CheckpointID = checkpointID
	}

	rep.ReplayedResults, rep.WrittenResults, err = appendMissing(s.resultsPath, results, rr, resultID, &written, maybeFault, torn)
	if err != nil {
		return rep, err
	}
	rep.ReplayedLates, rep.WrittenLates, err = appendMissing(s.latePath, lates, rl, lateID, &written, maybeFault, torn)
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

// TailReport 描述一次残行修复对每个输出文件做了什么，供恢复日志说明。
type TailReport struct {
	ResultsPath string
	ResultsTrim bool
	LatePath    string
	LateTrim    bool
}

// Trimmed 报告是否实际裁掉过残数据。
func (r TailReport) Trimmed() bool { return r.ResultsTrim || r.LateTrim }

// RepairTails 检查并修复两个输出文件末尾写到一半的残行。
//
// 规则（残行 vs 真损坏）：
//   - 末尾没有换行符的半截内容：只可能是进程在 write 中途被杀留下的，
//     裁掉（truncate + fsync），随后可由检查点批次幂等补齐；
//   - 以换行完整结束、但无法解析或 CRC 校验不匹配的行：属于真损坏，
//     原样保留并返回 wrapped ErrSinkCorrupt，绝不静默丢弃“完整”数据。
//
// 文件不存在视为正常（尚未产出过），返回 nil。
func (s *FileSink) RepairTails() (TailReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rep := TailReport{ResultsPath: s.resultsPath, LatePath: s.latePath}
	trimmed, err := repairPartialTail[WindowResult](s.resultsPath)
	if err != nil {
		return rep, err
	}
	rep.ResultsTrim = trimmed
	trimmed, err = repairPartialTail[LateRecord](s.latePath)
	if err != nil {
		return rep, err
	}
	rep.LateTrim = trimmed
	return rep, nil
}

// appendState 是续写前对一个输出文件的扫描结果。
type appendState struct {
	exists   map[string]struct{} // 文件中已存在的本批（同 cpID）记录身份
	repaired bool                // 是否裁掉了末尾残缺行
}

// prepareAppend 修补残缺尾行并收集已存在的本批记录身份。
// 返回的 repaired 标记本次是否实际裁掉过残数据。
func prepareAppend[T any](path string, checkpointID uint64, idOf func(T) string, cpOf func(T) uint64) (appendState, error) {
	st := appendState{exists: map[string]struct{}{}}
	repaired, err := repairPartialTail[T](path)
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
		rec, _, derr := decodeSinkLine[T](line)
		if derr != nil {
			return st, fmt.Errorf("%s: %w", path, derr)
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
//
// 判定方式：逐行扫描。完整行（以换行结束）必须可解析且 CRC 通过，
// 否则视为真正损坏并立即返回 ErrSinkCorrupt，文件原样不动；
// 读到的最后一段若没有换行结尾，则它是写入中途被杀留下的残行，
// 连同其后任何字节一起裁掉（JSON 序列化不会产生裸换行，因此残行不可能含换行）。
func repairPartialTail[T any](path string) (bool, error) {
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
			// 完整行：必须可解析、校验通过；否则是真正损坏，不做任何修改直接报错。
			trimmed := trimSpace(line)
			if len(trimmed) > 0 {
				if _, _, derr := decodeSinkLine[T](trimmed); derr != nil {
					return false, fmt.Errorf("%s: %w", path, derr)
				}
			}
			validLen = offset + n
			offset += n
			continue
		}
		if !errors.Is(rerr, io.EOF) {
			return false, rerr
		}
		// EOF：剩余这段没有以换行结束（包括纯空白），属于残行，停止扫描。
		break
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
// torn(data) 返回 >0 时，本条只写前 h 字节并 flush（不写换行）后返回 ErrSinkFault，
// 在文件末尾确定性留下一截半截残行。
func appendMissing[T any](path string, records []T, st appendState, idOf func(T) string,
	written *int, maybeFault func() error, torn func(data []byte) int) (skipped, added int, err error) {
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
		data, merr := encodeSinkLine(rec)
		if merr != nil {
			w.Flush()
			return skipped, added, merr
		}
		if h := torn(data); h > 0 {
			if _, werr := w.Write(data[:h]); werr != nil {
				w.Flush()
				return skipped, added, werr
			}
			// 不写换行并立即 flush：模拟进程在 write 中途被杀。
			if ferr := w.Flush(); ferr != nil {
				return skipped, added, ferr
			}
			return skipped, added, fmt.Errorf("%w: torn write (half line, no newline) at %s checkpoint",
				ErrSinkFault, filepath.Base(path))
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
// 调用前文件末尾可能存在写到一半的残行，先修复再扫描，避免恢复直接失败。
func (s *FileSink) TruncateTo(checkpointID uint64) error {
	if _, err := repairPartialTail[WindowResult](s.resultsPath); err != nil {
		return err
	}
	if _, err := repairPartialTail[LateRecord](s.latePath); err != nil {
		return err
	}
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
		rec, _, derr := decodeSinkLine[T](line)
		if derr != nil {
			return fmt.Errorf("%s: %w", path, derr)
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
		data, err := encodeSinkLine(rec)
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
		rec, _, derr := decodeSinkLine[T](line)
		if derr != nil {
			return nil, fmt.Errorf("%s: %w", path, derr)
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}
