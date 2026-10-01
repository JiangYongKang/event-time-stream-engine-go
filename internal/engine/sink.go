package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// ErrSinkFault 是故障注入产生的写出错误，测试用它区分“模拟磁盘失败”与真实 I/O 错误。
var ErrSinkFault = errors.New("simulated sink write fault")

// ErrSinkCorrupt 表示输出文件里一条“以换行结束的完整记录”未通过校验
// （无法解析 / 格式未知 / CRC32 不匹配）。这属于真正的文件损坏，
// 与“末尾没写完的半截残行”严格区分：后者启动时自动裁掉补齐，前者明确报错、绝不硬接。
var ErrSinkCorrupt = errors.New("sink file corrupt")

// ErrSinkPartialTail 表示文件末尾残留一段没有换行结束的半截记录
// （写出中途进程被杀）。引擎在提交/回滚前会自动裁掉它；
// 只读扫描遇到它时单独报错，提示“重启引擎即可修补”，而不是混同为损坏。
var ErrSinkPartialTail = errors.New("sink file partial tail")

// sinkFormatV1 是当前写出记录封装的格式标记。旧版本直接把
// WindowResult/LateRecord 裸 JSON 写成一行（没有 format 字段），读取时仍兼容。
const sinkFormatV1 = "etse-jsonl/v1"

// sinkEnvelope 是输出文件每行的封装：业务记录原样 JSON + CRC32。
// CRC 让“内容完整但字节已损坏”的记录能被明确识别为真正损坏，
// 而不会被当成可修补残行或正常记录。
type sinkEnvelope struct {
	Format string          `json:"format"`
	CRC32  uint32          `json:"crc32"`
	Body   json.RawMessage `json:"body"`
}

// encodeRecord 把一条业务记录封装成带 CRC32 的 JSON 行（不含换行）。
func encodeRecord[T any](rec T) ([]byte, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal sink record: %w", err)
	}
	data, err := json.Marshal(sinkEnvelope{
		Format: sinkFormatV1,
		CRC32:  crc32.ChecksumIEEE(body),
		Body:   body,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal sink envelope: %w", err)
	}
	return data, nil
}

// decodeRecord 解析输出文件中的一行（不含行尾换行）。
// legacy=true 表示该行是旧版本裸 JSON（无 CRC，按兼容语义信任）；
// 新封装行必须格式与 CRC32 都通过，否则返回包装了 ErrSinkCorrupt 的错误。
func decodeRecord[T any](path string, line []byte) (rec T, legacy bool, err error) {
	var probe map[string]json.RawMessage
	if uerr := json.Unmarshal(line, &probe); uerr != nil {
		return rec, false, sinkCorruptError(path, fmt.Sprintf("complete line is not valid JSON: %v", uerr))
	}
	if probe == nil {
		return rec, false, sinkCorruptError(path, "complete line is not a JSON object")
	}
	if _, marked := probe["format"]; marked {
		var env sinkEnvelope
		if jerr := json.Unmarshal(line, &env); jerr != nil {
			return rec, false, sinkCorruptError(path, fmt.Sprintf("record envelope invalid: %v", jerr))
		}
		if env.Format != sinkFormatV1 {
			return rec, false, sinkCorruptError(path, fmt.Sprintf("unknown record format %q", env.Format))
		}
		if crc32.ChecksumIEEE(env.Body) != env.CRC32 {
			return rec, false, sinkCorruptError(path, "record checksum mismatch: file content is damaged (a complete record, not a partial tail)")
		}
		if derr := json.Unmarshal(env.Body, &rec); derr != nil {
			return rec, false, sinkCorruptError(path, fmt.Sprintf("record body invalid: %v", derr))
		}
		return rec, false, nil
	}
	// 旧版本裸记录：无 CRC 可校验，按向后兼容直接解码。
	if derr := json.Unmarshal(line, &rec); derr != nil {
		return rec, false, sinkCorruptError(path, fmt.Sprintf("legacy record invalid: %v", derr))
	}
	return rec, true, nil
}

func sinkCorruptError(path, detail string) error {
	return fmt.Errorf("%w: %s: %s", ErrSinkCorrupt, path, detail)
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

	logger *log.Logger // 残行修补/真正损坏的区分日志；nil 则丢弃
}

func NewFileSink(resultsPath, latePath string) *FileSink {
	return &FileSink{resultsPath: resultsPath, latePath: latePath}
}

// SetLogger 注入运行日志（仅应在引擎启动、无并发写出时设置）。
func (s *FileSink) SetLogger(l *log.Logger) { s.logger = l }

func (s *FileSink) log() *log.Logger {
	if s.logger != nil {
		return s.logger
	}
	return log.New(nopSinkWriter{}, "", 0)
}

type nopSinkWriter struct{}

func (nopSinkWriter) Write(p []byte) (int, error) { return len(p), nil }

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
	logger := s.log()

	rr, err := prepareAppend[WindowResult](s.resultsPath, checkpointID, logger,
		resultID, func(r WindowResult) uint64 { return r.CheckpointID })
	if err != nil {
		return rep, err
	}
	rl, err := prepareAppend[LateRecord](s.latePath, checkpointID, logger,
		lateID, func(r LateRecord) uint64 { return r.CheckpointID })
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
func prepareAppend[T any](path string, checkpointID uint64, logger *log.Logger,
	idOf func(T) string, cpOf func(T) uint64) (appendState, error) {
	st := appendState{exists: map[string]struct{}{}}
	repaired, err := repairPartialTail[T](path, logger)
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
	err = scanRecords[T](f, path, func(rec T, _ bool) error {
		if cpOf(rec) > checkpointID {
			return fmt.Errorf("sink file %s contains future checkpoint %d (committing %d): refusing to resume",
				path, cpOf(rec), checkpointID)
		}
		if cpOf(rec) == checkpointID {
			st.exists[idOf(rec)] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return st, err
	}
	return st, nil
}

// repairPartialTail 检查并裁掉文件末尾“写到一半的半截残行”，返回是否实际裁剪过。
//
// 判定原则（严格区分可修补残行与真正损坏）：
//   - 逐行读取：以 '\n' 结束的每一行都视为“完整写完”的记录，
//     必须能通过格式/CRC32 校验；任何一条完整行校验失败 -> ErrSinkCorrupt，
//     明确报错，绝不当残行裁掉（它前面可能还有好数据，会静默丢记录）；
//   - 只有文件最后一段“没有 '\n' 结束的尾巴”才是写出中途被杀留下的残行：
//     无论它是否恰好是合法 JSON（可能是没来得及写换行的完整记录），
//     由于该批尚未确认，一律裁掉并由后续提交/重放补齐；
//   - 纯空白尾巴（如尾随空行）不裁剪、不算修补。
func repairPartialTail[T any](path string, logger *log.Logger) (bool, error) {
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
			// 完整行：必须通过校验；失败即真正损坏，报错且不裁剪。
			if _, _, derr := decodeRecord[T](path, trimSpace(line)); derr != nil {
				logger.Printf("SINK CORRUPT %s: %v (complete record failed verification; not treated as partial tail, refusing recovery)", path, derr)
				return false, derr
			}
			validLen = offset + n
			offset += n
			continue
		}
		if !errors.Is(rerr, io.EOF) {
			return false, rerr
		}
		// 读到文件尾：line 是最后一段没有换行结束的字节（可能为空）。
		if len(line) > 0 && len(trimSpace(line)) > 0 {
			// 半截残行（进程写出中途被杀）：截到最后一条完整记录之后。
			logger.Printf("SINK TAIL REPAIR %s: discarding %d unterminated byte(s) left by a killed mid-write at offset %d (batch will be re-written, no data loss)",
				path, len(line), offset)
			if terr := f.Truncate(validLen); terr != nil {
				return false, terr
			}
			if serr := f.Sync(); serr != nil {
				return false, serr
			}
			return true, nil
		}
		break
	}
	return false, nil
}

// scanRecords 逐行扫描一个已确认没有残尾的输出文件（调用方应先 repairPartialTail）。
// cb 对每条校验通过的完整记录回调；任何校验失败/残尾都返回可区分错误。
func scanRecords[T any](r io.Reader, path string, cb func(rec T, legacy bool) error) error {
	reader := bufio.NewReader(r)
	for {
		line, rerr := reader.ReadBytes('\n')
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return rerr
			}
			if len(line) > 0 && len(trimSpace(line)) > 0 {
				return fmt.Errorf("%w: %s: unterminated trailing record (restart engine to repair the partial tail)", ErrSinkPartialTail, path)
			}
			return nil
		}
		line = trimSpace(line)
		if len(line) == 0 {
			continue
		}
		rec, _, derr := decodeRecord[T](path, line)
		if derr != nil {
			return derr
		}
		if err := cb(rec, false); err != nil {
			return err
		}
	}
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
		data, merr := encodeRecord(rec)
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

// TruncateReport 汇报一次崩溃恢复回滚在输出文件上实际做了什么。
type TruncateReport struct {
	ResultsRepaired bool // results.jsonl 是否裁掉了半截残行
	LatesRepaired   bool // late.jsonl 是否裁掉了半截残行
}

// TruncateTo 回滚所有属于 checkpointID 之后（未确认）检查点的写出记录。
// 用于崩溃恢复：检查点保存成功之前的 Commit 视为未提交，必须撤销。
//
// 回滚前先修补两个文件末尾的半截残行：进程可能在写出这批未确认记录时
// 直接被杀，残行本身没有合法换行、扫描会失败；它属于未确认批，
// 随回滚一起裁掉，随后由检查点状态重算/批次重放补齐。
// 一条“以换行结束”的完整记录若校验失败，属于真正损坏，直接报错、绝不硬接。
func (s *FileSink) TruncateTo(checkpointID uint64) (TruncateReport, error) {
	var rep TruncateReport
	r1, err := truncateFile[WindowResult](s.resultsPath, checkpointID, s.log(),
		func(r WindowResult) uint64 { return r.CheckpointID })
	if err != nil {
		return rep, err
	}
	r2, err := truncateFile[LateRecord](s.latePath, checkpointID, s.log(),
		func(r LateRecord) uint64 { return r.CheckpointID })
	if err != nil {
		return rep, err
	}
	rep.ResultsRepaired = r1
	rep.LatesRepaired = r2
	return rep, nil
}

// truncateFile 先裁掉末尾半截残行，再重写只含 cp<=checkpointID 的记录，
// 返回是否修补过残行。
func truncateFile[T any](path string, checkpointID uint64, logger *log.Logger, cpOf func(T) uint64) (bool, error) {
	repaired, err := repairPartialTail[T](path, logger)
	if err != nil {
		return false, err
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return repaired, nil
		}
		return repaired, err
	}
	var kept []T
	scanErr := scanRecords[T](f, path, func(rec T, _ bool) error {
		if cpOf(rec) <= checkpointID {
			kept = append(kept, rec)
		}
		return nil
	})
	f.Close()
	if scanErr != nil {
		return repaired, scanErr
	}
	tmp := path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return repaired, err
	}
	w := bufio.NewWriter(out)
	for _, rec := range kept {
		data, err := encodeRecord(rec)
		if err != nil {
			out.Close()
			return repaired, err
		}
		if _, err := w.Write(data); err != nil {
			out.Close()
			return repaired, err
		}
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		out.Close()
		return repaired, err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return repaired, err
	}
	if err := out.Close(); err != nil {
		return repaired, err
	}
	return repaired, os.Rename(tmp, path)
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
	err = scanRecords[T](f, path, func(rec T, _ bool) error {
		out = append(out, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
