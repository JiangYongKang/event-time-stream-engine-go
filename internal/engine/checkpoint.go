package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	// ErrCheckpointNotFound 表示没有任何可用检查点（首次启动属正常情况）。
	ErrCheckpointNotFound = errors.New("checkpoint not found")
	// ErrCheckpointCorrupt 表示检查点文件存在但内容损坏（校验和不匹配或无法解析）。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt")
	// ErrCheckpointVersion 表示检查点由不兼容的版本写入。
	ErrCheckpointVersion = errors.New("checkpoint version mismatch")
)

const (
	// checkpointVersion 是当前写出的检查点格式版本。
	checkpointVersion = 2
	// minSupportedVersion 是仍可读取的最老格式版本：
	// v1（上一版本的单文件检查点）升级后必须还能读、还能接着用。
	minSupportedVersion = 1
)

// Checkpoint 覆盖一次一致快照所需的全部状态：
// 读取位点、各分区水位、窗口累计值与去重集合。
//
// 兼容性：新增字段一律 omitempty，v1 写出的文件没有这些字段也能正常解码。
type Checkpoint struct {
	ID        uint64        `json:"id"`
	Version   int           `json:"version"`
	Offset    int64         `json:"offset"`
	Watermark map[int]int64 `json:"watermark"`
	Windows   []WindowState `json:"windows"`
	SeenIDs   []string      `json:"seen_ids"`
	// PendingResults / PendingLates 是“本代提交时写出、但需在恢复时重放确认”的
	// 最近一批输出。它们与已提交到 sink 的批次内容相同：
	// 正常保存后这批即被确认（ID 相同，TruncateTo 不会删）；
	// 若崩溃发生在 sink 写出与本代保存之间，恢复端先回滚再据此幂等续写，不丢不重。
	// 对 v1 老检查点：字段缺省为 nil，语义与“没有未确认批次”一致。
	PendingResults []WindowResult `json:"pending_results,omitempty"`
	PendingLates   []LateRecord   `json:"pending_lates,omitempty"`
	// CreatedAtUnix 为 v2 新增：该代检查点的生成时间（秒）。仅用于排查展示。
	CreatedAtUnix int64 `json:"created_at_unix,omitempty"`
}

// checkpointFile 是磁盘格式：payload + CRC32 校验和。
type checkpointFile struct {
	Payload  json.RawMessage `json:"payload"`
	Checksum uint32          `json:"checksum"`
}

// SaveInfo 汇报一次检查点持久化的附带结果，便于日志说明保留/迁移情况。
type SaveInfo struct {
	ID       uint64   // 本次保存的检查点代
	Pruned   []uint64 // 因超过保留代数被删除的旧代
	Migrated bool     // 本次是否从老版单文件布局迁移而来
}

const (
	genPrefix = "checkpoint-"
	genSuffix = ".json"
	genWidth  = 10
)

// CheckpointStore 基于本地文件的原子检查点存储。
//
// 布局（v2，多代）：
//   - 每代一个不可变文件 <dir>/checkpoint-NNNNNNNNNN.json（tmp 写入 + rename）；
//   - <legacyPath>（老版本的单文件路径）始终保留为“最新一代”的指针副本，
//     老工具/老版本布局仍能识别；升级后第一次保存即完成迁移；
//   - 超过 Retain 代的旧文件按 ID 顺序删除。
//
// 兼容性：若目录下没有任何代文件，则 Load 回退读取 legacyPath，
// 因此上一版本留下的老检查点文件升级后仍可直接恢复。
type CheckpointStore struct {
	dir        string // 代文件所在目录
	legacyPath string // 老版本单文件路径（同时作为最新代指针副本）
	retain     int    // 保留的最大代数
}

// NewCheckpointStore 创建存储。path 为检查点主路径（兼容老版本的单文件路径），
// 代文件保存在其同目录下。默认保留 3 代，可用 SetRetain 调整。
func NewCheckpointStore(path string) *CheckpointStore {
	return &CheckpointStore{
		dir:        filepath.Dir(path),
		legacyPath: path,
		retain:     3,
	}
}

// SetRetain 设置保留的检查点代数（小于 1 视为 1）。
func (s *CheckpointStore) SetRetain(n int) *CheckpointStore {
	if n < 1 {
		n = 1
	}
	s.retain = n
	return s
}

// Retain 返回保留代数。
func (s *CheckpointStore) Retain() int { return s.retain }

func genName(id uint64) string {
	return fmt.Sprintf("%s%0*d%s", genPrefix, genWidth, id, genSuffix)
}

func (s *CheckpointStore) genPath(id uint64) string {
	return filepath.Join(s.dir, genName(id))
}

// parseGenName 仅识别严格命名的代文件，忽略目录中的其他文件（input/results 等）。
func parseGenName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, genPrefix) || !strings.HasSuffix(name, genSuffix) {
		return 0, false
	}
	mid := name[len(genPrefix) : len(name)-len(genSuffix)]
	if len(mid) != genWidth {
		return 0, false
	}
	id, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// Generations 按 ID 升序返回当前存在的检查点代。
func (s *CheckpointStore) Generations() ([]uint64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []uint64
	for _, en := range entries {
		if en.IsDir() {
			continue
		}
		if id, ok := parseGenName(en.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// marshal 序列化并加 CRC 校验（版本由调用方设置）。
func marshalCheckpoint(cp *Checkpoint) ([]byte, error) {
	payload, err := json.Marshal(cp)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint: %w", err)
	}
	file := checkpointFile{Payload: payload, Checksum: crc32.ChecksumIEEE(payload)}
	data, err := json.Marshal(file)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint file: %w", err)
	}
	return data, nil
}

// writeAtomic 先写临时文件并 fsync，再 rename 覆盖目标。
func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Save 原子写入一代检查点，刷新最新代指针副本，并按保留代数清理旧代。
func (s *CheckpointStore) Save(cp *Checkpoint) error {
	_, err := s.SaveWithInfo(cp)
	return err
}

// SaveWithInfo 与 Save 相同，但返回迁移/清理信息供日志展示。
func (s *CheckpointStore) SaveWithInfo(cp *Checkpoint) (*SaveInfo, error) {
	cp.Version = checkpointVersion

	ids, err := s.Generations()
	if err != nil {
		return nil, fmt.Errorf("list checkpoint generations: %w", err)
	}
	_, legacyErr := os.Stat(s.legacyPath)
	legacyExists := legacyErr == nil
	migrated := len(ids) == 0 && legacyExists

	data, err := marshalCheckpoint(cp)
	if err != nil {
		return nil, err
	}
	// 1) 先写不可变代文件。
	if err := writeAtomic(s.genPath(cp.ID), data); err != nil {
		return nil, fmt.Errorf("write checkpoint generation: %w", err)
	}
	// 2) 再刷新老路径指针副本（老版本工具与升级流程依赖它）。
	if err := writeAtomic(s.legacyPath, data); err != nil {
		return nil, fmt.Errorf("refresh checkpoint pointer: %w", err)
	}

	// 3) 超出保留代数的旧代按 ID 升序删除（绝不删刚写的当代）。
	ids, err = s.Generations()
	if err != nil {
		return nil, fmt.Errorf("list checkpoint generations after save: %w", err)
	}
	info := &SaveInfo{ID: cp.ID, Migrated: migrated}
	if excess := len(ids) - s.retain; excess > 0 {
		for _, id := range ids[:excess] {
			if err := os.Remove(s.genPath(id)); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("prune checkpoint generation %d: %w", id, err)
			}
			info.Pruned = append(info.Pruned, id)
		}
	}
	return info, nil
}

// loadFile 读取并校验单个检查点文件。失败原因可区分：
// 不存在 -> ErrCheckpointNotFound；损坏 -> ErrCheckpointCorrupt；版本不符 -> ErrCheckpointVersion。
func loadFile(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}
	var file checkpointFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCheckpointCorrupt, path, err)
	}
	if crc32.ChecksumIEEE(file.Payload) != file.Checksum {
		return nil, fmt.Errorf("%w: %s: checksum mismatch", ErrCheckpointCorrupt, path)
	}
	var cp Checkpoint
	if err := json.Unmarshal(file.Payload, &cp); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCheckpointCorrupt, path, err)
	}
	if cp.Version < minSupportedVersion || cp.Version > checkpointVersion {
		return nil, fmt.Errorf("%w: %s: got %d, supported [%d,%d]",
			ErrCheckpointVersion, path, cp.Version, minSupportedVersion, checkpointVersion)
	}
	return &cp, nil
}

// Load 读取最新一代检查点。
// 优先读取多代文件；没有任何代文件时回退到老版本单文件（v1/v2 均可）。
// 目标文件损坏或版本不符时明确返回对应错误，绝不静默回退到其他代“硬接”。
func (s *CheckpointStore) Load() (*Checkpoint, error) {
	ids, err := s.Generations()
	if err != nil {
		return nil, fmt.Errorf("list checkpoint generations: %w", err)
	}
	if len(ids) > 0 {
		return loadFile(s.genPath(ids[len(ids)-1]))
	}
	cp, err := loadFile(s.legacyPath)
	if err != nil {
		if errors.Is(err, ErrCheckpointNotFound) {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}
	return cp, nil
}

// LoadGeneration 读取指定代检查点，用于“从哪一代恢复”。
// 若代文件已被保留策略清理，则回退匹配老路径指针；仍不匹配则返回 ErrCheckpointNotFound。
func (s *CheckpointStore) LoadGeneration(id uint64) (*Checkpoint, error) {
	cp, err := loadFile(s.genPath(id))
	if err == nil {
		return cp, nil
	}
	if !errors.Is(err, ErrCheckpointNotFound) {
		return nil, err
	}
	// 代文件不存在：可能该代正好是指针副本（例如只跑过一次的老布局）。
	cp, err = loadFile(s.legacyPath)
	if err != nil {
		if errors.Is(err, ErrCheckpointNotFound) {
			return nil, fmt.Errorf("%w: generation %d", ErrCheckpointNotFound, id)
		}
		return nil, err
	}
	if cp.ID != id {
		return nil, fmt.Errorf("%w: generation %d (latest is %d)", ErrCheckpointNotFound, id, cp.ID)
	}
	return cp, nil
}

// DeleteGenerationsAfter 删除 ID 大于 target 的所有代文件，
// 用于“从旧代恢复”时清理分叉的未来代；返回被删除的代数。
// 老路径指针副本不动（下一次 Save 自然刷新）。
func (s *CheckpointStore) DeleteGenerationsAfter(target uint64) (int, error) {
	ids, err := s.Generations()
	if err != nil {
		return 0, fmt.Errorf("list checkpoint generations: %w", err)
	}
	n := 0
	for _, id := range ids {
		if id <= target {
			continue
		}
		if err := os.Remove(s.genPath(id)); err != nil && !os.IsNotExist(err) {
			return n, fmt.Errorf("delete forked generation %d: %w", id, err)
		}
		n++
	}
	return n, nil
}
