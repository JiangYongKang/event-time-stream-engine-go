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
	"sync"
	"sync/atomic"
)

var (
	// ErrCheckpointNotFound 表示没有任何可用检查点（首次启动属正常情况）。
	ErrCheckpointNotFound = errors.New("checkpoint not found")
	// ErrCheckpointCorrupt 表示检查点文件存在但内容损坏（校验和不匹配或无法解析）。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt")
	// ErrCheckpointVersion 表示检查点由不兼容的版本写入（太新或太旧）。
	ErrCheckpointVersion = errors.New("checkpoint version mismatch")
)

const (
	// checkpointVersion 是当前进程写出的检查点版本。
	checkpointVersion = 2
	// minSupportedCheckpointVersion 是仍可读取的最老版本：
	// 升级前由老版本进程写出的 v1 检查点必须能继续恢复。
	minSupportedCheckpointVersion = 1

	genDirSuffix  = ".d"
	genFilePrefix = "cp-"
	genFileSuffix = ".json"
)

// Checkpoint 覆盖一次一致快照所需的全部状态：
// 读取位点、各分区水位、窗口累计值与去重集合。
//
// 兼容性：v2 仅在 v1 基础上新增可选字段（omitempty）。
// v1 文件缺这些字段时按零值读入，不影响恢复；v2 文件也不写入零值冗余字段。
type Checkpoint struct {
	ID        uint64        `json:"id"`
	Version   int           `json:"version"`
	Offset    int64         `json:"offset"`
	Watermark map[int]int64 `json:"watermark"`
	Windows   []WindowState `json:"windows"`
	SeenIDs   []string      `json:"seen_ids"`
	// CreatedAtMs 为 v2 新增：检查点生成时刻（unix 毫秒），仅用于可观测。
	CreatedAtMs int64 `json:"created_at_ms,omitempty"`
}

// checkpointFile 是磁盘格式：payload + CRC32 校验和（v1/v2 相同）。
type checkpointFile struct {
	Payload  json.RawMessage `json:"payload"`
	Checksum uint32          `json:"checksum"`
}

// CheckpointStore 基于本地文件的检查点存储。
//
// 布局（v2）：
//
//	<path>.d/cp-<20位ID>.json   每代一个不可变文件（tmp 写入 + rename）
//
// 升级前的 v1 单文件 <path> 仍可读取；第一次成功写出 v2 后旧文件被移除。
type CheckpointStore struct {
	path string // v1 单文件路径（兼容读取）
	dir  string // v2 代数目录

	maxGenMu       sync.Mutex
	maxGenerations int

	evicted atomic.Uint64

	legacyMu      sync.Mutex
	legacyRemoved bool
}

func NewCheckpointStore(path string) *CheckpointStore {
	return &CheckpointStore{path: path, dir: path + genDirSuffix, maxGenerations: 3}
}

// SetMaxGenerations 设置保留的检查点代数上限（<=0 时取默认值 3）。
func (s *CheckpointStore) SetMaxGenerations(n int) {
	if n <= 0 {
		n = 3
	}
	s.maxGenMu.Lock()
	s.maxGenerations = n
	s.maxGenMu.Unlock()
}

// Evictions 返回因代数上限被删除、以及恢复旧代时被裁剪的检查点文件总数。
func (s *CheckpointStore) Evictions() uint64 { return s.evicted.Load() }

// ConsumeLegacyRemoved 返回并清除“本次运行中已把 v1 旧文件升级移除”标记。
func (s *CheckpointStore) ConsumeLegacyRemoved() bool {
	s.legacyMu.Lock()
	defer s.legacyMu.Unlock()
	v := s.legacyRemoved
	s.legacyRemoved = false
	return v
}

func (s *CheckpointStore) maxGen() int {
	s.maxGenMu.Lock()
	defer s.maxGenMu.Unlock()
	return s.maxGenerations
}

func genPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%020d%s", genFilePrefix, id, genFileSuffix))
}

// parseGenName 从文件名解析代数 ID；非代数文件返回 ok=false。
func parseGenName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, genFilePrefix) || !strings.HasSuffix(name, genFileSuffix) {
		return 0, false
	}
	mid := name[len(genFilePrefix) : len(name)-len(genFileSuffix)]
	id, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// Save 原子写入一代检查点：tmp + fsync + rename，并按代数上限裁剪旧代。
func (s *CheckpointStore) Save(cp *Checkpoint) error {
	cp.Version = checkpointVersion
	payload, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	file := checkpointFile{Payload: payload, Checksum: crc32.ChecksumIEEE(payload)}
	data, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("marshal checkpoint file: %w", err)
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	final := genPath(s.dir, cp.ID)
	tmp := fmt.Sprintf("%s.tmp-%d", final, os.Getpid())
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
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	if err := s.prune(cp.ID); err != nil {
		return err
	}
	// 首次 v2 写出成功后，升级前留下的 v1 单文件不再需要。
	if _, err := os.Stat(s.path); err == nil {
		if err := os.Remove(s.path); err != nil {
			return fmt.Errorf("remove legacy checkpoint after upgrade: %w", err)
		}
		s.legacyMu.Lock()
		s.legacyRemoved = true
		s.legacyMu.Unlock()
	}
	return nil
}

// prune 保留最新的 maxGenerations 代（含 keepID），删除更旧的代数文件。
func (s *CheckpointStore) prune(keepID uint64) error {
	ids, err := s.listIDs()
	if err != nil {
		return err
	}
	keep := s.maxGen()
	// ids 升序；最新 keep 代保留（末尾），其余比 keepID 旧的删除。
	removed := 0
	for i := 0; i+keep < len(ids); i++ {
		if ids[i] >= keepID {
			break
		}
		if err := os.Remove(genPath(s.dir, ids[i])); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed++
	}
	if removed > 0 {
		s.evicted.Add(uint64(removed))
		if err := fsyncDir(s.dir); err != nil {
			return err
		}
	}
	return nil
}

// PruneFrom 删除所有 ID 严格大于 id 的代数文件。
// 用于“从指定旧代恢复”：比恢复点更新的代数必须废弃，避免之后误用。
func (s *CheckpointStore) PruneFrom(id uint64) (int, error) {
	ids, err := s.listIDs()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, g := range ids {
		if g > id {
			if err := os.Remove(genPath(s.dir, g)); err != nil && !os.IsNotExist(err) {
				return removed, err
			}
			removed++
		}
	}
	if removed > 0 {
		s.evicted.Add(uint64(removed))
		if err := fsyncDir(s.dir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// listIDs 返回升序的全部代数 ID（忽略临时文件与无关文件）。
func (s *CheckpointStore) listIDs() ([]uint64, error) {
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

// List 返回降序的全部可用代数 ID（最新在前）。
func (s *CheckpointStore) List() ([]uint64, error) {
	ids, err := s.listIDs()
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(ids))
	for i := len(ids) - 1; i >= 0; i-- {
		out = append(out, ids[i])
	}
	return out, nil
}

// Load 加载最新检查点：优先 v2 代数目录；目录为空时回退读取 v1 单文件。
// 最新代数损坏时直接报错，绝不静默改用更旧代数“从中间硬接”。
func (s *CheckpointStore) Load() (*Checkpoint, error) {
	ids, err := s.listIDs()
	if err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		return s.LoadID(ids[len(ids)-1])
	}
	return s.loadLegacy()
}

// LoadID 加载指定代数；id 为 0 时等价于 Load（最新代数）。
func (s *CheckpointStore) LoadID(id uint64) (*Checkpoint, error) {
	if id == 0 {
		return s.Load()
	}
	return s.loadFile(genPath(s.dir, id), id)
}

func (s *CheckpointStore) loadLegacy() (*Checkpoint, error) {
	return s.loadFile(s.path, 0)
}

// LoadLegacy 显式读取 v1 单文件（主要用于测试与诊断）。
func (s *CheckpointStore) LoadLegacy() (*Checkpoint, error) { return s.loadLegacy() }

// loadFile 读取并校验单个检查点文件。失败原因严格区分：
// 文件不存在 -> ErrCheckpointNotFound；
// 读失败（权限等）-> 原始 OS 错误（不是损坏）；
// 内容损坏 -> ErrCheckpointCorrupt；
// 版本不兼容 -> ErrCheckpointVersion。
func (s *CheckpointStore) loadFile(path string, id uint64) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if id > 0 {
				return nil, fmt.Errorf("%w: generation %d (%s)", ErrCheckpointNotFound, id, path)
			}
			return nil, ErrCheckpointNotFound
		}
		// 权限不足、IO 错误等“读不了”：保持原始错误，不归类为损坏。
		return nil, fmt.Errorf("read checkpoint %s: %w", path, err)
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
	if cp.Version < minSupportedCheckpointVersion || cp.Version > checkpointVersion {
		return nil, fmt.Errorf("%w: %s: got version %d, supported [%d,%d]",
			ErrCheckpointVersion, path, cp.Version, minSupportedCheckpointVersion, checkpointVersion)
	}
	return &cp, nil
}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
