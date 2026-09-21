package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

var (
	// ErrCheckpointNotFound 表示没有任何可用检查点（首次启动属正常情况）。
	ErrCheckpointNotFound = errors.New("checkpoint not found")
	// ErrCheckpointCorrupt 表示检查点文件存在但内容损坏（校验和不匹配或无法解析）。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt")
	// ErrCheckpointVersion 表示检查点由不兼容的版本写入。
	ErrCheckpointVersion = errors.New("checkpoint version mismatch")
)

const checkpointVersion = 1

// Checkpoint 覆盖一次一致快照所需的全部状态：
// 读取位点、各分区水位、窗口累计值与去重集合。
type Checkpoint struct {
	ID        uint64        `json:"id"`
	Version   int           `json:"version"`
	Offset    int64         `json:"offset"`
	Watermark map[int]int64 `json:"watermark"`
	Windows   []WindowState `json:"windows"`
	SeenIDs   []string      `json:"seen_ids"`
}

// checkpointFile 是磁盘格式：payload + CRC32 校验和。
type checkpointFile struct {
	Payload  json.RawMessage `json:"payload"`
	Checksum uint32          `json:"checksum"`
}

// CheckpointStore 基于本地文件的原子检查点存储（tmp 写入 + rename）。
type CheckpointStore struct {
	path string
}

func NewCheckpointStore(path string) *CheckpointStore { return &CheckpointStore{path: path} }

// Save 原子写入检查点：先写临时文件并 fsync，再 rename 覆盖。
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
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
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
	return os.Rename(tmp, s.path)
}

// Load 读取并校验检查点。失败原因可区分：
// 不存在 -> ErrCheckpointNotFound；损坏 -> ErrCheckpointCorrupt；版本不符 -> ErrCheckpointVersion。
func (s *CheckpointStore) Load() (*Checkpoint, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}
	var file checkpointFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
	}
	if crc32.ChecksumIEEE(file.Payload) != file.Checksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCheckpointCorrupt)
	}
	var cp Checkpoint
	if err := json.Unmarshal(file.Payload, &cp); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
	}
	if cp.Version != checkpointVersion {
		return nil, fmt.Errorf("%w: got %d want %d", ErrCheckpointVersion, cp.Version, checkpointVersion)
	}
	return &cp, nil
}
