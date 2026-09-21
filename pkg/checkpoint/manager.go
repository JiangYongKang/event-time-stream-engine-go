package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// FormatVersion 是检查点格式版本，用于区分不兼容的旧格式。
const FormatVersion = 1

const (
	manifestName = "manifest.json"
	dataPrefix   = "data-"
)

// State 是一次检查点覆盖的全部可恢复状态。
type State struct {
	FormatVersion  int                 `json:"format_version"`
	ID             string              `json:"id"`
	CreatedAt      time.Time           `json:"created_at"`
	Watermark      time.Time           `json:"watermark"`
	PartitionTimes map[int]time.Time   `json:"partition_times"`
	Offsets        map[int]int64       `json:"offsets"`
	Windows        []WindowState       `json:"windows"`
	SeenIDs        map[string][]string `json:"seen_ids"`
	PendingResults []pendingResult     `json:"pending_results"`
	PendingLate    []pendingLate       `json:"pending_late"`
	AckedResultIDs []string            `json:"acked_result_ids"`
	AckedLateIDs   []string            `json:"acked_late_ids"`
}

// WindowState 是单个窗口的可恢复聚合状态。
type WindowState struct {
	WindowID string    `json:"window_id"`
	Key      string    `json:"key"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Count    int64     `json:"count"`
	Sum      int64     `json:"sum"`
}

// modelJSON 避免与 model 包形成导入循环：检查点包自行承载 JSON 结构。
type pendingResult struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Count     int64     `json:"count"`
	Sum       int64     `json:"sum"`
	EmittedAt time.Time `json:"emitted_at"`
}

type pendingLate struct {
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

type manifest struct {
	FormatVersion int       `json:"format_version"`
	ID            string    `json:"id"`
	DataFile      string    `json:"data_file"`
	DataChecksum  string    `json:"data_checksum"`
	CreatedAt     time.Time `json:"created_at"`
}

// ManifestInfo 是清单文件的可读视图（供测试/工具校验使用）。
type ManifestInfo = manifest

// ReadManifest 读取并解析清单文件。
func ReadManifest(dir string) (*ManifestInfo, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, err
	}
	var mf manifest
	if err := json.Unmarshal(b, &mf); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptManifest, err)
	}
	return &mf, nil
}

// Manager 负责检查点的原子持久化、校验与恢复。
type Manager struct {
	dir string
}

// NewManager 在 dir 目录下管理检查点。
func NewManager(dir string) *Manager { return &Manager{dir: dir} }

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func writeAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
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

// Save 将状态原子写入新检查点并使其成为最新检查点。
// 顺序：先落数据文件（含校验），再原子替换清单；崩溃只会留下
// “数据已在但清单未更新”的中间产物，恢复时可被明确识别。
func (m *Manager) Save(ctx context.Context, st *State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	st.FormatVersion = FormatVersion

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	dataFile := dataPrefix + st.ID + ".json"
	if err := writeAtomic(filepath.Join(m.dir, dataFile), data); err != nil {
		return fmt.Errorf("checkpoint: write data: %w", err)
	}

	mf := manifest{
		FormatVersion: FormatVersion,
		ID:            st.ID,
		DataFile:      dataFile,
		DataChecksum:  sha256Hex(data),
		CreatedAt:     st.CreatedAt,
	}
	mfBytes, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(m.dir, manifestName), mfBytes); err != nil {
		return err
	}

	// 清单成功后清理被取代的旧数据文件。
	entries, _ := os.ReadDir(m.dir)
	for _, e := range entries {
		name := e.Name()
		if len(name) > len(dataPrefix) && name[:len(dataPrefix)] == dataPrefix &&
			name != dataFile && name != dataFile+".tmp" {
			_ = os.Remove(filepath.Join(m.dir, name))
		}
	}
	return nil
}

// Load 读取并校验最新检查点，返回可区分的失败原因：
//   - ErrNoCheckpoint: 目录为空，全新启动
//   - ErrCorruptManifest / ErrCorruptData / ErrInconsistent / ErrDanglingCheckpoint
//   - ErrUnsupportedVersion: 旧格式/不兼容检查点
func (m *Manager) Load(ctx context.Context) (*State, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoCheckpoint
		}
		return nil, err
	}
	hasDataFile := false
	var dataFiles []string
	for _, e := range entries {
		name := e.Name()
		if len(name) > len(dataPrefix) && name[:len(dataPrefix)] == dataPrefix &&
			name[len(name)-len(".json"):] == ".json" {
			hasDataFile = true
			dataFiles = append(dataFiles, name)
		}
	}

	mfBytes, err := os.ReadFile(filepath.Join(m.dir, manifestName))
	if err != nil {
		if os.IsNotExist(err) {
			if hasDataFile {
				sort.Strings(dataFiles)
				return nil, fmt.Errorf("%w: %v", ErrDanglingCheckpoint, dataFiles)
			}
			return nil, ErrNoCheckpoint
		}
		return nil, err
	}
	var mf manifest
	if err := json.Unmarshal(mfBytes, &mf); err != nil || mf.ID == "" || mf.DataFile == "" || mf.DataChecksum == "" {
		return nil, fmt.Errorf("%w: %v", ErrCorruptManifest, err)
	}

	dataPath := filepath.Join(m.dir, mf.DataFile)
	data, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrInconsistent, mf.DataFile)
		}
		return nil, err
	}
	if sha256Hex(data) != mf.DataChecksum {
		return nil, fmt.Errorf("%w: %s", ErrCorruptData, mf.DataFile)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptData, err)
	}
	if st.FormatVersion != FormatVersion || mf.FormatVersion != FormatVersion {
		return nil, fmt.Errorf("%w: data=%d manifest=%d supported=%d",
			ErrUnsupportedVersion, st.FormatVersion, mf.FormatVersion, FormatVersion)
	}
	if st.ID != mf.ID {
		return nil, fmt.Errorf("%w: manifest id=%s data id=%s", ErrInconsistent, mf.ID, st.ID)
	}
	return &st, nil
}

// LatestID 返回最新检查点 ID（未加载过时返回空串）。
func (m *Manager) LatestID() string {
	mfBytes, err := os.ReadFile(filepath.Join(m.dir, manifestName))
	if err != nil {
		return ""
	}
	var mf manifest
	if err := json.Unmarshal(mfBytes, &mf); err != nil {
		return ""
	}
	return mf.ID
}

// IsNoCheckpoint 便于调用方区分“全新启动”。
func IsNoCheckpoint(err error) bool { return errors.Is(err, ErrNoCheckpoint) }
