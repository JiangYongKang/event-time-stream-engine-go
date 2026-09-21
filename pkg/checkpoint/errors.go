package checkpoint

import "errors"

// 检查点恢复失败必须给出可区分的原因，禁止从中间状态静默继续。
var (
	ErrNoCheckpoint       = errors.New("checkpoint: no checkpoint found")
	ErrCorruptData        = errors.New("checkpoint: corrupt data file (checksum mismatch or unreadable)")
	ErrCorruptManifest    = errors.New("checkpoint: corrupt manifest (missing or checksum mismatch)")
	ErrUnsupportedVersion = errors.New("checkpoint: unsupported format version (stale/incompatible checkpoint)")
	ErrInconsistent       = errors.New("checkpoint: manifest references missing data file")
	ErrDanglingCheckpoint = errors.New("checkpoint: data file exists without valid manifest (incomplete checkpoint)")
)
