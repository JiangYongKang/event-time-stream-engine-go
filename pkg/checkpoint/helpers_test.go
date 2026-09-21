package checkpoint

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// writeFutureVersion 落盘一个 FormatVersion 不兼容的检查点及其清单。
func writeFutureVersion(dir string, st *State) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	dataFile := dataPrefix + st.ID + ".json"
	if err := os.WriteFile(filepath.Join(dir, dataFile), data, 0o644); err != nil {
		return err
	}
	mf := manifest{
		FormatVersion: st.FormatVersion, ID: st.ID, DataFile: dataFile,
		DataChecksum: sha256Hex(data), CreatedAt: st.CreatedAt,
	}
	mfb, err := json.Marshal(mf)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, manifestName), mfb, 0o644)
}

var _ = context.Background
