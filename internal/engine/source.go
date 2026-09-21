package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// FileSource 从本地 JSONL 文件按字节位点顺序读取事件。
// 位点即“下一条待读行的起始字节偏移”，可随检查点持久化并精确恢复。
type FileSource struct {
	path   string
	file   *os.File
	reader *bufio.Reader
	offset int64
}

func NewFileSource(path string) *FileSource { return &FileSource{path: path} }

// Next 读取下一条事件；到达文件末尾返回 ok=false。
// 读取成功时内部位点推进到下一行起始处。
func (s *FileSource) Next() (ev Event, ok bool, err error) {
	if s.reader == nil {
		if err := s.SeekOffset(s.offset); err != nil {
			return Event{}, false, err
		}
	}
	for {
		line, err := s.reader.ReadBytes('\n')
		if len(line) > 0 {
			s.offset += int64(len(line))
			trimmed := trimSpace(line)
			if len(trimmed) > 0 {
				if jerr := json.Unmarshal(trimmed, &ev); jerr != nil {
					return Event{}, false, fmt.Errorf("decode event at offset %d: %w", s.offset-int64(len(line)), jerr)
				}
				return ev, true, nil
			}
		}
		if errors.Is(err, io.EOF) {
			return Event{}, false, nil
		}
		if err != nil {
			return Event{}, false, err
		}
		// 空行，继续读下一行
	}
}

// Offset 返回当前读取位点（下一条待读行的字节偏移）。
func (s *FileSource) Offset() int64 { return s.offset }

// SeekOffset 恢复到指定位点，使后续 Next 从该处继续读取。
func (s *FileSource) SeekOffset(offset int64) error {
	if s.file != nil {
		s.file.Close()
		s.file = nil
		s.reader = nil
	}
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	s.file = f
	s.reader = bufio.NewReaderSize(f, 64*1024)
	s.offset = offset
	return nil
}

// Close 关闭底层文件。
func (s *FileSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func trimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\t' || b[start] == '\n' || b[start] == '\r') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\n' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}
