package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage persists a node's hard state and log. When a method returns, the data
// must survive a crash.
type Storage interface {
	// Load returns the saved hard state and every log entry, starting at index 1.
	Load() (HardState, []Entry, error)
	SaveHardState(HardState) error
	// Append replaces every entry from ents[0].Index onward with ents.
	Append(ents []Entry) error
}

// MemoryStorage keeps everything in memory. The simulator "crashes" a node by
// throwing the Node away and building a new one on the same MemoryStorage, which
// models a disk that kept every write that returned.
type MemoryStorage struct {
	mu   sync.Mutex
	hs   HardState
	ents []Entry
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Load() (HardState, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, append([]Entry(nil), s.ents...), nil
}

func (s *MemoryStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	return nil
}

func (s *MemoryStorage) Append(ents []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := ents[0].Index
	if first < 1 || first > uint64(len(s.ents))+1 {
		return fmt.Errorf("append at %d leaves a gap after %d", first, len(s.ents))
	}
	s.ents = append(s.ents[:first-1], ents...)
	return nil
}

// FileStorage keeps the hard state in state.json (replaced atomically) and the
// log in an append-only file. Each log record is one line:
//
//	<crc32 of the JSON, 8 hex digits> <entry as JSON>\n
//
// A crash in the middle of a write leaves a torn last record. On open the file is
// read up to the first record that is incomplete or fails its checksum and is
// truncated there. Losing that record is safe: its Append never returned, so the
// node never acknowledged it.
type FileStorage struct {
	mu      sync.Mutex
	dir     string
	f       *os.File
	hs      HardState
	ents    []Entry
	offsets []int64 // offsets[i] is where entry i+1 starts in the file
	size    int64
}

func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir}
	if b, err := os.ReadFile(filepath.Join(dir, "state.json")); err == nil {
		if err := json.Unmarshal(b, &s.hs); err != nil {
			return nil, fmt.Errorf("state.json: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	if err := s.readLog(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *FileStorage) readLog() error {
	r := bufio.NewReader(s.f)
	var off int64
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break // a final line without '\n' is torn and gets truncated below
		}
		if err != nil {
			return err
		}
		e, ok := decodeRecord(line)
		if !ok || e.Index != uint64(len(s.ents)+1) {
			break
		}
		s.ents = append(s.ents, e)
		s.offsets = append(s.offsets, off)
		off += int64(len(line))
	}
	s.size = off
	if err := s.f.Truncate(off); err != nil {
		return err
	}
	return s.f.Sync()
}

func decodeRecord(line []byte) (Entry, bool) {
	var e Entry
	line = bytes.TrimSuffix(line, []byte("\n"))
	if len(line) < 10 || line[8] != ' ' {
		return e, false
	}
	var sum uint32
	if _, err := fmt.Sscanf(string(line[:8]), "%08x", &sum); err != nil {
		return e, false
	}
	body := line[9:]
	if crc32.ChecksumIEEE(body) != sum || json.Unmarshal(body, &e) != nil {
		return e, false
	}
	return e, true
}

func (s *FileStorage) Load() (HardState, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, append([]Entry(nil), s.ents...), nil
}

func (s *FileStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(hs)
	tmp := filepath.Join(s.dir, "state.json.tmp")
	if err := writeFileSync(tmp, b); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "state.json")); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	s.hs = hs
	return nil
}

func (s *FileStorage) Append(ents []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := ents[0].Index
	if first < 1 || first > uint64(len(s.ents))+1 {
		return fmt.Errorf("append at %d leaves a gap after %d", first, len(s.ents))
	}
	if first <= uint64(len(s.ents)) {
		// A new leader overwrote a suffix: cut the file back to where it starts.
		s.size = s.offsets[first-1]
		if err := s.f.Truncate(s.size); err != nil {
			return err
		}
		s.ents, s.offsets = s.ents[:first-1], s.offsets[:first-1]
	}
	var buf bytes.Buffer
	offs := make([]int64, len(ents))
	for i, e := range ents {
		offs[i] = s.size + int64(buf.Len())
		body, _ := json.Marshal(e)
		fmt.Fprintf(&buf, "%08x %s\n", crc32.ChecksumIEEE(body), body)
	}
	if _, err := s.f.WriteAt(buf.Bytes(), s.size); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.size += int64(buf.Len())
	s.ents = append(s.ents, ents...)
	s.offsets = append(s.offsets, offs...)
	return nil
}

func (s *FileStorage) Close() error { return s.f.Close() }

func writeFileSync(path string, b []byte) error {
	f, err := os.Create(path)
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
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
