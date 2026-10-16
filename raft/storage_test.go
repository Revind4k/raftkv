package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func ents(from, to, term uint64) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	must(t, s.SaveHardState(HardState{Term: 3, Vote: 2}))
	must(t, s.Append(ents(1, 5, 1)))
	must(t, s.Append(ents(4, 6, 2))) // a new leader overwrites 4 and 5
	s.Close()

	s, err = OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs, got, _ := s.Load()
	want := append(ents(1, 3, 1), ents(4, 6, 2)...)
	if hs != (HardState{Term: 3, Vote: 2}) || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v %+v", hs, got)
	}
	// Appending after a reopen continues at the right offset.
	must(t, s.Append(ents(7, 7, 2)))
	s.Close()
	s, _ = OpenFileStorage(dir)
	if _, got, _ = s.Load(); len(got) != 7 {
		t.Fatalf("len %d after reopen and append", len(got))
	}
}

// A crash during a write leaves a partial last record. Reopening drops it and
// keeps every complete record.
func TestFileStorageTornWrite(t *testing.T) {
	for name, tail := range map[string]string{
		"partial line":   `0badc0de {"index":4,"te`,
		"bad checksum":   "00000000 {\"index\":4,\"term\":1}\n",
		"garbage":        "\x00\x00\x00\n",
		"skipped index":  "", // filled below
		"trailing bytes": "abc",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, _ := OpenFileStorage(dir)
			must(t, s.Append(ents(1, 3, 1)))
			s.Close()
			f, _ := os.OpenFile(filepath.Join(dir, "log"), os.O_APPEND|os.O_WRONLY, 0)
			if name == "skipped index" {
				// A valid record for index 1 again: the index does not follow on.
				s2, _ := OpenFileStorage(t.TempDir())
				must(t, s2.Append(ents(1, 1, 1)))
				b, _ := os.ReadFile(filepath.Join(s2.dir, "log"))
				tail = string(b)
			}
			f.WriteString(tail)
			f.Close()

			s, err := OpenFileStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, got, _ := s.Load(); !reflect.DeepEqual(got, ents(1, 3, 1)) {
				t.Fatalf("got %+v", got)
			}
			must(t, s.Append(ents(4, 4, 1)))
			s.Close()
			s, _ = OpenFileStorage(dir)
			if _, got, _ := s.Load(); len(got) != 4 {
				t.Fatalf("append after recovery: len %d", len(got))
			}
		})
	}
}

func TestStorageRejectsGap(t *testing.T) {
	for _, s := range []Storage{NewMemoryStorage(), mustOpen(t)} {
		if err := s.Append(ents(3, 3, 1)); err == nil {
			t.Fatalf("%T accepted an append that leaves a gap", s)
		}
	}
}

func mustOpen(t *testing.T) *FileStorage {
	s, err := OpenFileStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
