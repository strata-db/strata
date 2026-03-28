package wal

import (
	"io"
	"math"
	"testing"
)

func TestSegmentWriterReader(t *testing.T) {
	dir := t.TempDir()
	const term uint64 = 3
	const firstRev int64 = 10

	entries := []*Entry{
		{Revision: 10, Term: term, Op: OpCreate, Key: "a", Value: []byte("1")},
		{Revision: 11, Term: term, Op: OpUpdate, Key: "a", Value: []byte("2"), CreateRevision: 10, PrevRevision: 10},
		{Revision: 12, Term: term, Op: OpDelete, Key: "a", CreateRevision: 10, PrevRevision: 11},
	}

	// Write.
	sw, err := OpenSegmentWriter(dir, term, firstRev)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	for _, e := range entries {
		if err := sw.Append(e); err != nil {
			t.Fatalf("Append rev=%d: %v", e.Revision, err)
		}
	}
	if sw.EntryCount() != len(entries) {
		t.Errorf("EntryCount: want %d got %d", len(entries), sw.EntryCount())
	}
	if sw.Term() != term {
		t.Errorf("Term: want %d got %d", term, sw.Term())
	}
	if sw.FirstRev() != firstRev {
		t.Errorf("FirstRev: want %d got %d", firstRev, sw.FirstRev())
	}
	if err := sw.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Read back.
	sr, closer, err := OpenSegmentFile(sw.Path())
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer closer()

	if sr.Term != term {
		t.Errorf("Reader.Term: want %d got %d", term, sr.Term)
	}
	if sr.FirstRev != firstRev {
		t.Errorf("Reader.FirstRev: want %d got %d", firstRev, sr.FirstRev)
	}

	got, err := sr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("ReadAll count: want %d got %d", len(entries), len(got))
	}
	for i, e := range entries {
		assertEntryEqual(t, e, got[i])
	}

	// Next after EOF.
	e, err := sr.Next()
	if err != io.EOF {
		t.Errorf("Next after end: want io.EOF, got err=%v e=%v", err, e)
	}
}

func TestSegmentName(t *testing.T) {
	name := SegmentName(1, 42)
	want := "0000000001-00000000000000000042.wal"
	if name != want {
		t.Errorf("SegmentName: want %q got %q", want, name)
	}
}

func TestParseSegmentName(t *testing.T) {
	// Round-trip: ParseSegmentName inverts SegmentName.
	cases := []struct {
		term     uint64
		firstRev int64
	}{
		{1, 1},
		{7, 42},
		{0, 0},
		{math.MaxUint32, math.MaxInt32},
	}
	for _, tc := range cases {
		name := SegmentName(tc.term, tc.firstRev)
		gotTerm, gotRev, ok := ParseSegmentName(name)
		if !ok {
			t.Errorf("ParseSegmentName(%q): ok=false", name)
			continue
		}
		if gotTerm != tc.term || gotRev != tc.firstRev {
			t.Errorf("ParseSegmentName(%q): want (%d, %d) got (%d, %d)",
				name, tc.term, tc.firstRev, gotTerm, gotRev)
		}
	}

	// Works without the .wal suffix.
	term, rev, ok := ParseSegmentName("0000000003-00000000000000000010")
	if !ok || term != 3 || rev != 10 {
		t.Errorf("without suffix: want (3,10,true) got (%d,%d,%v)", term, rev, ok)
	}

	// Invalid inputs return ok=false.
	invalid := []string{"", "notawalfile.wal", "abc-def.wal", "no-dash"}
	for _, s := range invalid {
		_, _, ok := ParseSegmentName(s)
		if ok {
			t.Errorf("ParseSegmentName(%q): want ok=false", s)
		}
	}
}

func TestLocalSegments(t *testing.T) {
	dir := t.TempDir()

	// Create three segments in reverse order to verify sort.
	for _, firstRev := range []int64{100, 1, 50} {
		sw, err := OpenSegmentWriter(dir, 1, firstRev)
		if err != nil {
			t.Fatal(err)
		}
		_ = sw.Append(&Entry{Revision: firstRev, Op: OpCreate, Key: "x"})
		_ = sw.Seal()
	}

	paths, err := LocalSegments(dir)
	if err != nil {
		t.Fatalf("LocalSegments: %v", err)
	}
	if len(paths) != 3 {
		t.Fatalf("want 3 paths, got %d", len(paths))
	}
	// Lexicographic == chronological given zero-padded names.
	for i := 1; i < len(paths); i++ {
		if paths[i] <= paths[i-1] {
			t.Errorf("segments not sorted: %q <= %q", paths[i], paths[i-1])
		}
	}
}
