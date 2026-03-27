package wal

import (
	"bytes"
	"io"
	"testing"
)

func TestEntryRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		e    Entry
	}{
		{
			name: "create",
			e: Entry{
				Revision: 1, Term: 1, Op: OpCreate,
				Key: "foo", Value: []byte("bar"), Lease: 0,
				CreateRevision: 1,
			},
		},
		{
			name: "update",
			e: Entry{
				Revision: 5, Term: 2, Op: OpUpdate,
				Key: "foo", Value: []byte("baz"), Lease: 42,
				CreateRevision: 1, PrevRevision: 3,
			},
		},
		{
			name: "delete",
			e: Entry{
				Revision: 6, Term: 2, Op: OpDelete,
				Key: "foo", CreateRevision: 1, PrevRevision: 5,
			},
		},
		{
			name: "compact",
			e: Entry{
				Revision: 7, Term: 2, Op: OpCompact,
				PrevRevision: 5,
			},
		},
		{
			name: "empty value",
			e: Entry{
				Revision: 1, Term: 1, Op: OpCreate,
				Key: "/a/b/c",
			},
		},
		{
			name: "large value",
			e: Entry{
				Revision: 100, Term: 1, Op: OpUpdate,
				Key:   "bigkey",
				Value: bytes.Repeat([]byte("x"), 64*1024),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := AppendEntry(&buf, &tc.e); err != nil {
				t.Fatalf("AppendEntry: %v", err)
			}

			got, err := ReadEntry(&buf)
			if err != nil {
				t.Fatalf("ReadEntry: %v", err)
			}

			assertEntryEqual(t, &tc.e, got)

			// Buffer should be fully consumed.
			if buf.Len() != 0 {
				t.Errorf("unconsumed bytes: %d", buf.Len())
			}
		})
	}
}

func TestReadEntryEOF(t *testing.T) {
	// Empty reader → clean EOF.
	e, err := ReadEntry(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("want io.EOF, got err=%v e=%v", err, e)
	}
}

func TestReadEntryTruncatedFrame(t *testing.T) {
	// Write a valid entry then truncate mid-payload → treated as clean EOF.
	var buf bytes.Buffer
	_ = AppendEntry(&buf, &Entry{Revision: 1, Op: OpCreate, Key: "k", Value: []byte("v")})
	truncated := buf.Bytes()[:buf.Len()-4] // chop last 4 bytes of payload

	e, err := ReadEntry(bytes.NewReader(truncated))
	if err != io.EOF {
		t.Fatalf("want io.EOF on truncation, got err=%v e=%v", err, e)
	}
}

func TestReadEntryCRCMismatch(t *testing.T) {
	var buf bytes.Buffer
	_ = AppendEntry(&buf, &Entry{Revision: 1, Op: OpCreate, Key: "k", Value: []byte("v")})
	b := buf.Bytes()
	// Corrupt the CRC bytes (bytes 4-7).
	b[4] ^= 0xff

	_, err := ReadEntry(bytes.NewReader(b))
	if err == nil {
		t.Fatal("expected CRC error, got nil")
	}
}

func assertEntryEqual(t *testing.T, want, got *Entry) {
	t.Helper()
	if got.Revision != want.Revision {
		t.Errorf("Revision: want %d got %d", want.Revision, got.Revision)
	}
	if got.Term != want.Term {
		t.Errorf("Term: want %d got %d", want.Term, got.Term)
	}
	if got.Op != want.Op {
		t.Errorf("Op: want %d got %d", want.Op, got.Op)
	}
	if got.Key != want.Key {
		t.Errorf("Key: want %q got %q", want.Key, got.Key)
	}
	if !bytes.Equal(got.Value, want.Value) {
		t.Errorf("Value: want %q got %q", want.Value, got.Value)
	}
	if got.Lease != want.Lease {
		t.Errorf("Lease: want %d got %d", want.Lease, got.Lease)
	}
	if got.CreateRevision != want.CreateRevision {
		t.Errorf("CreateRevision: want %d got %d", want.CreateRevision, got.CreateRevision)
	}
	if got.PrevRevision != want.PrevRevision {
		t.Errorf("PrevRevision: want %d got %d", want.PrevRevision, got.PrevRevision)
	}
}
