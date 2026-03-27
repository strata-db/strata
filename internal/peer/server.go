package peer

import (
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/makhov/strata/internal/wal"
)

// Server is the leader-side WAL streaming server.
//
// It maintains:
//   - A bounded ring buffer of recent entries for follower catch-up.
//   - A map of per-follower channels for live fan-out.
//
// Thread safety: Broadcast and Follow both hold mu.
type Server struct {
	mu        sync.Mutex
	buf       *entryBuffer
	followers map[string]chan *wal.Entry
}

// NewServer creates a Server with a ring buffer of capacity cap.
// cap=10_000 is a reasonable default for most workloads.
func NewServer(cap int) *Server {
	return &Server{
		buf:       newEntryBuffer(cap),
		followers: make(map[string]chan *wal.Entry),
	}
}

// Broadcast appends e to the buffer and fans it out to all connected followers.
// Called by the leader after every successful appendAndApply.
func (s *Server) Broadcast(e *wal.Entry) {
	s.mu.Lock()
	s.buf.push(e)
	for id, ch := range s.followers {
		select {
		case ch <- e:
		default:
			logrus.Warnf("peer: follower %q too slow — dropping entry rev=%d; it will reconnect", id, e.Revision)
		}
	}
	s.mu.Unlock()
}

// Follow implements WalStreamServer. It is called per follower connection.
func (s *Server) Follow(req *FollowRequest, stream WalStream_FollowServer) error {
	// Atomically snapshot the buffer and register the live channel.
	// Holding the lock here means Broadcast also blocks, so entries that arrive
	// during "snapshot + register" will be in the channel — no gap.
	s.mu.Lock()
	snapshot, ok := s.buf.since(req.FromRevision)
	if !ok {
		s.mu.Unlock()
		return ErrResyncRequired
	}
	ch := make(chan *wal.Entry, 512)
	s.followers[req.NodeID] = ch
	var maxSent int64
	if len(snapshot) > 0 {
		maxSent = snapshot[len(snapshot)-1].Revision
	} else {
		maxSent = req.FromRevision - 1
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.followers, req.NodeID)
		s.mu.Unlock()
	}()

	logrus.Infof("peer: follower %q connected (fromRev=%d, snapshot=%d entries)", req.NodeID, req.FromRevision, len(snapshot))

	// Send buffered catch-up entries.
	for _, e := range snapshot {
		if err := stream.Send(EntryToMsg(e)); err != nil {
			return err
		}
	}

	// Stream live entries; skip any that were already in the snapshot.
	for {
		select {
		case e := <-ch:
			if e.Revision <= maxSent {
				continue
			}
			maxSent = e.Revision
			if err := stream.Send(EntryToMsg(e)); err != nil {
				return err
			}
		case <-stream.Context().Done():
			logrus.Infof("peer: follower %q disconnected", req.NodeID)
			return stream.Context().Err()
		}
	}
}

// ── entry ring buffer ─────────────────────────────────────────────────────────

type entryBuffer struct {
	entries []*wal.Entry
	cap     int
}

func newEntryBuffer(cap int) *entryBuffer { return &entryBuffer{cap: cap} }

// push appends e, evicting the oldest entry when full.
// Must be called with Server.mu held.
func (b *entryBuffer) push(e *wal.Entry) {
	b.entries = append(b.entries, e)
	if len(b.entries) > b.cap {
		b.entries = b.entries[len(b.entries)-b.cap:]
	}
}

// since returns a snapshot of entries with Revision >= fromRev.
// Returns (nil, false) if fromRev predates the buffer window.
// Must be called with Server.mu held.
func (b *entryBuffer) since(fromRev int64) ([]*wal.Entry, bool) {
	if len(b.entries) == 0 {
		return nil, true // no entries yet; follower is up to date
	}
	minRev := b.entries[0].Revision
	if fromRev < minRev {
		return nil, false // too old; follower must resync from S3
	}
	for i, e := range b.entries {
		if e.Revision >= fromRev {
			out := make([]*wal.Entry, len(b.entries)-i)
			copy(out, b.entries[i:])
			return out, true
		}
	}
	return nil, true // all buffered entries are older than fromRev (already caught up)
}
