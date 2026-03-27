package strata

import (
	"os"
	"time"

	"github.com/makhov/strata/internal/object"
)

// Config holds all configuration for a Node.
type Config struct {
	// ── Storage ──────────────────────────────────────────────────────────────

	// DataDir is the directory used for local Pebble data and WAL segments.
	// Required.
	DataDir string

	// ObjectStore is used to archive WAL segments and checkpoints and to run
	// leader election. If nil the node runs in single-node mode.
	ObjectStore object.Store

	// SegmentMaxSize is the byte threshold that triggers WAL segment rotation.
	// Default: 50 MB.
	SegmentMaxSize int64

	// SegmentMaxAge is the time threshold that triggers WAL segment rotation.
	// Default: 10 s.
	SegmentMaxAge time.Duration

	// CheckpointInterval controls how often the leader writes a checkpoint.
	// Default: 15 minutes.
	CheckpointInterval time.Duration

	// CheckpointEntries triggers a checkpoint after this many WAL entries
	// regardless of time. 0 means disabled.
	CheckpointEntries int64

	// ── Multi-node (leader election + replication) ────────────────────────────
	//
	// Multi-node mode is enabled when PeerListenAddr is non-empty.
	// ObjectStore must also be configured (it hosts the leader lock).

	// NodeID is a stable, unique identifier for this node.
	// Defaults to the machine hostname.
	NodeID string

	// PeerListenAddr is the address on which the peer WAL-streaming gRPC
	// server listens (e.g. "0.0.0.0:2380"). Empty → single-node mode.
	PeerListenAddr string

	// AdvertisePeerAddr is the address followers use to reach this node's peer
	// server. Defaults to PeerListenAddr.
	AdvertisePeerAddr string

	// LockTTL is the leader lease duration. The leader must renew before it
	// expires or followers will elect a new leader.
	// Default: 30 s.
	LockTTL time.Duration

	// LockRenewInterval is how often the leader renews its lease.
	// Must be well below LockTTL. Default: 10 s.
	LockRenewInterval time.Duration

	// PeerBufferSize is the number of WAL entries the leader buffers for
	// follower catch-up. Default: 10 000.
	PeerBufferSize int
}

func (c *Config) setDefaults() {
	if c.SegmentMaxSize == 0 {
		c.SegmentMaxSize = 50 << 20
	}
	if c.SegmentMaxAge == 0 {
		c.SegmentMaxAge = 10 * time.Second
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 15 * time.Minute
	}
	if c.NodeID == "" {
		if h, err := os.Hostname(); err == nil {
			c.NodeID = h
		} else {
			c.NodeID = "node-0"
		}
	}
	if c.AdvertisePeerAddr == "" {
		c.AdvertisePeerAddr = c.PeerListenAddr
	}
	if c.LockTTL == 0 {
		c.LockTTL = 30 * time.Second
	}
	if c.LockRenewInterval == 0 {
		c.LockRenewInterval = 10 * time.Second
	}
	if c.PeerBufferSize == 0 {
		c.PeerBufferSize = 10_000
	}
}
