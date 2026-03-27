package peer

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/makhov/strata/internal/wal"
)

// Client is the follower-side WAL stream client.
type Client struct {
	leaderAddr string
	nodeID     string
}

// NewClient creates a Client that will connect to leaderAddr.
func NewClient(leaderAddr, nodeID string) *Client {
	return &Client{leaderAddr: leaderAddr, nodeID: nodeID}
}

// Follow streams WAL entries from the leader starting at fromRev, calling
// applyFn for each entry. fromRev advances automatically as entries are applied.
//
// Follow reconnects on transient errors and returns only when ctx is cancelled
// or a terminal error (resync required) is received.
func (c *Client) Follow(ctx context.Context, fromRev int64, applyFn func(wal.Entry) error) error {
	for {
		nextRev, err := c.followOnce(ctx, fromRev, applyFn)
		fromRev = nextRev

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsResyncRequired(err) {
			logrus.Errorf("peer: leader requires resync from rev=%d: %v", fromRev, err)
			return err // caller must re-bootstrap from S3
		}
		logrus.Warnf("peer: stream error (will retry): %v", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// followOnce makes one connection attempt and streams until error.
// Returns the last successfully applied revision + 1 as the next fromRev.
func (c *Client) followOnce(ctx context.Context, fromRev int64, applyFn func(wal.Entry) error) (int64, error) {
	conn, err := grpc.NewClient(
		c.leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(Codec{})),
	)
	if err != nil {
		return fromRev, err
	}
	defer conn.Close()

	wsc := NewWalStreamClient(conn)
	stream, err := wsc.Follow(ctx, &FollowRequest{
		FromRevision: fromRev,
		NodeID:       c.nodeID,
	})
	if err != nil {
		return fromRev, err
	}

	logrus.Infof("peer: connected to leader %s (fromRev=%d)", c.leaderAddr, fromRev)

	for {
		msg, err := stream.Recv()
		if err != nil {
			return fromRev, err
		}
		e := MsgToEntry(msg)
		if err := applyFn(e); err != nil {
			return fromRev, err
		}
		fromRev = e.Revision + 1
	}
}
