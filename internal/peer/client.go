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
	maxRetries int // consecutive failures before returning ErrLeaderUnreachable (0 = unlimited)
}

// NewClient creates a Client that will connect to leaderAddr.
// maxRetries is the number of consecutive connection failures before Follow
// returns ErrLeaderUnreachable so the caller can attempt a leader election.
// Use 0 for unlimited retries (single-node / testing).
func NewClient(leaderAddr, nodeID string, maxRetries int) *Client {
	return &Client{leaderAddr: leaderAddr, nodeID: nodeID, maxRetries: maxRetries}
}

// Follow streams WAL entries from the leader starting at fromRev, calling
// applyFn for each entry. fromRev advances automatically as entries are applied.
//
// Follow reconnects on transient errors. It returns:
//   - ctx.Err() on context cancellation.
//   - ErrResyncRequired when the leader's buffer no longer covers fromRev.
//   - ErrLeaderUnreachable after maxRetries consecutive connection failures.
func (c *Client) Follow(ctx context.Context, fromRev int64, applyFn func(wal.Entry) error) error {
	consecutiveFailures := 0
	for {
		nextRev, err := c.followOnce(ctx, fromRev, applyFn)

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsResyncRequired(err) {
			logrus.Errorf("peer: leader requires resync from rev=%d: %v", fromRev, err)
			return err
		}

		// Progress resets the failure counter.
		if nextRev > fromRev {
			consecutiveFailures = 0
		} else {
			consecutiveFailures++
		}
		fromRev = nextRev

		if c.maxRetries > 0 && consecutiveFailures >= c.maxRetries {
			logrus.Errorf("peer: leader unreachable after %d attempts", consecutiveFailures)
			return ErrLeaderUnreachable
		}

		logrus.Warnf("peer: stream error (attempt %d): %v", consecutiveFailures, err)
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
