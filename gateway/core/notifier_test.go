/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/notification"
	"github.com/stretchr/testify/require"
)

// resolveCall records one (txID, committed) resolveInflight invocation the
// txNotifier made, so a test can assert both the verdict and the call count.
type resolveCall struct {
	txID      string
	committed bool
}

// newRecordingNotifier builds a txNotifier over a buffered subscribe channel
// (so Watch's register send never blocks the test), a fallback stub returning
// fallbackVerdict, and a resolve recorder delivered over a buffered channel.
func newRecordingNotifier(t *testing.T, timeout time.Duration, fallbackVerdict bool) (*txNotifier, chan []string, chan resolveCall) {
	t.Helper()
	subscribe := make(chan []string, 16)
	resolved := make(chan resolveCall, 16)
	fallback := func(txID string) bool { return fallbackVerdict }
	resolve := func(txID string, committed bool) { resolved <- resolveCall{txID, committed} }
	return newTxNotifier(subscribe, timeout, fallback, resolve), subscribe, resolved
}

// timerPresent reports whether the notifier still tracks a client-side timer
// for txID (i.e. the batch has not been resolved by Handle/onTimeout yet).
func timerPresent(n *txNotifier, txID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.timers[txID]
	return ok
}

// Test-matrix item 1: Watch then a COMMITTED event -> resolve(txID, true) and
// the client-side timer is stopped/removed.
func TestTxNotifierCommittedResolvesTrue(t *testing.T) {
	n, _, resolved := newRecordingNotifier(t, time.Hour, false)
	n.Watch("tx1")

	require.NoError(t, n.Handle(context.Background(), []notification.TxStatusEvent{
		{TxID: "tx1", Status: committerpb.Status_COMMITTED},
	}))

	select {
	case got := <-resolved:
		require.Equal(t, resolveCall{txID: "tx1", committed: true}, got)
	default:
		t.Fatal("expected resolve(tx1, true) to have been called synchronously by Handle")
	}
	require.False(t, timerPresent(n, "tx1"), "committed event must stop and delete the timer")
}

// Test-matrix item 2: Watch then an aborted event (a real abort status, not the
// STATUS_UNSPECIFIED sidecar-timeout sentinel) -> resolve(txID, false) WITHOUT
// consulting the fallback.
func TestTxNotifierAbortResolvesFalse(t *testing.T) {
	// fallbackVerdict=true so a wrong fallback consultation would flip the verdict.
	n, _, resolved := newRecordingNotifier(t, time.Hour, true)
	n.Watch("tx1")

	require.NoError(t, n.Handle(context.Background(), []notification.TxStatusEvent{
		{TxID: "tx1", Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))

	select {
	case got := <-resolved:
		require.Equal(t, resolveCall{txID: "tx1", committed: false}, got)
	default:
		t.Fatal("expected resolve(tx1, false) for an aborted event")
	}
	require.False(t, timerPresent(n, "tx1"))
}

// Test-matrix item 3: Watch then a STATUS_UNSPECIFIED (sidecar-timeout) event ->
// resolve(txID, fallback(txID)); test both fallback verdicts.
func TestTxNotifierTimeoutEventUsesFallback(t *testing.T) {
	for _, verdict := range []bool{true, false} {
		n, _, resolved := newRecordingNotifier(t, time.Hour, verdict)
		n.Watch("tx1")

		require.NoError(t, n.Handle(context.Background(), []notification.TxStatusEvent{
			{TxID: "tx1", Status: committerpb.Status_STATUS_UNSPECIFIED},
		}))

		select {
		case got := <-resolved:
			require.Equal(t, resolveCall{txID: "tx1", committed: verdict}, got,
				"STATUS_UNSPECIFIED must resolve with the fallback verdict")
		default:
			t.Fatalf("expected resolve(tx1, %v) via fallback", verdict)
		}
		require.False(t, timerPresent(n, "tx1"))
	}
}

// Test-matrix item 4: Watch then NO event within the timeout -> the client-side
// backstop timer fires -> resolve(txID, fallback(txID)).
func TestTxNotifierClientTimeoutUsesFallback(t *testing.T) {
	n, _, resolved := newRecordingNotifier(t, 20*time.Millisecond, true)
	n.Watch("tx1")

	select {
	case got := <-resolved:
		require.Equal(t, resolveCall{txID: "tx1", committed: true}, got)
	case <-time.After(2 * time.Second):
		t.Fatal("client-side backstop timer never resolved the batch")
	}
	require.False(t, timerPresent(n, "tx1"))
}

// Test-matrix item 5: register-before-submit ordering. Watch must push
// []string{txID} onto the subscribe channel before it returns, so the caller
// can submit knowing the subscription is queued.
func TestTxNotifierWatchRegistersBeforeReturn(t *testing.T) {
	subscribe := make(chan []string, 1)
	n := newTxNotifier(subscribe, time.Hour, func(string) bool { return false }, func(string, bool) {})
	n.Watch("tx1")

	select {
	case got := <-subscribe:
		require.Equal(t, []string{"tx1"}, got)
	default:
		t.Fatal("Watch must register the TxID on the subscribe channel before returning")
	}
}

// Test-matrix item 6: exactly-once under race. A STATUS_UNSPECIFIED event and
// the client-side timer fire concurrently; resolve must be called exactly once
// (and the whole thing must be clean under -race).
func TestTxNotifierExactlyOnceUnderRace(t *testing.T) {
	var count int32
	subscribe := make(chan []string, 1)
	resolvedOnce := make(chan struct{}, 1)
	resolve := func(string, bool) {
		atomic.AddInt32(&count, 1)
		select {
		case resolvedOnce <- struct{}{}:
		default:
		}
	}
	// Short timeout so onTimeout races the event handler.
	n := newTxNotifier(subscribe, 10*time.Millisecond, func(string) bool { return false }, resolve)
	n.Watch("tx1")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = n.Handle(context.Background(), []notification.TxStatusEvent{
			{TxID: "tx1", Status: committerpb.Status_STATUS_UNSPECIFIED},
		})
	}()
	wg.Wait()

	// Give the client-side timer time to fire too, so both paths have raced.
	<-resolvedOnce
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&count), "resolve must be called exactly once")
	require.False(t, timerPresent(n, "tx1"))
}

// TestQueryCommitStatus exercises the gateway's query-service fallback verdict:
// committed iff every written key's committed version reached its spec version.
func TestQueryCommitStatus(t *testing.T) {
	const txID = "batch-tx"
	specVers := map[string]uint64{"k1": 5, "k2": 8}

	newGatewayWithBatch := func() *Gateway {
		g := &Gateway{}
		g.inflight = append(g.inflight, &inflightBatch{txID: txID, specVers: specVers})
		return g
	}

	t.Run("all keys reach spec -> committed", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			return specVers[key], true, nil // exactly at spec
		}
		require.True(t, g.queryCommitStatus(txID))
	})

	t.Run("a key past spec (later writer committed) -> committed", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			return specVers[key] + 3, true, nil
		}
		require.True(t, g.queryCommitStatus(txID))
	})

	t.Run("a key below spec -> not committed", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			if key == "k2" {
				return specVers[key] - 1, true, nil
			}
			return specVers[key], true, nil
		}
		require.False(t, g.queryCommitStatus(txID))
	})

	t.Run("missing key -> not committed", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			if key == "k1" {
				return 0, false, nil // absent
			}
			return specVers[key], true, nil
		}
		require.False(t, g.queryCommitStatus(txID))
	})

	t.Run("reader error -> not committed", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			return 0, false, context.DeadlineExceeded
		}
		require.False(t, g.queryCommitStatus(txID))
	})

	t.Run("nil reader -> not committed (conservative)", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = nil
		require.False(t, g.queryCommitStatus(txID))
	})

	t.Run("unknown txID -> not committed", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			return specVers[key], true, nil
		}
		require.False(t, g.queryCommitStatus("no-such-tx"))
	})

	t.Run("empty specVers -> not committed (conservative, even if reader says committed)", func(t *testing.T) {
		g := &Gateway{}
		g.inflight = append(g.inflight, &inflightBatch{txID: txID, specVers: map[string]uint64{}})
		g.committedVersion = func(_ context.Context, key string) (uint64, bool, error) {
			return 0, true, nil // would say "committed" for any key, if the loop ran
		}
		require.False(t, g.queryCommitStatus(txID),
			"an empty spec set must not be treated as vacuously committed")
	})

	t.Run("query context is bounded by commitTimeout", func(t *testing.T) {
		g := newGatewayWithBatch()
		g.commitTimeout = time.Minute
		g.committedVersion = func(ctx context.Context, key string) (uint64, bool, error) {
			_, ok := ctx.Deadline()
			require.True(t, ok, "fallback query context must carry a deadline")
			return specVers[key], true, nil
		}
		require.True(t, g.queryCommitStatus(txID))
	})
}

// compile-time guard: txNotifier must satisfy notification.TxStatusHandler.
var _ notification.TxStatusHandler = (*txNotifier)(nil)
