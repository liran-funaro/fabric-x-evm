package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/stretchr/testify/require"
)

// recordingSubmitter is a stub Submitter that records the endorsements it was
// asked to submit. It never errors.
type recordingSubmitter struct {
	mu        sync.Mutex
	submitted []sdk.Endorsement
}

func (s *recordingSubmitter) Submit(_ context.Context, end sdk.Endorsement) error {
	s.mu.Lock()
	s.submitted = append(s.submitted, end)
	s.mu.Unlock()
	return nil
}

func (s *recordingSubmitter) Close() error { return nil }

func (s *recordingSubmitter) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.submitted)
}

// endorsementWithTxID builds an sdk.Endorsement whose proposal carries txID in
// its channel header, matching what committerTxID recovers (see executor.go).
func endorsementWithTxID(t *testing.T, txID string) sdk.Endorsement {
	t.Helper()
	chdr := &common.ChannelHeader{Type: int32(common.HeaderType_MESSAGE), TxId: txID, ChannelId: "ch"}
	hdr := &common.Header{ChannelHeader: protoutil.MarshalOrPanic(chdr)}
	prop := &peer.Proposal{Header: protoutil.MarshalOrPanic(hdr)}
	return sdk.Endorsement{Proposal: prop}
}

// With the gate armed, submitOne unblocks promptly once the tx's TxID is
// observed in an ordered block.
func TestBatchSubmitter_GateUnblocksOnObserve(t *testing.T) {
	stub := &recordingSubmitter{}
	bs := NewBatchSubmitter([]Submitter{stub}, make(chan sdk.Endorsement), 1, 0)
	gate := NewOrderGate()
	bs.SetOrderGate(gate, 2*time.Second)
	require.NotNil(t, bs.orderGate, "gate should be installed for numWorkers==1")

	end := endorsementWithTxID(t, "tx-observe")
	go func() {
		// Give submitOne time to broadcast + arm, then observe.
		time.Sleep(20 * time.Millisecond)
		gate.Observe([]string{"tx-observe"})
	}()

	start := time.Now()
	require.NoError(t, bs.submitOne(context.Background(), 0, end))
	elapsed := time.Since(start)

	require.Equal(t, 1, stub.count(), "tx must be broadcast")
	require.Less(t, elapsed, time.Second, "should unblock on observe well before timeout")
}

// With the gate armed but the TxID never observed, submitOne proceeds after the
// wait timeout (does not hang) and still counts the broadcast.
func TestBatchSubmitter_GateProceedsOnTimeout(t *testing.T) {
	stub := &recordingSubmitter{}
	bs := NewBatchSubmitter([]Submitter{stub}, make(chan sdk.Endorsement), 1, 0)
	bs.SetOrderGate(NewOrderGate(), 200*time.Millisecond)

	end := endorsementWithTxID(t, "tx-timeout")
	start := time.Now()
	require.NoError(t, bs.submitOne(context.Background(), 0, end))
	elapsed := time.Since(start)

	require.Equal(t, 1, stub.count(), "tx must be broadcast even when never observed")
	require.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "should wait ~timeout before proceeding")
	require.Less(t, elapsed, time.Second, "should not wait much beyond the timeout")
}

// SetOrderGate is a no-op when numWorkers != 1 (single-armed invariant); the gate
// stays nil and submitOne returns immediately without arming.
func TestBatchSubmitter_GateIgnoredWhenMultiWorker(t *testing.T) {
	stub0, stub1 := &recordingSubmitter{}, &recordingSubmitter{}
	bs := NewBatchSubmitter([]Submitter{stub0, stub1}, make(chan sdk.Endorsement), 2, 0)
	bs.SetOrderGate(NewOrderGate(), time.Second)
	require.Nil(t, bs.orderGate, "gate must be ignored when numWorkers != 1")

	end := endorsementWithTxID(t, "tx-nogate")
	start := time.Now()
	require.NoError(t, bs.submitOne(context.Background(), 0, end))
	require.Less(t, time.Since(start), 100*time.Millisecond, "no gating -> immediate return")
	require.Equal(t, 1, stub0.count())
}
