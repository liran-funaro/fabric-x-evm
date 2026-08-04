/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/stretchr/testify/require"
)

// resetPhaseHooks clears the package-level metric hooks so one test's closures
// never leak into another. These tests must not run in parallel (they mutate
// shared package vars); none call t.Parallel.
func resetPhaseHooks() {
	RecordWarmPhaseDuration = nil
	RecordAuthPhaseDuration = nil
	RecordCommitLatency = nil
	RecordSpecAbort = nil
}

// TestExecutorRecordsAuthPhaseSerial: the serial executeCycle runs the combined
// authoritative ExecuteBatch, so RecordAuthPhaseDuration fires (once) and, since
// the serial path has no separate warm pass, RecordWarmPhaseDuration does NOT.
func TestExecutorRecordsAuthPhaseSerial(t *testing.T) {
	defer resetPhaseHooks()
	var warmCalls, authCalls atomic.Int64
	RecordWarmPhaseDuration = func(time.Duration) { warmCalls.Add(1) }
	RecordAuthPhaseDuration = func(d time.Duration) {
		require.GreaterOrEqual(t, d, time.Duration(0))
		authCalls.Add(1)
	}

	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	addThreeTxs(g)

	_, done := runCycleAndCapture(t, g)
	<-done

	require.GreaterOrEqual(t, authCalls.Load(), int64(1), "serial cycle should record an auth-phase duration")
	require.Equal(t, int64(0), warmCalls.Load(), "serial cycle has no separate warm pass")
}

// TestExecutorRecordsWarmAndAuthPhasePipelined: the pipelined path warms a batch
// (drainAndWarm -> WarmBatch) then authorizes it (pipelineIteration -> AuthBatch),
// so both RecordWarmPhaseDuration and RecordAuthPhaseDuration fire.
func TestExecutorRecordsWarmAndAuthPhasePipelined(t *testing.T) {
	defer resetPhaseHooks()
	var warmCalls, authCalls atomic.Int64
	RecordWarmPhaseDuration = func(d time.Duration) {
		require.GreaterOrEqual(t, d, time.Duration(0))
		warmCalls.Add(1)
	}
	RecordAuthPhaseDuration = func(d time.Duration) {
		require.GreaterOrEqual(t, d, time.Duration(0))
		authCalls.Add(1)
	}

	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	addThreeTxs(g)

	ctx := context.Background()
	warmed := g.drainAndWarm(ctx)
	require.NotNil(t, warmed, "drainAndWarm should establish the pipeline invariant")
	require.GreaterOrEqual(t, warmCalls.Load(), int64(1), "warm pass should record a duration")

	g.pipelineIteration(ctx, warmed)
	require.GreaterOrEqual(t, authCalls.Load(), int64(1), "auth pass should record a duration")
}

// TestExecutorRecordsCommitLatency: a batch that commits (a COMMITTED
// notification resolves the in-flight batch via HandleTx -> resolveInflight)
// fires RecordCommitLatency once with a non-negative submit->commit duration.
func TestExecutorRecordsCommitLatency(t *testing.T) {
	defer resetPhaseHooks()
	var commitCalls atomic.Int64
	RecordCommitLatency = func(d time.Duration) {
		require.GreaterOrEqual(t, d, time.Duration(0))
		commitCalls.Add(1)
	}

	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done
	require.Equal(t, int64(0), commitCalls.Load(), "no commit latency until the batch actually commits")

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	require.Equal(t, int64(1), commitCalls.Load(), "commit should record exactly one latency sample")
}

// TestExecutorPhaseHooksNilSafe: with all hooks unset (the production default),
// a full serial cycle runs without panicking.
func TestExecutorPhaseHooksNilSafe(t *testing.T) {
	resetPhaseHooks() // ensure nil regardless of prior test ordering

	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	addThreeTxs(g)

	_, done := runCycleAndCapture(t, g)
	<-done // completes without panic
}
