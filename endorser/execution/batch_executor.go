/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// ExecuteBatch runs an ordered batch of transactions under a single view.
//
//   - len(txs)==1: one pass, sharing runOn/newState with Execute so the two are
//     identical.
//   - len(txs)>1: a warm pass runs each tx concurrently against the shared
//     snapshot to fill any caching reader's read cache (results discarded), then
//     an authoritative pass re-runs them in order against an overlay of that
//     snapshot, applying each tx's write-set before running the next so a later
//     tx observes earlier writes.
func (e *EVMEngine) ExecuteBatch(ctx context.Context, txs []*types.Transaction) ([]endorsement.ExecutionResult, error) {
	if len(txs) == 0 {
		return nil, nil
	}

	// One snapshot (one view) for the whole batch: every tx, in both passes,
	// simulates against the same consistent point-in-time state.
	reader, err := e.kvs.NewSnapshot(0)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if len(txs) == 1 {
		state, err := e.newState(reader)
		if err != nil {
			return nil, err
		}
		res, err := e.runOn(state, txs[0])
		if err != nil {
			return nil, err
		}
		return []endorsement.ExecutionResult{res}, nil
	}

	// Warm pass: run every tx concurrently against the shared snapshot. Each tx
	// gets its own state so concurrent execution can't corrupt a shared journal.
	// Results and errors are intentionally discarded (`_, _ =`): this pass exists
	// only to warm a caching reader (e.g. the query-service view backing
	// `reader`) before the sequential pass below, which is what surfaces real
	// results and errors. A tx that would fail here (e.g. against pre-batch
	// state) is not a bug — the authoritative pass is what runs for real.
	var wg sync.WaitGroup
	for _, tx := range txs {
		wg.Add(1)
		go func(tx *types.Transaction) {
			defer wg.Done()
			// The StateDB accessors panic when the underlying store returns a
			// read error (as opposed to the clean absent-key (nil, nil) case),
			// and PrepareMessage reads the sender's nonce before anything else.
			// A transient reader error in this best-effort warm pass must not
			// crash the endorser: recover and leave the key un-warmed. The
			// authoritative pass re-runs the tx and surfaces any real error.
			defer func() { _ = recover() }()
			if s, err := e.newState(reader); err == nil {
				_, _ = e.runOn(s, tx) // warm only; ignore result/error.
			}
		}(tx)
	}
	wg.Wait()

	// Authoritative pass: sequential, each tx against the snapshot plus an
	// overlay carrying every earlier tx's writes from this batch.
	overlay := &overlayReader{under: reader, writes: map[string]*blocks.WriteRecord{}}
	out := make([]endorsement.ExecutionResult, 0, len(txs))
	for _, tx := range txs {
		state, err := e.newState(overlay)
		if err != nil {
			return nil, err
		}
		res, err := e.runOn(state, tx)
		if err != nil {
			return nil, err
		}
		overlay.apply(res.RWS)
		out = append(out, res)
	}
	return out, nil
}

// newState builds an ExtendedStateDB over reader, wrapping it with StateDBLogger
// when debug logging is enabled. It mirrors the StateDB construction inside
// newExecutor minus the snapshot lifecycle: the caller owns reader (it may be a
// fresh per-tx snapshot, as in newExecutor/Execute, or a shared batch reader).
func (e *EVMEngine) newState(reader ReadStore) (ExtendedStateDB, error) {
	stateDB, err := NewStateDB(context.TODO(), reader, e.namespace, 0, e.monotonicVersions)
	if err != nil {
		return nil, err
	}
	if e.evmConfig.DebugLogs {
		return NewStateDBLogger(stateDB), nil
	}
	return stateDB, nil
}

// overlayReader layers an in-memory, batch-local write set over an underlying
// snapshot ReadStore, so the authoritative pass sees earlier transactions'
// writes in this batch as if they were already committed.
type overlayReader struct {
	under  ReadStore
	mu     sync.Mutex
	writes map[string]*blocks.WriteRecord
}

// Get returns the value visible to the authoritative pass for (ns, key): the
// latest in-batch write if one exists, else the underlying snapshot's value.
//
// The record's version metadata always comes from the underlying snapshot,
// never fabricated from the in-batch write (a blocks.KVWrite carries no version
// at all). The whole batch is validated for MVCC at commit time against the
// ledger state this snapshot was taken from, so a later tx's read dependency on
// an in-batch write must still be checked against that same pre-batch version —
// not a synthetic intra-batch one — or MVCC read-versions would stop being
// view-consistent.
//
// A write to a key that is absent from the underlying snapshot (under == nil)
// is reported back with IsDelete forced true, so the read is recorded with a
// nil MVCC version (i.e. "absent"), while Value still carries the in-batch
// write. This reuses the existing convention StateDB relies on for
// snapshot-revert: see getStateFromStore's "IsDelete: true with non-nil Value"
// case in statedb.go.
func (o *overlayReader) Get(ns, key string) (*blocks.WriteRecord, error) {
	under, err := o.under.Get(ns, key)
	if err != nil {
		return nil, err
	}

	o.mu.Lock()
	rec, overlaid := o.writes[key]
	o.mu.Unlock()
	if !overlaid {
		return under, nil
	}

	result := &blocks.WriteRecord{Value: rec.Value, IsDelete: rec.IsDelete || under == nil}
	if under != nil {
		result.BlockNum = under.BlockNum
		result.TxNum = under.TxNum
		result.Version = under.Version
	}
	return result, nil
}

// Close is a no-op: the underlying snapshot's lifecycle is owned by the caller
// of ExecuteBatch (via the `reader` it wraps), not by the overlay itself.
func (o *overlayReader) Close() error { return nil }

// apply records tx's write-set so later Get calls in this batch observe it.
func (o *overlayReader) apply(rws blocks.ReadWriteSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, w := range rws.Writes {
		o.writes[w.Key] = &blocks.WriteRecord{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete}
	}
}

var _ ReadStore = (*overlayReader)(nil)
