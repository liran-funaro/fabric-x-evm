<!--
SPDX-License-Identifier: Apache-2.0 AND LGPL-3.0-or-later
-->

# Two-Phase Batch Execution — Stage 2.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the gateway's fixed worker pool + dependency-aware queue with a single drain-all executor loop that two-phase-executes all pending transactions and commits each batch as one atomic Fabric transaction (wait-for-commit between batches). No global overlay yet (that is stage 2.3).

**Architecture:** A `PendingPool` replaces `TxQueue`/`TxQueueV2`. A single drain-all executor loop (replacing the worker pool) each cycle drains the pool, asks the endorser to two-phase-execute the batch and return one signed **merged** read/write-set, submits it as one Fabric tx, waits for it to commit, then repeats. A new `ProposalTypeEVMBatch` envelope carries the batch's N EVM txs; the block parser learns to extract them as `(block, index, sub-index)` with per-tx receipts.

**Tech Stack:** Go 1.26; go-ethereum EVM; `github.com/hyperledger/fabric-x-sdk` (`endorsement.{Invocation,ExecutionResult,Builder}`, `blocks.{ReadWriteSet,Block,Transaction}`); existing `endorser/execution.BatchExecutor` (slice 1).

## Global Constraints

- License header on every new Go file (copy verbatim):
  ```go
  /*
  Copyright IBM Corp. All Rights Reserved.

  SPDX-License-Identifier: LGPL-3.0-or-later
  */
  ```
- CFT only: one trusted gateway owns the order; one signer meets the endorsement policy. No global counter, no BFT envelope.
- Fabric-X MVCC: reads carry per-key `WriteRecord.Version` (uint64); merged read-set = union of reads at view-consistent versions; merged write-set = last-write-wins per key.
- Stage 2.1 sequences batches with **wait-for-commit**: batch N+1 is drained only after batch N commits (batch N+1 reads committed state via the query service). No in-memory overlay, no version prediction (those are stage 2.3).
- A batch commits **atomically** as one Fabric tx; its EVM txs are addressable `(block, index, sub-index)` and each keeps its own hash + receipt.
- Run Go tests: `go test ./<pkg>/... -run <Name> -count=1 -v`. Build: `make build`.
- Do not change the endorser's query-service read path (slice 1) or the EVM state key layout.

## File Structure

**New:**
- `gateway/core/pending.go` — `PendingPool`: the set of accepted-but-not-committed txs (add / drain-all / remove). Replaces `TxQueue`/`TxQueueV2`.
- `gateway/core/executor.go` — the drain-all executor loop (replaces `Gateway.Start`/`worker`).
- `endorser/execution/merge.go` — `MergeResults([]ExecutionResult) (blocks.ReadWriteSet, [][]byte)`: merge per-tx RWS into one (union reads / last-write-wins) and return per-tx event blobs (for receipts).

**Modified:**
- `common/proposal.go` — add `ProposalTypeEVMBatch`.
- `endorser/api/service.go` (the `Service` interface file) — add `ExecuteBatch`.
- `endorser/core/endorser.go` — implement `ExecuteBatch`: run `Engine.ExecuteBatch`, merge, sign once.
- `gateway/core/endorse.go` — add `EndorsementClient.ExecuteBatch` (build the batch invocation, collect endorsements, assemble one `sdk.Endorsement`).
- `gateway/core/api.go` — `Gateway` struct + `New` (drop `workerCount`/`TxQueue`, add `PendingPool` + executor); `SendTransaction` adds to the pool; remove `Start`/`worker`/`processTx`.
- `gateway/core/chain.go` — `ConvertToDomain`: parse `ProposalTypeEVMBatch` into N domain txs with sub-index + per-tx status/receipts.
- `gateway/domain/models.go` — add `SubIndex` to `domain.Transaction`.
- `gateway/app/app.go` + `gateway/app/wiring.go` — construct the `PendingPool` + executor; drop `WorkerCount` plumbing.
- `gateway/config` — remove/deprecate `Gateway.WorkerCount`.

**Deleted:**
- `gateway/core/txqueue_v2.go` (+ `_test.go`), `gateway/core/txqueue.go` (+ `_test.go`), `gateway/core/txqueue_helpers.go` — the dependency-aware queue and worker-pool queue.

---

## Task 1: `ProposalTypeEVMBatch` envelope type

**Files:**
- Modify: `common/proposal.go`
- Test: `common/proposal_test.go` (create if absent)

**Interfaces:**
- Produces: `common.ProposalTypeEVMBatch common.ProposalType` — the envelope byte marking a merged EVM batch tx.

- [ ] **Step 1: Read the current type block.** Open `common/proposal.go`; confirm `ProposalTypeEVMTx ProposalType = 0xfb`, `ProposalTypeCall`, `ProposalTypeState` (iota-style). Add the new constant after `ProposalTypeState` so existing values are unchanged.

- [ ] **Step 2: Write the failing test** — `common/proposal_test.go`

```go
package common_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-evm/common"
)

func TestProposalTypeEVMBatchDistinct(t *testing.T) {
	got := map[common.ProposalType]string{
		common.ProposalTypeEVMTx:    "evmtx",
		common.ProposalTypeCall:     "call",
		common.ProposalTypeState:    "state",
		common.ProposalTypeEVMBatch: "batch",
	}
	if len(got) != 4 {
		t.Fatalf("proposal types collide: %v", got)
	}
	if common.ProposalTypeEVMBatch == common.ProposalTypeEVMTx {
		t.Fatal("batch type must differ from evmtx")
	}
}
```

- [ ] **Step 3: Run test to verify it fails** — `go test ./common/... -run TestProposalTypeEVMBatchDistinct -count=1 -v` → FAIL (`ProposalTypeEVMBatch` undefined).

- [ ] **Step 4: Add the constant** in `common/proposal.go` after `ProposalTypeState`:

```go
	// ProposalTypeEVMBatch marks a Fabric tx that carries a merged batch of EVM
	// transactions (Args[0]=type, Args[1..N]=the EVM txs), committed atomically.
	ProposalTypeEVMBatch
```

- [ ] **Step 5: Run test** — `go test ./common/... -run TestProposalTypeEVMBatchDistinct -count=1 -v` → PASS.

- [ ] **Step 6: Commit** — `git add common/proposal.go common/proposal_test.go && git commit -m "feat(common): add ProposalTypeEVMBatch envelope type"`

---

## Task 2: merge per-tx execution results into one RWS

**Files:**
- Create: `endorser/execution/merge.go`
- Test: `endorser/execution/merge_test.go`

**Interfaces:**
- Consumes: `endorsement.ExecutionResult` (`fabric-x-sdk/endorsement`, fields `RWS blocks.ReadWriteSet`, `Event []byte`, `Status int32`, `Payload []byte`); `blocks.{ReadWriteSet,KVRead,KVWrite}` (`RWS.Reads []KVRead{Key string, Version *Version}`, `RWS.Writes []KVWrite{Key string, IsDelete bool, Value []byte}`).
- Produces: `func MergeResults(results []endorsement.ExecutionResult) (blocks.ReadWriteSet, [][]byte)` — one merged RWS (reads: union keyed by Key keeping the first-seen version, which is the view-consistent committed version; writes: last-write-wins per Key in order) and a parallel slice of per-tx `Event` blobs (index = sub-index) for receipts.

- [ ] **Step 1: Write the failing test** — `endorser/execution/merge_test.go`

```go
package execution

import (
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

func TestMergeResultsUnionAndLastWrite(t *testing.T) {
	v := &blocks.Version{BlockNum: 7}
	r1 := endorsement.ExecutionResult{
		RWS:   blocks.ReadWriteSet{Reads: []blocks.KVRead{{Key: "a", Version: v}}, Writes: []blocks.KVWrite{{Key: "a", Value: []byte("1")}}},
		Event: []byte("e1"),
	}
	r2 := endorsement.ExecutionResult{
		RWS:   blocks.ReadWriteSet{Reads: []blocks.KVRead{{Key: "a", Version: v}, {Key: "b", Version: nil}}, Writes: []blocks.KVWrite{{Key: "a", Value: []byte("2")}, {Key: "b", Value: []byte("9")}}},
		Event: []byte("e2"),
	}

	rws, events := MergeResults([]endorsement.ExecutionResult{r1, r2})

	// Reads: union by key -> {a,b}
	readKeys := map[string]bool{}
	for _, rd := range rws.Reads {
		readKeys[rd.Key] = true
	}
	if !readKeys["a"] || !readKeys["b"] || len(rws.Reads) != 2 {
		t.Fatalf("reads = %+v, want union {a,b}", rws.Reads)
	}
	// Writes: last-write-wins -> a="2", b="9"
	got := map[string]string{}
	for _, w := range rws.Writes {
		got[w.Key] = string(w.Value)
	}
	if got["a"] != "2" || got["b"] != "9" || len(rws.Writes) != 2 {
		t.Fatalf("writes = %+v, want a=2 b=9", rws.Writes)
	}
	// Per-tx events preserved by sub-index.
	if len(events) != 2 || string(events[0]) != "e1" || string(events[1]) != "e2" {
		t.Fatalf("events = %v, want [e1 e2]", events)
	}
}
```

- [ ] **Step 2: Run test to verify it fails** — `go test ./endorser/execution/... -run TestMergeResultsUnionAndLastWrite -count=1 -v` → FAIL (`MergeResults` undefined).

- [ ] **Step 3: Write `endorser/execution/merge.go`**

```go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// MergeResults folds an ordered batch of per-tx execution results into one
// read/write-set plus the per-tx event blobs (index = sub-index).
//
// Reads are unioned by key, keeping the first-seen version: because the whole
// batch executes under one query-service view (+ overlay in later stages), the
// first read of a key already carries the view-consistent committed version, so
// every tx that read it depends on the same version. Writes are applied in
// batch order with last-write-wins, matching the serial authoritative pass.
func MergeResults(results []endorsement.ExecutionResult) (blocks.ReadWriteSet, [][]byte) {
	var merged blocks.ReadWriteSet
	readIdx := make(map[string]int)  // key -> index in merged.Reads
	writeIdx := make(map[string]int) // key -> index in merged.Writes
	events := make([][]byte, len(results))

	for i, res := range results {
		events[i] = res.Event
		for _, rd := range res.RWS.Reads {
			if _, ok := readIdx[rd.Key]; ok {
				continue // union: keep first-seen version
			}
			readIdx[rd.Key] = len(merged.Reads)
			merged.Reads = append(merged.Reads, rd)
		}
		for _, w := range res.RWS.Writes {
			if j, ok := writeIdx[w.Key]; ok {
				merged.Writes[j] = w // last-write-wins
				continue
			}
			writeIdx[w.Key] = len(merged.Writes)
			merged.Writes = append(merged.Writes, w)
		}
	}
	return merged, events
}
```

> Confirm `blocks.ReadWriteSet` field names (`Reads []KVRead`, `Writes []KVWrite`) and `KVRead{Key,Version}` / `KVWrite{Key,IsDelete,Value}` in `fabric-x-sdk/blocks/types.go` before finalizing (verified during slice 1: they match).

- [ ] **Step 4: Run test** — `go test ./endorser/execution/... -run TestMergeResultsUnionAndLastWrite -count=1 -v` → PASS.

- [ ] **Step 5: Commit** — `git add endorser/execution/merge.go endorser/execution/merge_test.go && git commit -m "feat(endorser): merge batch execution results into one RWS + per-tx events"`

---

## Task 3: endorser `ExecuteBatch` (two-phase + merged, signed once)

**Files:**
- Modify: `endorser/api/service.go` (the file declaring `type Service interface`)
- Modify: `endorser/core/endorser.go`
- Test: `endorser/core/endorser_test.go` (add a case)

**Interfaces:**
- Consumes: `execution.EVMEngine.ExecuteBatch(ctx, []*types.Transaction) ([]endorsement.ExecutionResult, error)` (slice 1); `MergeResults` (Task 2); `endorsement.Builder.Endorse(inv, res) (*peer.ProposalResponse, error)`; `endorsement.Invocation`.
- Produces:
  - `Service.ExecuteBatch(ctx context.Context, inv endorsement.Invocation, txs []*types.Transaction) (*peer.ProposalResponse, error)` — one signed ProposalResponse over the merged RWS. Its `Response.Payload` carries the per-tx events/statuses (JSON) so the indexer can build per-tx receipts.
  - `EVMEngineInterface.ExecuteBatch(ctx, []*types.Transaction) ([]endorsement.ExecutionResult, error)` added to the interface in `endorser/core/endorser.go` (the concrete `*execution.EVMEngine` already implements it from slice 1).

- [ ] **Step 1: Read the current `Endorser.Execute` + `EVMEngineInterface`** in `endorser/core/endorser.go` to mirror its result→response classification and builder usage. Read `endorser/api/service.go` for the `Service` interface shape.

- [ ] **Step 2: Add `ExecuteBatch` to `EVMEngineInterface`** (endorser/core/endorser.go), alongside `Execute`:

```go
	ExecuteBatch(ctx context.Context, txs []*types.Transaction) (endorsement.ExecutionResult, [][]byte, error)
```

> Design note: give the ENGINE a convenience `ExecuteBatch` here that already returns the MERGED result + per-tx events, wrapping `execution.EVMEngine.ExecuteBatch` (per-tx) through `execution.MergeResults`. Add this wrapper method to `execution.EVMEngine` in `endorser/execution/executor.go`:
> ```go
> // ExecuteMergedBatch runs the batch two-phase and folds the per-tx results into
> // one merged ExecutionResult (status 200) plus per-tx event blobs for receipts.
> func (e *EVMEngine) ExecuteMergedBatch(ctx context.Context, txs []*types.Transaction) (endorsement.ExecutionResult, [][]byte, error) {
> 	results, err := e.ExecuteBatch(ctx, txs)
> 	if err != nil {
> 		return endorsement.ExecutionResult{}, nil, err
> 	}
> 	rws, events := MergeResults(results)
> 	return endorsement.ExecutionResult{RWS: rws, Status: 200}, events, nil
> }
> ```
> Then the interface method name is `ExecuteMergedBatch` (use that consistently below).

- [ ] **Step 3: Write the failing test** — add to `endorser/core/endorser_test.go` a `TestExecuteBatchMergedEndorsement` that builds an `Endorser` over a fake engine returning two per-tx results (reuse the existing endorser_test fake/engine pattern — read the file first), calls `ExecuteBatch`, and asserts the returned `*peer.ProposalResponse` has `Status == 200` and a non-empty merged write-set (decode via the same path `Execute`'s test uses). If the existing test uses a real `execution.EVMEngine` over the in-memory query client, mirror that instead of a fake.

- [ ] **Step 4: Run test to verify it fails** — `go test ./endorser/core/... -run TestExecuteBatchMergedEndorsement -count=1 -v` → FAIL (`ExecuteBatch` undefined).

- [ ] **Step 5: Implement `ExecuteBatch` in `endorser/core/endorser.go`:**

```go
// ExecuteBatch endorses a merged batch of EVM transactions: it two-phase-executes
// them, folds the results into one read/write-set, and signs a single
// ProposalResponse. The per-tx events ride in the response payload so the indexer
// can reconstruct per-tx receipts. CFT: one signature meets the policy.
func (f *Endorser) ExecuteBatch(ctx context.Context, inv endorsement.Invocation, txs []*types.Transaction) (*peer.ProposalResponse, error) {
	res, events, err := f.Engine.ExecuteMergedBatch(ctx, txs)
	if err != nil {
		return response(nil, err), nil
	}
	// Per-tx events -> response payload for receipts.
	payload, err := json.Marshal(events)
	if err != nil {
		return response(nil, fmt.Errorf("marshal batch events: %w", err)), nil
	}
	res.Payload = payload
	resp, err := f.builder.Endorse(inv, res)
	if err != nil {
		return response(nil, fmt.Errorf("endorse batch: %w", err)), nil
	}
	return resp, nil
}
```

Add `ExecuteBatch` to the `Service` interface in `endorser/api/service.go` (same signature). Add `encoding/json` to imports if missing.

- [ ] **Step 6: Run test** — `go test ./endorser/core/... -run TestExecuteBatchMergedEndorsement -count=1 -v` → PASS. Then `go build ./endorser/...` → PASS (the concrete engine and any `testimpl` wrappers must satisfy the extended interfaces; add the wrapper method to `endorser/testimpl` engine/endorser wrappers if they implement these interfaces — build errors will name them).

- [ ] **Step 7: Commit** — `git add endorser/ && git commit -m "feat(endorser): ExecuteBatch endorses a merged batch with one signature"`

---

## Task 4: gateway `EndorsementClient.ExecuteBatch`

**Files:**
- Modify: `gateway/core/endorse.go`
- Test: `gateway/core/endorse_test.go` (add a case)

**Interfaces:**
- Consumes: `Service.ExecuteBatch` (Task 3); `common.ProposalTypeEVMBatch` (Task 1); `createInvocation` (existing, in endorse.go); `sdk.Endorsement{Proposal, Responses}`.
- Produces: `func (e *EndorsementClient) ExecuteBatch(ctx context.Context, txs []*types.Transaction) (sdk.Endorsement, error)` — builds one batch invocation (`Args = [{ProposalTypeEVMBatch}, txBytes...]`), calls each endorser's `ExecuteBatch`, verifies matching statuses, returns one `sdk.Endorsement`.

- [ ] **Step 1: Read `EndorsementClient.ExecuteTransaction`** (endorse.go lines ~52–120) to mirror the invocation build + multi-endorser collection + status check.

- [ ] **Step 2: Write the failing test** — add `TestExecuteBatchBuildsMergedInvocation` to `gateway/core/endorse_test.go`, mirroring the existing `endorse_test.go` fake-endorser setup: pass two txs, assert the fake endorser received an invocation whose `Args[0] == byte(common.ProposalTypeEVMBatch)` and `len(Args) == 3` (type + 2 txs), and that `ExecuteBatch` returns a single-response `sdk.Endorsement`.

- [ ] **Step 3: Run test to verify it fails** — `go test ./gateway/core/... -run TestExecuteBatchBuildsMergedInvocation -count=1 -v` → FAIL.

- [ ] **Step 4: Implement `ExecuteBatch`** in `gateway/core/endorse.go`, modeled on `ExecuteTransaction`:

```go
// ExecuteBatch endorses a batch of EVM transactions as one merged Fabric tx.
// The invocation carries the batch: Args[0]=ProposalTypeEVMBatch, Args[1..N]=the
// marshaled EVM txs, in batch order.
func (e *EndorsementClient) ExecuteBatch(ctx context.Context, txs []*types.Transaction) (sdk.Endorsement, error) {
	args := make([][]byte, 0, len(txs)+1)
	args = append(args, []byte{byte(common.ProposalTypeEVMBatch)})
	for _, tx := range txs {
		b, err := tx.MarshalBinary()
		if err != nil {
			return sdk.Endorsement{}, err
		}
		args = append(args, b)
	}
	inv, err := e.createInvocation(args)
	if err != nil {
		return sdk.Endorsement{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := make([]*peer.ProposalResponse, len(e.endorsers))
	errs := make([]error, len(e.endorsers))
	var wg sync.WaitGroup
	for i, end := range e.endorsers {
		run := func(index int, endorser api.Service) {
			pResp, err := endorser.ExecuteBatch(ctx, inv, txs)
			if err != nil {
				errs[index] = fmt.Errorf("call endorser: %w", err)
				cancel()
				return
			}
			if pResp.Response.Status != common.StatusOK {
				errs[index] = fmt.Errorf("process EVM batch: %s", pResp.Response.Message)
				cancel()
				return
			}
			res[index] = pResp
		}
		if len(e.endorsers) > 1 {
			wg.Add(1)
			go func(i int, en api.Service) { defer wg.Done(); run(i, en) }(i, end)
		} else {
			run(i, end)
		}
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return sdk.Endorsement{}, err
		}
	}
	return sdk.Endorsement{Proposal: inv.Proposal, Responses: res}, nil
}
```

- [ ] **Step 5: Run test** — `go test ./gateway/core/... -run TestExecuteBatchBuildsMergedInvocation -count=1 -v` → PASS.

- [ ] **Step 6: Commit** — `git add gateway/core/endorse.go gateway/core/endorse_test.go && git commit -m "feat(gateway): EndorsementClient.ExecuteBatch builds a merged batch invocation"`

---

## Task 5: `PendingPool` (replaces the queue)

**Files:**
- Create: `gateway/core/pending.go`
- Test: `gateway/core/pending_test.go`

**Interfaces:**
- Produces:
  - `type PendingPool struct{ ... }` with `func NewPendingPool() *PendingPool`
  - `func (*PendingPool) Add(tx *types.Transaction)` — no-op if the hash is already present.
  - `func (*PendingPool) Has(hash common.Hash) bool`
  - `func (*PendingPool) DrainAll() []*types.Transaction` — returns all current txs in insertion order and leaves them present (they stay pending until `Remove`d on commit); returns nil if empty.
  - `func (*PendingPool) Remove(hashes []common.Hash)`
  - `func (*PendingPool) Len() int`

- [ ] **Step 1: Write the failing test** — `gateway/core/pending_test.go`

```go
package core

import (
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func txWithNonce(n uint64) *types.Transaction {
	return types.NewTx(&types.LegacyTx{Nonce: n, Gas: 21000})
}

func TestPendingPoolAddDrainRemove(t *testing.T) {
	p := NewPendingPool()
	a, b := txWithNonce(1), txWithNonce(2)
	p.Add(a)
	p.Add(b)
	p.Add(a) // dedup by hash
	if p.Len() != 2 {
		t.Fatalf("len = %d, want 2", p.Len())
	}
	drained := p.DrainAll()
	if len(drained) != 2 {
		t.Fatalf("drained %d, want 2", len(drained))
	}
	// DrainAll leaves entries pending until Remove.
	if p.Len() != 2 {
		t.Fatalf("after drain len = %d, want 2 (still pending)", p.Len())
	}
	p.Remove([]ethcommon.Hash{a.Hash()})
	if p.Len() != 1 || !p.Has(b.Hash()) || p.Has(a.Hash()) {
		t.Fatalf("after remove: len=%d hasB=%v hasA=%v", p.Len(), p.Has(b.Hash()), p.Has(a.Hash()))
	}
}
```

- [ ] **Step 2: Run test to verify it fails** — `go test ./gateway/core/... -run TestPendingPoolAddDrainRemove -count=1 -v` → FAIL (`NewPendingPool` undefined).

- [ ] **Step 3: Write `gateway/core/pending.go`**

```go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"sync"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// PendingPool holds accepted-but-not-committed transactions. It has no
// dependency graph and no ordering beyond insertion order: the drain-all
// executor takes the whole pool as one batch, and the two-phase authoritative
// pass + MVCC establish correctness. Entries stay pending until their batch
// commits (Remove) or, if excluded from a batch (nonce gap) or belonging to an
// aborted batch, until a later cycle picks them up again.
type PendingPool struct {
	mu    sync.Mutex
	order []ethcommon.Hash
	txs   map[ethcommon.Hash]*types.Transaction
}

func NewPendingPool() *PendingPool {
	return &PendingPool{txs: make(map[ethcommon.Hash]*types.Transaction)}
}

func (p *PendingPool) Add(tx *types.Transaction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := tx.Hash()
	if _, ok := p.txs[h]; ok {
		return
	}
	p.txs[h] = tx
	p.order = append(p.order, h)
}

func (p *PendingPool) Has(hash ethcommon.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.txs[hash]
	return ok
}

func (p *PendingPool) DrainAll() []*types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.order) == 0 {
		return nil
	}
	out := make([]*types.Transaction, 0, len(p.order))
	for _, h := range p.order {
		out = append(out, p.txs[h])
	}
	return out
}

func (p *PendingPool) Remove(hashes []ethcommon.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range hashes {
		delete(p.txs, h)
	}
	kept := p.order[:0]
	for _, h := range p.order {
		if _, ok := p.txs[h]; ok {
			kept = append(kept, h)
		}
	}
	p.order = kept
}

func (p *PendingPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.txs)
}
```

- [ ] **Step 4: Run test** — `go test ./gateway/core/... -run TestPendingPoolAddDrainRemove -count=1 -v` → PASS.

- [ ] **Step 5: Commit** — `git add gateway/core/pending.go gateway/core/pending_test.go && git commit -m "feat(gateway): add PendingPool (replaces dependency queue)"`

---

## Task 6: merged-batch block parsing + per-tx receipts (indexer)

**Files:**
- Modify: `gateway/core/chain.go` (`ConvertToDomain`)
- Modify: `gateway/domain/models.go` (add `SubIndex`)
- Test: `gateway/core/chain_test.go` (add a case)

**Interfaces:**
- Consumes: `common.ProposalTypeEVMBatch` (Task 1); `blocks.Transaction` (fields `InputArgs [][]byte`, `Number int64`, `Valid bool`, `Status int`, `Events []byte`, `ID string`); the per-tx events JSON in the merged tx's response payload (Task 3).
- Produces: `domain.Transaction.SubIndex int64`; `ConvertToDomain` emits one `domain.Transaction` per EVM tx inside a `ProposalTypeEVMBatch` Fabric tx, with `TxIndex = Fabric tx.Number`, `SubIndex = 0..N-1`, and per-tx status/events.

- [ ] **Step 1: Read `ConvertToDomain` + `convertTransaction`** (chain.go ~130–200) and `domain.Transaction` (`gateway/domain/models.go`) to see how a single-tx block is built today (`InputArgs[0]==ProposalTypeEVMTx`, `InputArgs[1]`=ethTx, one domain tx per Fabric tx).

- [ ] **Step 2: Add `SubIndex int64` to `domain.Transaction`** in `gateway/domain/models.go` (default 0 for legacy single-tx path).

- [ ] **Step 3: Write the failing test** — add `TestConvertToDomainBatch` to `gateway/core/chain_test.go`: build a `blocks.Block` with one `blocks.Transaction` whose `InputArgs = [{ProposalTypeEVMBatch}, ethTx1Bytes, ethTx2Bytes]` (reuse the test's existing eth-tx-bytes helper), call `ConvertToDomain`, and assert it yields 2 `domain.Transaction`s with the same `TxIndex` and `SubIndex` 0 and 1. Mirror the existing single-tx `chain_test.go` case for block/tx construction.

- [ ] **Step 4: Run test to verify it fails** — `go test ./gateway/core/... -run TestConvertToDomainBatch -count=1 -v` → FAIL.

- [ ] **Step 5: Extend `ConvertToDomain`** to handle the batch type. In the per-Fabric-tx loop, branch on `tx.InputArgs[0]`:

```go
switch {
case bytes.Equal(tx.InputArgs[0], []byte{byte(fc.ProposalTypeEVMTx)}):
	// existing single-tx path (SubIndex 0)
	// ... build one domain.Transaction with subIndex = 0 ...
case bytes.Equal(tx.InputArgs[0], []byte{byte(fc.ProposalTypeEVMBatch)}):
	for sub, ethTxBytes := range tx.InputArgs[1:] {
		// per-tx status/events: decode the per-tx events blob (index=sub) from the
		// merged tx's payload if present; otherwise fall back to tx.Events.
		etx, err := convertTransaction(ethTxBytes, b.Hash, b.Number, tx.Number, tx.ID, statusFor(sub), tx.Status, eventsFor(sub), &logIndex)
		if err != nil {
			panic(err)
		}
		etx.SubIndex = int64(sub)
		ebl.Transactions = append(ebl.Transactions, etx)
	}
default:
	continue // non-eth tx
}
```

> Verify-first: how per-tx events/status are carried on a committed merged Fabric tx. The endorser put per-tx events JSON in the ProposalResponse `Payload` (Task 3), but the committed `blocks.Transaction` exposes `Events` and `Status`, not the response payload. Before writing `statusFor`/`eventsFor`, confirm in `fabric-x-sdk` how `blocks.Transaction.Events` is populated from a committed tx and whether the response payload survives to the committed block. If the payload is NOT recoverable from the committed block, carry the per-tx events inside the batch envelope itself (e.g. Args layout `[{type}, ethTx1, events1, ethTx2, events2, ...]` or a trailing metadata arg) — decide this in Task 3's envelope encoding and reflect it here. This coupling MUST be resolved before Task 3 and Task 6 are both final; if it forces an envelope change, update Task 3/Task 4 accordingly and re-run their tests.

- [ ] **Step 6: Run test** — `go test ./gateway/core/... -run TestConvertToDomainBatch -count=1 -v` → PASS. Run the whole `chain` suite: `go test ./gateway/core/... -run TestConvert -count=1` → PASS (legacy single-tx path unchanged).

- [ ] **Step 7: Commit** — `git add gateway/core/chain.go gateway/domain/models.go gateway/core/chain_test.go && git commit -m "feat(gateway): parse merged-batch Fabric txs into (block,index,sub-index) domain txs"`

---

## Task 7: drain-all executor loop + wait-for-commit

**Files:**
- Create: `gateway/core/executor.go`
- Modify: `gateway/core/api.go` (`Gateway` struct, `New`, `SendTransaction`; remove `Start`/`worker`/`processTx`)
- Test: `gateway/core/executor_test.go`

**Interfaces:**
- Consumes: `PendingPool` (Task 5); `EndorsementClient.ExecuteBatch` (Task 4); the `BatchSubmitter` submit path + a commit-observation signal; `Store.BlockNumber`/commit notifications.
- Produces:
  - `Gateway` gains `pending *PendingPool` and an executor loop started by `Gateway.Start(ctx)`.
  - The loop: `batch := pending.DrainAll(); if batch == nil { wait for new arrivals }; end := endorsers.ExecuteBatch(ctx, batch); submit(end); wait-for-commit; pending.Remove(committed hashes)`.

- [ ] **Step 1: Read the current `Gateway.Start`/`worker`/`processTx`/`SubmitFabricTx`** (api.go ~128–203) and how commit is observed today (the path that calls `TxQueue.Complete` — trace the `AllTxBatchDispatcher`/`BatchSubmitter` cache to find the commit signal). This determines how the loop learns a batch committed.

- [ ] **Step 2: Wait on the committer-tx outcome via the notification service.** The notification service now serves exactly one purpose: report each committer tx's (= EVM batch's) outcome so the executor can finalize on success and **roll back on failure**. The signal is `common.TxNotification{FabricTxID string, Status committerpb.Status}` delivered to a registered `common.TxHandler.HandleTx([]TxNotification)` by the `AllTxBatchDispatcher` (fed by the committed-block/notification stream). Implement `awaitCommit(ctx, fabricTxID string) error`: the executor registers, per submitted committer tx, a one-shot waiter (e.g. a `map[string]chan committerpb.Status` guarded by a mutex, populated at submit, signaled by the gateway's `HandleTx`); `awaitCommit` blocks on that channel and returns **nil when the committer tx committed valid**, or an **error when it committed invalid (MVCC abort) or ctx is done** — the error case leaves the batch's txs in the pending pool so the next cycle re-drains them (rollback). The committer `fabricTxID` is the endorsement invocation's `TxID` (`sdk.Endorsement`/the invocation the batch was submitted with). Do NOT poll the store and do NOT count committer txs as throughput — throughput is EVM txs, one committer tx carries the whole batch.

- [ ] **Step 3: Write the failing test** — `gateway/core/executor_test.go`: with a fake `EndorsementClient`-like seam and a fake submitter+commit signal, drive one cycle: add 3 txs to the pool, run one executor iteration, assert `ExecuteBatch` was called once with all 3 txs, the merged endorsement was submitted once, and after the (faked) commit the 3 txs are removed from the pool. (Introduce a minimal interface for the batch-endorse + submit + await-commit steps so the loop is unit-testable without a network — mirror how `batch_submitter_test.go` / `chain_resilience_test.go` fake their dependencies.)

- [ ] **Step 4: Run test to verify it fails** — `go test ./gateway/core/... -run TestExecutorDrainCycle -count=1 -v` → FAIL.

- [ ] **Step 5: Implement the loop** in `gateway/core/executor.go` (single goroutine):

```go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import "context"

// runExecutor is the drain-all loop: one batch in flight at a time. Each cycle
// drains the whole pending pool, two-phase-executes + merges it into one Fabric
// tx, submits it, waits for it to commit, and removes the committed txs. No
// worker pool, no dependency ordering. (Stage 2.1: wait-for-commit; the overlay
// + wait-for-ordered pipeline is stage 2.3.)
func (g *Gateway) runExecutor(ctx context.Context) {
	defer g.wg.Done()
	for {
		batch := g.pending.DrainAll()
		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-g.arrivals: // signaled by SendTransaction when the pool was empty
			}
			continue
		}

		end, err := g.endorsers.ExecuteBatch(ctx, batch)
		if err != nil {
			logger.Errorf("batch endorse failed (%d txs): %v", len(batch), err)
			// leave txs pending; excluded/invalid handling refined below
			continue
		}
		if err := g.SubmitFabricTx(ctx, end); err != nil {
			logger.Errorf("batch submit failed: %v", err)
			continue
		}
		if err := g.awaitCommit(ctx, end); err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Errorf("await commit failed: %v", err)
			continue // txs stay pending -> re-drained
		}
		g.pending.Remove(hashesOf(batch)) // refine: remove only INCLUDED txs (Task 8)
	}
}
```

- [ ] **Step 6: Rewire `api.go`** — `Gateway` struct: replace `TxQueue TxQueueInterface`, `workerCount int` with `pending *PendingPool` and `arrivals chan struct{}`. `New(...)` drops `workerCount`/`txQueue`, constructs `pending`+`arrivals`. `Start` launches one `go g.runExecutor(ctx)`. `SendTransaction`: `ValidateTx` → `if g.pending.Has(tx.Hash()) { return ErrTransactionAlreadyPending }` → `g.pending.Add(tx)` → non-blocking signal on `arrivals`. Delete `worker`/`processTx`. Keep `SubmitFabricTx`.

- [ ] **Step 7: Run tests** — `go test ./gateway/core/... -run TestExecutorDrainCycle -count=1 -v` → PASS. `go build ./gateway/...` → expect errors at callers of the removed `TxQueue`/`workerCount` (fixed in Task 9/10).

- [ ] **Step 8: Commit** — `git add gateway/core/executor.go gateway/core/api.go gateway/core/executor_test.go && git commit -m "feat(gateway): drain-all two-phase executor loop with wait-for-commit"`

---

## Task 8: nonce-gap exclusion + included-set tracking

**Files:**
- Modify: `endorser/core/endorser.go` (`ExecuteBatch` reports excluded sub-txs) or `endorser/execution` (surface per-tx OK/excluded)
- Modify: `gateway/core/executor.go` (remove only INCLUDED txs; leave excluded pending)
- Test: `gateway/core/executor_test.go` (add a case)

**Interfaces:**
- Produces: the batch endorsement carries which input txs were INCLUDED vs EXCLUDED (nonce-too-high / rejected). The executor removes only included txs on commit; excluded stay pending.

- [ ] **Step 1: Decide where exclusion is detected.** In `execution.EVMEngine.ExecuteBatch`'s authoritative pass, a tx that fails `PrepareMessage` with `ErrNonceTooHigh` (or is otherwise unexecutable) is excluded from the merged RWS. Return, alongside the results, the indices/hashes that were included. Extend `ExecuteMergedBatch` (Task 3) to also return `included []common.Hash` (or a bool mask), and thread it through `Endorser.ExecuteBatch` → the response payload / a side return → `EndorsementClient.ExecuteBatch` → the executor.

- [ ] **Step 2: Write the failing test** — a batch with a nonce-gap tx (nonce N+1 with no N present) asserts: the merged batch commits the executable txs, and the gap tx remains in the pending pool after the cycle.

- [ ] **Step 3: Run to fail, implement, run to pass.** Implement the included-set threading and change the executor's `pending.Remove` to remove only included hashes. Command: `go test ./gateway/core/... -run TestExecutorNonceGap -count=1 -v`.

- [ ] **Step 4: Commit** — `git commit -am "feat(gateway): exclude nonce-gap txs from the batch and retain them pending"`

---

## Task 9: wire the gateway app onto the pending pool + executor

**Files:**
- Modify: `gateway/app/app.go`, `gateway/app/wiring.go`
- Modify: `gateway/config` (remove/deprecate `Gateway.WorkerCount`)

- [ ] **Step 1: Read `gateway/app/app.go` `buildApp`/`BuildGateway`** to see how `New(...)`/`WorkerCount`/`TxQueue` are currently constructed and passed.

- [ ] **Step 2: Update `BuildGateway`/`New` call sites** to the new `New` signature (no `workerCount`/`txQueue`; the gateway builds its own `PendingPool`). Remove `Gateway.WorkerCount` from config plumbing (delete the field or leave it ignored with a deprecation comment — pick deletion unless other code reads it; grep first).

- [ ] **Step 3: Build** — `go build ./...` → expect remaining errors only at `TxQueue`/`TxQueueV2` references (removed in Task 10) and the perf test's `Gateway.WorkerCount` override (Task 10).

- [ ] **Step 4: Commit** — `git commit -am "refactor(gateway): wire app onto PendingPool + drain-all executor; drop WorkerCount"`

---

## Task 10: delete the dependency queue + fix references

**Files:**
- Delete: `gateway/core/txqueue_v2.go` (+ `_test.go`), `gateway/core/txqueue.go` (+ `_test.go`), `gateway/core/txqueue_helpers.go`
- Modify: `gateway/core/api.go` (remove `TxQueueInterface`), `integration/perf/replay_json_dataset_test.go` + `integration/test_helpers.go` (drop `TxQueueV2`/`-oldqueue`/`WorkerCount` usage), any other references.

- [ ] **Step 1: Find references** — `grep -rn "TxQueue\|TxQueueV2\|NewTxQueue\|WorkerCount\|oldqueue" $(find . -name '*.go' -not -path './vendor/*')`.

- [ ] **Step 2: Delete the queue files** — `git rm gateway/core/txqueue.go gateway/core/txqueue_v2.go gateway/core/txqueue_helpers.go` and their `_test.go` files. Remove `TxQueueInterface` from `api.go`.

- [ ] **Step 3: Fix callers** — the perf test (`replay_json_dataset_test.go`) passes a `TxQueueV2`/`TxQueue` and an `-oldqueue` flag + `Gateway.WorkerCount`/`Gateway.SubmitterCount` overrides; drop the queue argument and `-oldqueue`, and remove the `WorkerCount` override (keep `SubmitterCount` — that's the orderer submitter count, still valid). `test_helpers.go` `buildTestHarness*` passes `txQueue` into gateway construction; drop it.

- [ ] **Step 4: Build + vet** — `go build ./... && go vet ./gateway/... ./integration/...` → PASS.

- [ ] **Step 5: Run the fast suites** — `go test ./gateway/... ./endorser/... -count=1` → PASS (except the known pre-existing `TestTxQueueV2_*` — which is now DELETED, so it should simply be gone; confirm no new failures).

- [ ] **Step 6: Commit** — `git commit -am "refactor(gateway): remove TxQueue/TxQueueV2 dependency detection and worker pool"`

---

## Task 11: integration test — merged batch commits with per-tx receipts

**Files:**
- Modify: `integration/integration_test.go` or a new `integration/batch_test.go` (in-process fabric-x harness)

- [ ] **Step 1: Write an in-process test** (mirror `TestLocalX`'s harness setup) that submits a small set of TXs — some independent, two from the same sender with consecutive nonces — through the gateway, waits for commit, and asserts: (a) they land in one committed block; (b) `eth_getTransactionReceipt` for each returns status 1 (or 0 for a revert) with correct `(blockNumber, transactionIndex)` and a distinct sub-index; (c) `eth_getTransactionByHash` resolves each. Use the harness's memory-mode endorser (query-service mem client).

- [ ] **Step 2: Run** — `go test ./integration/ -run TestBatchMergedCommit -count=1 -v` → PASS (in-process fabric-x, no docker).

- [ ] **Step 3: Commit** — `git commit -am "test(integration): merged batch commits with per-tx receipts"`

> **Perf-harness refactor (done in the end-to-end perf-testing phase, not this task):** the perf replay (`integration/perf/replay_json_dataset_test.go`) must (a) DROP the `-outstanding` flow control / semaphore entirely — it existed to mitigate the thrashing this work removes; fire all txs at the gateway (`SendTransaction`), and the pending pool + drain-all loop + query-service `GetRows` batching pace them naturally; and (b) report throughput as **EVM tx/s** (committed EVM transactions per second), NOT committer-tx/s — one committer tx carries the whole batch, so committer-tx/s is far smaller and misleading.

---

## Task 12: docs

**Files:**
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1** — In the Performance / concurrency section, replace the "in-memory dependency manager + retry loop + worker pool" description with the drain-all two-phase batch model: pending pool → parallel warm → serial authoritative → one merged Fabric tx per batch (wait-for-commit in this stage); note that concurrency = batch size (no worker count) and that dependency detection was removed. Note stages 2.2/2.3 as follow-ups.

- [ ] **Step 2: Commit** — `git commit -am "docs: describe the drain-all two-phase batch execution model"`

---

## Self-Review

- **Spec coverage:** RQ1 no worker pool (Tasks 7, 9, 10); RQ2 no dependency detection (Task 10); RQ3 two-phase execution (Tasks 2–4, reusing slice-1 BatchExecutor); RQ4 merged-batch commit + `(blk,idx,sub-idx)` (Tasks 1, 3, 6); RQ5 pipelined overlay — **deferred to stage 2.3** (this plan is wait-for-commit, per the spec's staging); RQ6 nonce-gap handling (Task 8); RQ7 throughput scales with batch size (validated in Task 11 + the stage-level perf run). Wait-for-commit is the stage-2.1 substitute for RQ5.
- **Verify-first flags (resolve inside the task, not as placeholders):** per-tx events/status carriage on a committed merged Fabric tx (Task 6 Step 5 — may force the batch envelope in Task 3/4 to embed per-tx events; resolve the envelope encoding before finalizing Tasks 3, 4, 6); the wait-for-commit signal wired in the production gateway (Task 7 Steps 1–2); whether `WorkerCount` is read elsewhere before deleting (Task 9); testimpl wrappers implementing the extended endorser interfaces (Task 3 Step 6).
- **Type consistency:** `ExecuteMergedBatch` (engine) → `Endorser.ExecuteBatch` → `EndorsementClient.ExecuteBatch` → executor loop; `PendingPool` method names (`Add`/`Has`/`DrainAll`/`Remove`/`Len`); `common.ProposalTypeEVMBatch`; `domain.Transaction.SubIndex`; `MergeResults` used by `ExecuteMergedBatch`.
- **Cross-stage note:** the merged batch immediately requires Task 6's parsing (else committed batches are unreadable), so the "2.2 indexer" work's *parsing core* is folded into this plan; richer indexer/receipt work (logs by sub-index, block-hash detail) can remain a 2.2 follow-up.
