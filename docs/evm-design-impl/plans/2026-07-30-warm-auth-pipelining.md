# Warm(N+1) ‖ Auth(N) Execution Pipelining Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Overlap the I/O-bound concurrent *warm* pass of batch N+1 with the CPU-bound serial *authoritative* pass of batch N, collapsing the gateway's per-batch wall from `warm + auth` to `max(warm, auth) + boundary`, behind an opt-in flag.

**Architecture:** Split the engine's `ExecuteBatch` into `WarmBatch` (opens+warms a snapshot, returns an open handle) and an authoritative half (`authBatch`/`AuthMergedBatch`). A new opt-in gateway loop `runExecutorPipelined` carries one warmed batch across iterations (depth 1): it launches `warm(N+1)` concurrently with `auth(N)`, **joins at a barrier** before any cross-batch cache mutation, then applies the boundary work on the single executor goroutine — preserving the "mutate caches only at the boundary, never during warm reads" invariant with no lock and no snapshot. Reservations in `PendingPool` stop the non-destructive drain from re-drawing an in-flight batch.

**Tech Stack:** Go; `endorser/execution` (EVM engine), `endorser/api` + `endorser/core` (endorser), `gateway/core` (executor + pending pool), `integration` (perf harness).

## Global Constraints

- Branch `bft-redesign`; **NO push**. Commit locally only.
- Commit trailer, verbatim: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- The **default serial path is byte-for-byte behavior-unchanged**; the pipeline is opt-in (`SetPipelined`, default false).
- The `len(txs)==1` and `len(txs)==0` engine fast paths stay intact.
- Auth pass stays **serial and in-order** (MVCC correctness); warm results are discarded (staleness harmless).
- No new lock on the `VersionedCache` read path; safety comes from the barrier join, not locking.
- Do **not** commit scratch scripts (rig-only `/tmp/*.sh`).
- Naming note: the existing "pipelined executor" in code/docs means the async in-flight *commit* window (`maxInflight`). This feature is the *warm/auth execution* pipeline. Keep the two distinct in comments.

---

## File Structure

- `gateway/core/pending.go` — add reservation set + `DrainUpToReserved`/`Release`; `Remove` also clears reservations. Serial `DrainUpTo` unchanged.
- `endorser/execution/batch_executor.go` — add `WarmedBatch` handle, `WarmBatch`, unexported `authBatch`, `AuthMergedBatch`; refactor `ExecuteBatch` to `WarmBatch`→`authBatch` for `len>1`.
- `endorser/api/service.go` — add `WarmedBatch` interface (`Close() error`) + `WarmBatch`/`AuthBatch` to `Service`.
- `endorser/core/endorser.go` — extend `EVMEngineInterface`; add `endorseBatch` helper; add `WarmBatch`/`AuthBatch` on `Endorser`.
- `gateway/core/endorse.go` — add gateway-level `WarmedBatch` bundle + `WarmBatch`/`AuthBatch` fan-out.
- `gateway/core/executor.go` — extract `submitExecuted`; add `runExecutorPipelined`, `warmNextBatch`, `pipelineIteration`; dispatch from `runExecutor`.
- `gateway/core/api.go` — add `pipelined bool` field + `SetPipelined`.
- `gateway/config/config.go` — add `Pipelined bool` gateway config field.
- `integration/test_helpers.go` — call `gw.SetPipelined(cfg.Gateway.Pipelined)` in the pre-Start window.
- `integration/perf/replay_json_dataset_test.go` — add `-pipeline` flag → `"Gateway.Pipelined"` override.
- Tests: `gateway/core/pending_test.go`, `endorser/execution/batch_executor_test.go`, `endorser/core/endorser_test.go`, `gateway/core/endorse_test.go`, `gateway/core/executor_test.go`.

---

### Task 1: PendingPool reservation

**Files:**
- Modify: `gateway/core/pending.go`
- Test: `gateway/core/pending_test.go`

**Interfaces:**
- Consumes: existing `PendingPool{mu, order []ethcommon.Hash, txs map[ethcommon.Hash]*types.Transaction}`, `NewPendingPool()`, `DrainUpTo(max int) []*types.Transaction`, `Remove(hashes []ethcommon.Hash)`.
- Produces: `(*PendingPool).DrainUpToReserved(max int) []*types.Transaction`, `(*PendingPool).Release(hashes []ethcommon.Hash)`; `reserved map[ethcommon.Hash]struct{}` field; `Remove` additionally clears reservations. `DrainUpTo` semantics UNCHANGED.

- [ ] **Step 1: Write the failing test** (append to `gateway/core/pending_test.go`)

```go
func TestPendingPoolReservation(t *testing.T) {
	p := NewPendingPool()
	h1, h2, h3 := ethcommon.HexToHash("0x1"), ethcommon.HexToHash("0x2"), ethcommon.HexToHash("0x3")
	p.Add(&pendingTx{hash: h1})
	p.Add(&pendingTx{hash: h2})
	p.Add(&pendingTx{hash: h3})

	// First reserved drain takes the batch and reserves it.
	first := p.DrainUpToReserved(2)
	if len(first) != 2 || first[0].Hash() != h1 || first[1].Hash() != h2 {
		t.Fatalf("first reserved drain = %v, want [h1 h2] in order", hashesOf(first))
	}
	// A second reserved drain SKIPS the reserved h1,h2 and returns h3.
	second := p.DrainUpToReserved(2)
	if len(second) != 1 || second[0].Hash() != h3 {
		t.Fatalf("second reserved drain = %v, want [h3]", hashesOf(second))
	}
	// A third reserved drain returns nothing (all reserved).
	if got := p.DrainUpToReserved(2); len(got) != 0 {
		t.Fatalf("third reserved drain = %v, want empty", hashesOf(got))
	}
	// Release h1,h2 -> re-drawable again.
	p.Release([]ethcommon.Hash{h1, h2})
	redraw := p.DrainUpToReserved(2)
	if len(redraw) != 2 || redraw[0].Hash() != h1 {
		t.Fatalf("re-draw after release = %v, want [h1 h2]", hashesOf(redraw))
	}
	// Remove clears reservations AND deletes from pending.
	p.Remove([]ethcommon.Hash{h1, h2, h3})
	if p.Len() != 0 {
		t.Fatalf("Len after remove = %d, want 0", p.Len())
	}
	if len(p.reserved) != 0 {
		t.Fatalf("reserved after remove = %d, want 0", len(p.reserved))
	}
	// Serial DrainUpTo is unaffected by reservations (non-destructive peek).
	p.Add(&pendingTx{hash: h1})
	if got := p.DrainUpTo(10); len(got) != 1 || got[0].Hash() != h1 {
		t.Fatalf("serial DrainUpTo = %v, want [h1]", hashesOf(got))
	}
}
```

Note: use the same `pendingTx`/`Add`/`Len`/`hashesOf` helpers the existing `pending_test.go` uses. If `pending_test.go` constructs pending entries differently (e.g. via real `*types.Transaction`), mirror that construction instead — do not invent a `pendingTx` type. Verify against the existing file first.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestPendingPoolReservation -v`
Expected: FAIL — `DrainUpToReserved`/`Release`/`reserved` undefined.

- [ ] **Step 3: Add the reservation field + init**

In the struct literal add `reserved map[ethcommon.Hash]struct{}`; in `NewPendingPool` init it:

```go
func NewPendingPool() *PendingPool {
	return &PendingPool{
		txs:      make(map[ethcommon.Hash]*types.Transaction),
		reserved: make(map[ethcommon.Hash]struct{}),
	}
}
```

(Preserve any other existing field inits verbatim.)

- [ ] **Step 4: Add `DrainUpToReserved` and `Release`; extend `Remove`**

```go
// DrainUpToReserved peeks up to max txs in FIFO order, SKIPPING any already
// reserved, and marks the returned hashes reserved before returning them. It is
// the pipelined executor's drain: it lets the executor hold batch N reserved
// (drained-but-not-yet-resolved) while it drains batch N+1, so the pool never
// re-draws an in-flight batch. max <= 0 means "all unreserved". The serial path
// uses DrainUpTo, which does not reserve.
func (p *PendingPool) DrainUpToReserved(max int) []*types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]*types.Transaction, 0, len(p.order))
	for _, h := range p.order {
		if _, isReserved := p.reserved[h]; isReserved {
			continue
		}
		tx, ok := p.txs[h]
		if !ok {
			continue
		}
		out = append(out, tx)
		p.reserved[h] = struct{}{}
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

// Release clears the reservation for the given hashes that remain in pending
// (e.g. a batch whose results were handled: included/terminal txs are Remove()d,
// the rest are Released so a later drain can re-draw them). Releasing an unknown
// or already-removed hash is a no-op.
func (p *PendingPool) Release(hashes []ethcommon.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range hashes {
		delete(p.reserved, h)
	}
}
```

In `Remove`, inside the existing loop that `delete(p.txs, h)`, also clear the reservation:

```go
	for _, h := range hashes {
		delete(p.txs, h)
		delete(p.reserved, h)
	}
```

(Keep the existing `order` rebuild via `kept := p.order[:0]` exactly as-is.)

- [ ] **Step 5: Run tests**

Run: `go test ./gateway/core/ -run TestPendingPool -v`
Expected: PASS (new + existing pending tests).

- [ ] **Step 6: Commit**

```bash
git add gateway/core/pending.go gateway/core/pending_test.go
git commit -m "feat(gateway): PendingPool reservation for the warm/auth pipeline

DrainUpToReserved skips+reserves so the pipelined executor can hold batch N
reserved while draining N+1; Release un-reserves staying txs; Remove also
clears reservations. Serial DrainUpTo unchanged.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Engine WarmBatch / authBatch / AuthMergedBatch split

**Files:**
- Modify: `endorser/execution/batch_executor.go`
- Modify: `endorser/execution/executor.go` (only if `AuthMergedBatch` is placed there; otherwise leave)
- Test: `endorser/execution/batch_executor_test.go`

**Interfaces:**
- Consumes: `EVMEngine`, `ReadStore`, `e.kvs.NewSnapshot(0)`, `overlayReader`, `newReusableExecutor`, `countingReader`, `EVMConfig`, `endorsement.ExecutionResult`, `MergeResults`, `PerTxOutcome`, `excludedResult`, existing warm/auth loop bodies.
- Produces:
  - `type WarmedBatch struct { ... Close() error }` (exported; idempotent Close via `sync.Once`).
  - `(e *EVMEngine) WarmBatch(ctx context.Context, txs []*types.Transaction) (*WarmedBatch, error)` — opens snapshot + runs warm pass; snapshot NOT closed.
  - `(e *EVMEngine) authBatch(wb *WarmedBatch) ([]endorsement.ExecutionResult, error)` — unexported; `defer wb.Close()`; runs auth pass + timing logs.
  - `(e *EVMEngine) AuthMergedBatch(ctx context.Context, wb *WarmedBatch) (endorsement.ExecutionResult, []PerTxOutcome, error)` — calls `authBatch`, folds via `MergeResults` (mirror of `ExecuteMergedBatch`).
  - `ExecuteBatch` unchanged externally; internally for `len>1` becomes `wb,err := e.WarmBatch(...); if err {return nil,err}; return e.authBatch(wb)`. `len==0`/`len==1` inline (each opens+defers its own snapshot).

- [ ] **Step 1: Write the failing test** (append to `endorser/execution/batch_executor_test.go`)

```go
// WarmBatch + AuthMergedBatch must produce the SAME merged RWS and outcomes as
// the one-shot ExecuteMergedBatch over the same txs (the split is behavior-
// preserving; only the warm/auth boundary is exposed).
func TestWarmThenAuthEqualsExecuteMerged(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:warmauth?mode=memory&cache=shared")
	require.NoError(t, err)
	key := newTestKey(t)
	to := newTestKey(t)
	seedAccounts(t, backend, map[ethcommon.Address]int64{
		crypto.PubkeyToAddress(key.PublicKey): 1_000_000,
	})
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	kvs := &testVersionedDBSnapshotter{db: backend}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	toAddr := crypto.PubkeyToAddress(to.PublicKey)
	tx1 := newTransferTx(t, cfg.ChainConfig, key, toAddr, 10, 0)
	tx2 := newTransferTx(t, cfg.ChainConfig, key, toAddr, 20, 1)
	txs := []*types.Transaction{tx1, tx2}

	wantRWS, wantOutcomes, err := engine.ExecuteMergedBatch(context.Background(), txs)
	require.NoError(t, err)

	wb, err := engine.WarmBatch(context.Background(), txs)
	require.NoError(t, err)
	gotRWS, gotOutcomes, err := engine.AuthMergedBatch(context.Background(), wb)
	require.NoError(t, err)

	assertSameRWS(t, "warm+auth vs execute-merged", gotRWS.Reads, wantRWS.Reads)
	assertSameRWS(t, "warm+auth vs execute-merged", gotRWS.Writes, wantRWS.Writes)
	require.Equal(t, len(wantOutcomes), len(gotOutcomes))
	for i := range wantOutcomes {
		require.Equal(t, wantOutcomes[i].Status, gotOutcomes[i].Status)
		require.Equal(t, wantOutcomes[i].Event, gotOutcomes[i].Event)
	}
}

// Close is idempotent (double Close must not panic / double-close the snapshot).
func TestWarmedBatchCloseIdempotent(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:warmclose?mode=memory&cache=shared")
	require.NoError(t, err)
	key := newTestKey(t)
	seedAccounts(t, backend, map[ethcommon.Address]int64{
		crypto.PubkeyToAddress(key.PublicKey): 1_000_000,
	})
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, &testVersionedDBSnapshotter{db: backend}, cfg, false)
	tx := newTransferTx(t, cfg.ChainConfig, key, crypto.PubkeyToAddress(newTestKey(t).PublicKey), 10, 0)

	wb, err := engine.WarmBatch(context.Background(), []*types.Transaction{tx})
	require.NoError(t, err)
	require.NoError(t, wb.Close())
	require.NoError(t, wb.Close()) // second close is a no-op
}
```

Match the exact helper names/signatures already in `batch_executor_test.go` (`newTestKey`, `newTransferTx`, `seedAccounts`, `assertSameRWS`, `Channel`, `Namespace`, `NewEVMEngine`, `testVersionedDBSnapshotter`). Adjust the transfer-value/nonce helper call to the file's real signature if it differs.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/execution/ -run 'TestWarmThenAuthEqualsExecuteMerged|TestWarmedBatchCloseIdempotent' -v`
Expected: FAIL — `WarmBatch`/`AuthMergedBatch`/`WarmedBatch` undefined.

- [ ] **Step 3: Add the `WarmedBatch` handle** (top of `batch_executor.go`, after imports)

```go
// WarmedBatch owns an open query-service snapshot that WarmBatch has already
// primed (warmed) for txs. The authoritative pass (authBatch/AuthMergedBatch)
// consumes it and closes the snapshot; an abandoned handle must be Close()d to
// release the view. Close is idempotent.
type WarmedBatch struct {
	reader        ReadStore // the open snapshot (NOT yet closed)
	readSrc       ReadStore // counted wrapper when timing, else == reader
	txs           []*types.Transaction
	fast          bool
	timing        bool
	counted       *countingReader
	snapDur       time.Duration
	warmDur       time.Duration
	warmDoneNanos []int64
	authCostNanos []int64
	closeOnce     sync.Once
}

// Close releases the warmed snapshot. Safe to call multiple times.
func (wb *WarmedBatch) Close() error {
	var err error
	wb.closeOnce.Do(func() {
		if wb.reader != nil {
			err = wb.reader.Close()
		}
	})
	return err
}
```

(If `batch_executor.go` does not already import `sync`, add it.)

- [ ] **Step 4: Add `WarmBatch`** — move the snapshot-open + warm loop out of `ExecuteBatch` verbatim, returning the handle (do NOT `defer reader.Close()`).

```go
// WarmBatch opens a query-service snapshot and runs the concurrent warm pass
// against it (goroutine-per-tx work-stealing pool priming the per-view read
// cache; results discarded). It returns the STILL-OPEN snapshot bundled in a
// WarmedBatch; the caller MUST later call AuthMergedBatch/authBatch (which close
// it) or Close() directly. Only used on the len(txs) > 1 pipeline path.
func (e *EVMEngine) WarmBatch(ctx context.Context, txs []*types.Transaction) (*WarmedBatch, error) {
	reader, err := e.kvs.NewSnapshot(0)
	if err != nil {
		return nil, err
	}

	// --- COPY the existing ExecuteBatch len>1 warm section verbatim from here:
	//   timing := batchLogger.IsEnabledFor(zapcore.DebugLevel)
	//   var readSrc ReadStore = reader; var counted *countingReader; if timing {...}
	//   warmDoneNanos/authCostNanos allocation
	//   fast := e.stateDecorator == nil && !e.evmConfig.DebugLogs
	//   warmWorkers, next atomic.Int64, wg, work-stealing warm loop, wg.Wait()
	//   warmDur := ...
	// On any unrecoverable error, reader.Close() then return nil, err.
	// ---

	return &WarmedBatch{
		reader:        reader,
		readSrc:       readSrc,
		txs:           txs,
		fast:          fast,
		timing:        timing,
		counted:       counted,
		snapDur:       snapDur,       // if ExecuteBatch measured it; else 0
		warmDur:       warmDur,
		warmDoneNanos: warmDoneNanos,
		authCostNanos: authCostNanos,
	}, nil
}
```

The engineer MUST paste the real warm-section statements from the current `ExecuteBatch` (this plan intentionally names every local it produces so the struct fields line up). No behavior change.

- [ ] **Step 5: Add `authBatch`** — the auth loop + timing logs, consuming the handle.

```go
// authBatch runs the serial, in-order authoritative pass over wb's already-
// warmed snapshot, closes the snapshot, and returns the same []ExecutionResult
// ExecuteBatch's len>1 path returns. It is the second half of the split.
func (e *EVMEngine) authBatch(wb *WarmedBatch) ([]endorsement.ExecutionResult, error) {
	defer wb.Close()

	txs := wb.txs
	// --- COPY the existing ExecuteBatch len>1 auth section verbatim, reading
	//   wb.readSrc as the overlay's `under`, wb.fast, wb.timing, wb.counted,
	//   wb.warmDoneNanos, wb.authCostNanos, wb.warmDur, wb.snapDur:
	//   overlay := &overlayReader{under: wb.readSrc, writes: map[string]*blocks.WriteRecord{}}
	//   authSdb/authEx, per-tx classify or newState/runOn, TxRejected -> excludedResult+continue,
	//   else overlay.apply + append, authDur, and the ENDORSE-TIMING + OVERLAP-SIM debug logs.
	// ---
	return results, nil
}
```

- [ ] **Step 6: Add `AuthMergedBatch`** (mirror of `ExecuteMergedBatch`)

```go
// AuthMergedBatch runs the authoritative pass over the warmed handle and folds
// the per-tx results into one merged read-write set + per-tx outcomes, exactly
// as ExecuteMergedBatch does for the one-shot path.
func (e *EVMEngine) AuthMergedBatch(ctx context.Context, wb *WarmedBatch) (endorsement.ExecutionResult, []PerTxOutcome, error) {
	results, err := e.authBatch(wb)
	if err != nil {
		return endorsement.ExecutionResult{}, nil, err
	}
	mergedRWS, events := MergeResults(results)
	outcomes := make([]PerTxOutcome, len(results))
	for i, r := range results {
		outcomes[i] = PerTxOutcome{Status: r.Status, Event: events[i]}
	}
	return endorsement.ExecutionResult{ReadWriteSet: mergedRWS}, outcomes, nil
}
```

**IMPORTANT:** copy the exact folding logic from the current `ExecuteMergedBatch` (field names, how `Status`/`Event` are populated, how `ReadWriteSet` is assigned). The block above is a template — reconcile it to the real `ExecuteMergedBatch` body so the two produce identical output (the test asserts this).

- [ ] **Step 7: Refactor `ExecuteBatch` len>1 path**

Replace the (now-moved) warm+auth body of the `len(txs) > 1` branch with:

```go
	wb, err := e.WarmBatch(ctx, txs)
	if err != nil {
		return nil, err
	}
	return e.authBatch(wb)
```

Keep `len==0` (`return nil, nil`) and the `len==1` fast path exactly as they are (the `len==1` path opens its own snapshot with `defer reader.Close()`).

- [ ] **Step 8: Run tests**

Run: `go test ./endorser/execution/ -run 'Warm|ExecuteBatch|Merged' -v`
Expected: PASS (new + existing).

- [ ] **Step 9: Run the full package under race**

Run: `go test -race ./endorser/execution/...`
Expected: PASS, no data races.

- [ ] **Step 10: Commit**

```bash
git add endorser/execution/batch_executor.go endorser/execution/executor.go endorser/execution/batch_executor_test.go
git commit -m "feat(execution): split ExecuteBatch into WarmBatch + authBatch/AuthMergedBatch

WarmBatch opens+warms a snapshot and returns an open handle; authBatch runs the
serial auth pass and closes it; AuthMergedBatch folds outcomes like
ExecuteMergedBatch. ExecuteBatch len>1 now composes the two; len==0/1 unchanged.
Behavior-preserving (test: warm+auth == execute-merged).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: api.Service + Endorser plumbing

**Files:**
- Modify: `endorser/api/service.go`
- Modify: `endorser/core/endorser.go`
- Test: `endorser/core/endorser_test.go`

**Interfaces:**
- Consumes: `Service` interface, `execution.WarmedBatch`, `execution.PerTxOutcome`, `Endorser{Engine EVMEngineInterface; builder}`, `EVMEngineInterface`, `Endorser.ExecuteBatch`, `endorsement.Invocation`, `peer.ProposalResponse`, `f.builder.Endorse`, `response(nil, err)`.
- Produces:
  - `api.WarmedBatch` interface (`Close() error`).
  - `Service.WarmBatch(ctx, txs) (WarmedBatch, error)` and `Service.AuthBatch(ctx, inv endorsement.Invocation, wb WarmedBatch) (*peer.ProposalResponse, error)`.
  - `EVMEngineInterface.WarmBatch(ctx, txs) (*execution.WarmedBatch, error)` and `.AuthMergedBatch(ctx, *execution.WarmedBatch) (endorsement.ExecutionResult, []execution.PerTxOutcome, error)`.
  - `Endorser.endorseBatch(inv, res, outcomes) (*peer.ProposalResponse, error)` helper.
  - `Endorser.WarmBatch(ctx, txs) (api.WarmedBatch, error)` and `Endorser.AuthBatch(ctx, inv, wb api.WarmedBatch) (*peer.ProposalResponse, error)`.

- [ ] **Step 1: Extend `api.Service`** (`endorser/api/service.go`)

```go
// WarmedBatch is an opaque, endorser-owned handle to a snapshot that has been
// warmed for a batch. The gateway holds it between WarmBatch and AuthBatch and
// Closes it if it abandons the batch. Its concrete type is
// *execution.WarmedBatch, but api must not import execution, so the interface
// exposes only Close.
type WarmedBatch interface {
	Close() error
}
```

Add to the `Service` interface (alongside `ExecuteBatch`):

```go
	// WarmBatch opens+warms a snapshot for txs and returns an open handle.
	WarmBatch(ctx context.Context, txs []*types.Transaction) (WarmedBatch, error)
	// AuthBatch runs the authoritative pass over a warmed handle, signs, and
	// returns the proposal response exactly as ExecuteBatch does.
	AuthBatch(ctx context.Context, inv endorsement.Invocation, wb WarmedBatch) (*peer.ProposalResponse, error)
```

- [ ] **Step 2: Extend `EVMEngineInterface`** (`endorser/core/endorser.go`)

```go
	WarmBatch(ctx context.Context, txs []*types.Transaction) (*execution.WarmedBatch, error)
	AuthMergedBatch(ctx context.Context, wb *execution.WarmedBatch) (endorsement.ExecutionResult, []execution.PerTxOutcome, error)
```

- [ ] **Step 3: Extract `endorseBatch` and add `WarmBatch`/`AuthBatch`** (`endorser/core/endorser.go`)

Refactor `ExecuteBatch`'s marshal→Event→Endorse tail into:

```go
// endorseBatch marshals the per-tx outcomes into res.Event and signs the merged
// result, returning the proposal response. Shared by ExecuteBatch and AuthBatch.
func (f *Endorser) endorseBatch(inv endorsement.Invocation, res endorsement.ExecutionResult, outcomes []execution.PerTxOutcome) (*peer.ProposalResponse, error) {
	outcomesPayload, err := json.Marshal(outcomes)
	if err != nil {
		return response(nil, fmt.Errorf("marshal batch outcomes: %w", err))
	}
	res.Event = outcomesPayload
	return f.builder.Endorse(inv, res)
}
```

Refactor `ExecuteBatch` to call it (behavior identical). Then add:

```go
// WarmBatch opens+warms a snapshot for txs via the engine and returns the open
// handle. On error it returns a nil interface (not a typed-nil) so callers can
// compare == nil.
func (f *Endorser) WarmBatch(ctx context.Context, txs []*types.Transaction) (api.WarmedBatch, error) {
	wb, err := f.Engine.WarmBatch(ctx, txs)
	if err != nil {
		return nil, err
	}
	return wb, nil
}

// AuthBatch runs the authoritative pass over the warmed handle, then signs the
// merged result exactly as ExecuteBatch does.
func (f *Endorser) AuthBatch(ctx context.Context, inv endorsement.Invocation, wb api.WarmedBatch) (*peer.ProposalResponse, error) {
	ewb, ok := wb.(*execution.WarmedBatch)
	if !ok {
		return response(nil, fmt.Errorf("auth batch: unexpected warmed-batch handle type %T", wb))
	}
	res, outcomes, err := f.Engine.AuthMergedBatch(ctx, ewb)
	if err != nil {
		return response(nil, err)
	}
	return f.endorseBatch(inv, res, outcomes)
}
```

(Match `ExecuteBatch`'s exact error-wrapping idiom — if it wraps engine errors via `response(nil, err)` without extra context, keep that; if it adds context, mirror it.)

- [ ] **Step 4: Extend the test fakes** (`endorser/core/endorser_test.go`)

Add to `stubEngine` (keeps it implementing `EVMEngineInterface`):

```go
func (s *stubEngine) WarmBatch(ctx context.Context, txs []*types.Transaction) (*execution.WarmedBatch, error) {
	if s.warmErr != nil {
		return nil, s.warmErr
	}
	return &execution.WarmedBatch{}, nil // zero-value handle; Close is a no-op on nil reader
}

func (s *stubEngine) AuthMergedBatch(ctx context.Context, wb *execution.WarmedBatch) (endorsement.ExecutionResult, []execution.PerTxOutcome, error) {
	return s.mergedRes, s.mergedOutcomes, s.mergedErr
}
```

Add `warmErr error` to the `stubEngine` struct fields.

- [ ] **Step 5: Write the failing test** — WarmBatch+AuthBatch signs the same outcomes as ExecuteBatch.

```go
func TestEndorserWarmThenAuthEndorses(t *testing.T) {
	eng := &stubEngine{
		mergedRes:      endorsement.ExecutionResult{ReadWriteSet: blocks.ReadWriteSet{}},
		mergedOutcomes: []execution.PerTxOutcome{{Status: 1, Event: []byte("ok")}},
	}
	bld := &stubBuilder{resp: &peer.ProposalResponse{Response: &peer.Response{Status: int32(cmn.StatusOK)}}}
	f := &Endorser{Engine: eng, builder: bld}

	inv := endorsement.Invocation{ /* minimal, as other endorser_test cases build it */ }
	wb, err := f.WarmBatch(context.Background(), []*types.Transaction{})
	require.NoError(t, err)
	resp, err := f.AuthBatch(context.Background(), inv, wb)
	require.NoError(t, err)
	require.Equal(t, int32(cmn.StatusOK), resp.Response.Status)

	// endorseBatch marshalled the outcomes into res.Event before signing.
	var gotOutcomes []execution.PerTxOutcome
	require.NoError(t, json.Unmarshal(bld.gotRes.Event, &gotOutcomes))
	require.Equal(t, eng.mergedOutcomes, gotOutcomes)
}
```

Reconcile `inv` construction and the `stubBuilder` field names (`resp`/`gotRes`/`gotInv`) with the real `endorser_test.go`.

- [ ] **Step 6: Run tests under race**

Run: `go test -race ./endorser/core/... ./endorser/api/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add endorser/api/service.go endorser/core/endorser.go endorser/core/endorser_test.go
git commit -m "feat(endorser): WarmBatch/AuthBatch on Service + Endorser

api.Service and Endorser gain WarmBatch (open+warm, returns opaque handle) and
AuthBatch (auth over the handle, then sign). endorseBatch extracts the shared
marshal+Endorse tail. EVMEngineInterface extended; ExecuteBatch unchanged.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: EndorsementClient fan-out

**Files:**
- Modify: `gateway/core/endorse.go`
- Test: `gateway/core/endorse_test.go`

**Interfaces:**
- Consumes: `EndorsementClient{endorsers []api.Service; signer; channel; namespace; nsVersion}`, `createInvocation(args)`, `classifyBatchOutcomes(signed, txs)`, `decodeMergedRWS(signed, namespace)`, `common.ProposalTypeEVMBatch`, `common.StatusOK`, `sdk.Endorsement`, `api.WarmedBatch`, `endorsement.Invocation`.
- Produces:
  - `type WarmedBatch struct { per []api.WarmedBatch; inv endorsement.Invocation; txs []*types.Transaction; closeOnce sync.Once }` with `Close() error` (idempotent).
  - `(e *EndorsementClient) WarmBatch(ctx, txs) (*WarmedBatch, error)`.
  - `(e *EndorsementClient) AuthBatch(ctx, wb *WarmedBatch) (sdk.Endorsement, []*types.Transaction, []*types.Transaction, blocks.ReadWriteSet, error)` — same tuple as `ExecuteBatch`.

- [ ] **Step 1: Add the gateway-level bundle** (`gateway/core/endorse.go`)

```go
// WarmedBatch bundles the per-endorser warm handles for one batch with the
// invocation and txs they were warmed for, so AuthBatch can fan the SAME
// invocation back out (one sub-handle per endorser) and merge the signed
// responses exactly as ExecuteBatch does. Handles N >= 1 endorsers (today N=1).
type WarmedBatch struct {
	per       []api.WarmedBatch
	inv       endorsement.Invocation
	txs       []*types.Transaction
	closeOnce sync.Once
}

// Close releases every per-endorser warm handle. Idempotent: AuthBatch's per-
// endorser calls each close their own snapshot, and an abandoned iteration calls
// this as a backstop; the underlying execution.WarmedBatch.Close is itself
// idempotent, so double-close is safe.
func (w *WarmedBatch) Close() error {
	w.closeOnce.Do(func() {
		for _, p := range w.per {
			if p != nil {
				_ = p.Close()
			}
		}
	})
	return nil
}
```

(Add `"sync"` to `endorse.go` imports if absent.)

- [ ] **Step 2: Add `WarmBatch`** — build the same invocation as `ExecuteBatch`, fan out.

```go
// WarmBatch opens+warms a snapshot on every endorser for txs and bundles the
// handles with the invocation ExecuteBatch would build (Args[0]=batch type,
// Args[1..]=marshaled txs), so AuthBatch signs the identical invocation. On any
// endorser error it closes whatever handles opened and returns the error.
func (e *EndorsementClient) WarmBatch(ctx context.Context, txs []*types.Transaction) (*WarmedBatch, error) {
	args := make([][]byte, 0, len(txs)+1)
	args = append(args, []byte{byte(common.ProposalTypeEVMBatch)})
	for _, tx := range txs {
		b, err := tx.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal tx: %w", err)
		}
		args = append(args, b)
	}
	inv, err := e.createInvocation(args)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	per := make([]api.WarmedBatch, len(e.endorsers))
	errs := make([]error, len(e.endorsers))
	var wg sync.WaitGroup
	run := func(index int, endorser api.Service) {
		wb, err := endorser.WarmBatch(ctx, txs)
		if err != nil {
			errs[index] = fmt.Errorf("call endorser warm: %w", err)
			cancel()
			return
		}
		per[index] = wb
	}
	for i, end := range e.endorsers {
		if len(e.endorsers) > 1 {
			wg.Add(1)
			go func(index int, endorser api.Service) { defer wg.Done(); run(index, endorser) }(i, end)
		} else {
			run(i, end)
		}
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			for _, p := range per {
				if p != nil {
					_ = p.Close()
				}
			}
			return nil, err
		}
	}
	return &WarmedBatch{per: per, inv: inv, txs: txs}, nil
}
```

(Mirror `ExecuteBatch`'s exact arg-building — if `ExecuteBatch` uses a helper instead of `MarshalBinary` inline, reuse that helper.)

- [ ] **Step 3: Add `AuthBatch`** — fan the warmed handles back out, merge like `ExecuteBatch`.

```go
// AuthBatch runs the authoritative pass on every endorser over wb's warmed
// handles, checks each signed response's status, and merges outcomes exactly as
// ExecuteBatch does. Each endorser's AuthBatch closes its own snapshot; the
// caller may still call wb.Close() as an idempotent backstop.
func (e *EndorsementClient) AuthBatch(ctx context.Context, wb *WarmedBatch) (sdk.Endorsement, []*types.Transaction, []*types.Transaction, blocks.ReadWriteSet, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := make([]*peer.ProposalResponse, len(e.endorsers))
	errs := make([]error, len(e.endorsers))
	run := func(index int, endorser api.Service) {
		pResp, err := endorser.AuthBatch(ctx, wb.inv, wb.per[index])
		if err != nil {
			errs[index] = fmt.Errorf("call endorser: %w", err)
			cancel()
			return
		}
		if pResp.Response.Status != int32(common.StatusOK) {
			errs[index] = fmt.Errorf("process EVM batch: %s", pResp.Response.Message)
			cancel()
			return
		}
		res[index] = pResp
	}
	var wg sync.WaitGroup
	for i, end := range e.endorsers {
		if len(e.endorsers) > 1 {
			wg.Add(1)
			go func(index int, endorser api.Service) { defer wg.Done(); run(index, endorser) }(i, end)
		} else {
			run(i, end)
		}
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, err
		}
	}

	var signed *peer.ProposalResponse
	for _, r := range res {
		if r != nil {
			signed = r
			break
		}
	}
	included, terminal, err := classifyBatchOutcomes(signed, wb.txs)
	if err != nil {
		return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, fmt.Errorf("decode included txs: %w", err)
	}
	mergedRWS, err := decodeMergedRWS(signed, e.namespace)
	if err != nil {
		return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, fmt.Errorf("decode merged read-write set: %w", err)
	}
	return sdk.Endorsement{Proposal: wb.inv.Proposal, Responses: res}, included, terminal, mergedRWS, nil
}
```

**Reconcile every detail with the real `ExecuteBatch`**: the `sdk.Endorsement` struct field names (`Proposal`/`Responses` may differ), the status comparison (`!= common.StatusOK` vs `!= int32(common.StatusOK)`), and the exact error strings. The returned `sdk.Endorsement` MUST be byte-identical in shape to what `ExecuteBatch` returns so `committerTxID` + `SubmitFabricTx` behave the same.

- [ ] **Step 4: Extend the test stub** (`gateway/core/endorse_test.go`)

```go
type stubWarmed struct{}

func (stubWarmed) Close() error { return nil }

func (s *stubEndorser) WarmBatch(ctx context.Context, txs []*types.Transaction) (api.WarmedBatch, error) {
	if s.warmErr != nil {
		return nil, s.warmErr
	}
	return stubWarmed{}, nil
}

func (s *stubEndorser) AuthBatch(ctx context.Context, inv endorsement.Invocation, wb api.WarmedBatch) (*peer.ProposalResponse, error) {
	s.gotInv = inv
	return s.execResp, s.execErr
}
```

Add `warmErr error` to `stubEndorser`.

- [ ] **Step 5: Write the failing test** — WarmBatch+AuthBatch classifies/merges like ExecuteBatch.

```go
func TestClientWarmThenAuthMatchesExecuteBatch(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	c := signingClient(stub)

	txs := []*types.Transaction{ /* build 2-3 txs as other endorse_test cases do */ }

	wb, err := c.WarmBatch(context.Background(), txs)
	require.NoError(t, err)
	end, included, terminal, _, err := c.AuthBatch(context.Background(), wb)
	require.NoError(t, err)
	require.Len(t, included, len(txs))
	require.Empty(t, terminal)
	require.NotNil(t, end.Proposal)
	// The invocation the endorser saw carries the batch proposal type.
	require.Equal(t, byte(common.ProposalTypeEVMBatch), stub.gotInv.Args[0][0])
}
```

Match `signingClient`, `okBatchResponse`, and tx construction to the real `endorse_test.go`. If `okBatchResponse()` yields outcomes that make some txs terminal, assert accordingly.

- [ ] **Step 6: Run tests under race**

Run: `go test -race ./gateway/core/ -run 'Client|Endorse|WarmThenAuth' -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add gateway/core/endorse.go gateway/core/endorse_test.go
git commit -m "feat(gateway): EndorsementClient WarmBatch/AuthBatch fan-out

WarmBatch fans open+warm across endorsers and bundles the handles with the
invocation; AuthBatch fans the auth pass back out and merges outcomes exactly as
ExecuteBatch. Idempotent bundle Close backstops snapshot release. N>=1 endorsers.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Gateway pipelined executor loop

**Files:**
- Modify: `gateway/core/api.go` (add `pipelined bool` + `SetPipelined`)
- Modify: `gateway/core/executor.go` (extract `submitExecuted`; add pipelined loop; dispatch)
- Test: `gateway/core/executor_test.go`

**Interfaces:**
- Consumes: `Gateway`, `g.pending` (Task 1 methods), `g.endorsers` (Task 4 `WarmBatch`/`AuthBatch`), `g.cache.DrainEvictions()`, `g.rebuildCacheFromInflight()`, `g.cache.MaintainReadOnly()`, `g.cache.ApplyWrites`, `g.cache.Read`, `g.maxBatchSize`, `g.inflightSlots`, `g.trackInflight`, `g.resolveInflight`, `committerTxID`, `g.SubmitFabricTx`, `hashesOf`, `g.waitForWork`, `g.backoff`, `g.arrivals`, `g.wg`, `sdk.Endorsement`, `blocks.ReadWriteSet`, `WarmedBatch` (Task 4).
- Produces: `(g *Gateway) SetPipelined(bool)`; `pipelined bool` field; `(g *Gateway) submitExecuted(ctx, end sdk.Endorsement, included []*types.Transaction, rws blocks.ReadWriteSet) error`; `(g *Gateway) runExecutorPipelined(ctx context.Context)`; `(g *Gateway) warmNextBatch(ctx context.Context) *WarmedBatch`; `(g *Gateway) pipelineIteration(ctx context.Context, warmed *WarmedBatch) *WarmedBatch`.

- [ ] **Step 1: Add `pipelined` field + `SetPipelined`** (`gateway/core/api.go`)

Add field to the `Gateway` struct (near `maxBatchSize`):

```go
	// pipelined selects runExecutorPipelined (warm(N+1) ‖ auth(N) overlap) over
	// the serial runExecutor. Default false. Must be set before Start -- it
	// selects the loop the executor goroutine runs. NOT the in-flight commit
	// pipeline (that is always on; see maxInflight).
	pipelined bool
```

Add the setter (near `SetMaxBatchSize`):

```go
// SetPipelined selects the barrier-pipelined executor loop, which overlaps the
// warm pass of the next batch with the authoritative pass + submit of the
// current one. Default false (serial). Call before Start.
func (g *Gateway) SetPipelined(v bool) { g.pipelined = v }
```

- [ ] **Step 2: Extract `submitExecuted`** (`gateway/core/executor.go`) — the boundary tail shared by both loops.

```go
// submitExecuted performs the boundary work for one executed batch with a non-
// empty included set: apply its writes to the cross-batch cache, capture spec
// versions, acquire an in-flight slot, track it, remove its included txs from
// pending, and submit. Shared by the serial executeCycle and pipelineIteration.
// Returns an error the caller must react to (ctx cancellation, tx-id extraction,
// or submit failure); a submit failure has already rolled the batch back.
func (g *Gateway) submitExecuted(ctx context.Context, end sdk.Endorsement, included []*types.Transaction, rws blocks.ReadWriteSet) error {
	fabricTxID, err := committerTxID(end.Proposal)
	if err != nil {
		return fmt.Errorf("extract committer tx id: %w", err)
	}

	g.cache.ApplyWrites(fabricTxID, rws)

	specVers := make(map[string]uint64, len(rws.Writes))
	for _, w := range rws.Writes {
		if rec, ok := g.cache.Read(w.Key); ok {
			specVers[w.Key] = rec.Version
		}
	}

	select {
	case g.inflightSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	g.trackInflight(fabricTxID, included, rws, specVers)
	g.pending.Remove(hashesOf(included))

	if err := g.SubmitFabricTx(ctx, end); err != nil {
		g.resolveInflight(fabricTxID, false)
		return fmt.Errorf("batch submit failed (tx %s): %w", fabricTxID, err)
	}
	return nil
}
```

**Reconcile with `executeCycle`'s real tail**: copy the exact `trackInflight` argument list, the exact `specVers`/`ApplyWrites`/`Read` calls, and the exact `resolveInflight` signature from the current code. This block must reproduce those statements verbatim.

- [ ] **Step 3: Refactor `executeCycle` to use `submitExecuted`** (behavior-preserving)

Replace the committerTxID→ApplyWrites→specVers→slot→trackInflight→Remove(included)→SubmitFabricTx sequence in `executeCycle` with:

```go
	if err := g.submitExecuted(ctx, end, included, rws); err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Errorf("submit executed batch (%d txs): %v", len(batch), err)
		g.backoff(ctx)
		return
	}
```

Keep everything before it (DrainEvictions, MaintainReadOnly, DrainUpTo, ExecuteBatch, terminal Remove, `included==0` waitForWork) exactly as-is.

- [ ] **Step 4: Add the pipelined loop, warm helper, and iteration** (`gateway/core/executor.go`)

```go
// runExecutorPipelined is the barrier-pipelined executor loop (SetPipelined
// true). It carries one fully-warmed batch across iterations (prefetch depth 1):
// each iteration launches warm(N+1) concurrently with auth(N)+submit(N), joins
// at a barrier before the boundary cache mutations, and applies them on this one
// goroutine -- preserving the "mutate caches only at the boundary, never during
// warm reads" invariant. runExecutor owns g.wg.Done, so this must NOT call it.
func (g *Gateway) runExecutorPipelined(ctx context.Context) {
	var warmed *WarmedBatch
	for ctx.Err() == nil {
		if warmed == nil {
			warmed = g.warmNextBatch(ctx)
			if warmed == nil {
				g.waitForWork(ctx)
				continue
			}
		}
		warmed = g.pipelineIteration(ctx, warmed)
	}
	// Shutdown: release any still-open warmed snapshot and its reservation.
	if warmed != nil {
		_ = warmed.Close()
		g.pending.Release(hashesOf(warmed.txs))
	}
}

// warmNextBatch drains+reserves the next batch (skipping reserved txs) and runs
// its warm pass, returning the open handle. Returns nil when there is no
// unreserved work (caller waits) or on a transient warm error (already backed
// off + self-woke so the caller retries rather than blocking forever).
func (g *Gateway) warmNextBatch(ctx context.Context) *WarmedBatch {
	txs := g.pending.DrainUpToReserved(int(g.maxBatchSize.Load()))
	if len(txs) == 0 {
		return nil
	}
	wb, err := g.endorsers.WarmBatch(ctx, txs)
	if err != nil {
		logger.Errorf("warm batch (%d txs): %v", len(txs), err)
		g.pending.Release(hashesOf(txs))
		g.backoff(ctx)
		select {
		case g.arrivals <- struct{}{}: // self-wake: txs are pending again, no external arrival will fire
		default:
		}
		return nil
	}
	return wb
}

// pipelineIteration handles the already-warmed batch N (`warmed`) while warming
// batch N+1 concurrently, then applies batch N's boundary work after the
// barrier. Returns the next warmed batch (N+1), or nil if pending was empty.
func (g *Gateway) pipelineIteration(ctx context.Context, warmed *WarmedBatch) *WarmedBatch {
	// Boundary prep at the TOP, with NO warm reads in flight (the previous
	// iteration joined its warm at the barrier): drain queued evictions and run
	// read-only maintenance before either the next warm or this auth reads the
	// cache. Mirrors executeCycle steps 1-2.
	if _, invalidated := g.cache.DrainEvictions(); len(invalidated) > 0 {
		g.rebuildCacheFromInflight()
	}
	g.cache.MaintainReadOnly()

	// (a,b) Drain+reserve batch N+1 (DrainUpToReserved skips still-reserved N)
	// and launch its warm pass. It reads the LIVE caches read-only; no mutation
	// happens until after the barrier.
	txsNext := g.pending.DrainUpToReserved(int(g.maxBatchSize.Load()))
	var warmFut chan *WarmedBatch
	if len(txsNext) > 0 {
		warmFut = make(chan *WarmedBatch, 1)
		go func() {
			wb, err := g.endorsers.WarmBatch(ctx, txsNext)
			if err != nil {
				logger.Errorf("warm next batch (%d txs): %v", len(txsNext), err)
				g.pending.Release(hashesOf(txsNext))
				warmFut <- nil
				return
			}
			warmFut <- wb
		}()
	}

	// (c) Authoritative pass over batch N, concurrent with warm(N+1). Reads the
	// LIVE caches read-only; writes only its per-batch overlay.
	end, included, terminal, rws, authErr := g.endorsers.AuthBatch(ctx, warmed)

	// (d) BARRIER: warm(N+1) fully done before any boundary cache mutation.
	var warmedNext *WarmedBatch
	if warmFut != nil {
		warmedNext = <-warmFut
	}

	// (e) Boundary on this goroutine, AFTER the barrier -- no warm reads in
	// flight. Clear batch N's reservation, close its snapshot (backstop), handle
	// its result.
	g.pending.Release(hashesOf(warmed.txs))
	_ = warmed.Close()

	if authErr != nil {
		if ctx.Err() != nil {
			return warmedNext
		}
		logger.Errorf("batch auth failed (%d txs): %v", len(warmed.txs), authErr)
		g.backoff(ctx) // batch N stays pending (Released above); re-drawn later
		return warmedNext
	}

	if len(terminal) > 0 {
		g.pending.Remove(hashesOf(terminal))
	}
	if len(included) == 0 {
		return warmedNext
	}

	if err := g.submitExecuted(ctx, end, included, rws); err != nil {
		if ctx.Err() != nil {
			return warmedNext
		}
		logger.Errorf("submit executed batch (%d txs): %v", len(warmed.txs), err)
		g.backoff(ctx)
	}
	return warmedNext
}
```

- [ ] **Step 5: Dispatch from `runExecutor`**

```go
func (g *Gateway) runExecutor(ctx context.Context) {
	defer g.wg.Done()
	if g.pipelined {
		g.runExecutorPipelined(ctx)
		return
	}
	for ctx.Err() == nil {
		g.executeCycle(ctx)
	}
}
```

(Match the exact current `runExecutor` body — if it uses a different loop condition or has extra setup, preserve it and only add the `if g.pipelined` dispatch.)

- [ ] **Step 6: Add imports** — `sdk "github.com/hyperledger/fabric-x-sdk"` to `executor.go` (for `submitExecuted`'s `sdk.Endorsement` parameter). No `sync` needed (the warm goroutine uses a plain channel). Verify the exact sdk import path/alias used elsewhere in the package.

- [ ] **Step 7: Write the failing tests** (append to `gateway/core/executor_test.go`)

```go
// runPipelineAndCapture starts the pipelined loop and returns the first
// submitted endorsement + the cancel to stop the loop.
func runPipelineAndCapture(t *testing.T, g *Gateway) (sdk.Endorsement, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go g.runExecutorPipelined(ctx)
	select {
	case end := <-g.endorsementChan:
		return end, cancel
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for the pipelined batch to be submitted")
		return sdk.Endorsement{}, cancel
	}
}

func TestPipelinedDrainSubmitsAndTracks(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetPipelined(true)
	addThreeTxs(g)

	end, cancel := runPipelineAndCapture(t, g)
	defer cancel()

	require.NotNil(t, end.Proposal)
	require.Equal(t, 1, g.inflightCount()) // one merged batch in flight
	require.Equal(t, 0, g.pending.Len())   // all three included -> removed
}

func TestPipelinedAppliesWritesBeforeCommit(t *testing.T) {
	writeKey := "acct/bal"
	stub := &stubEndorser{execResp: batchResponseWithWrites(t, map[string][]byte{writeKey: {0x2a}})}
	g := newExecutorTestGateway(stub)
	g.SetPipelined(true)
	addThreeTxs(g)

	_, cancel := runPipelineAndCapture(t, g)
	defer cancel()

	rec, ok := g.cache.Read(writeKey)
	require.True(t, ok, "ApplyWrites must run at the pipeline boundary before commit")
	require.Equal(t, []byte{0x2a}, rec.Value)
}
```

Reconcile helper names/shapes with the real `executor_test.go` (`newExecutorTestGateway`, `okBatchResponse`, `batchResponseWithWrites`, `addThreeTxs`, `inflightCount`, `g.pending.Len`, `rec.Value` field). If `batchResponseWithWrites`'s write-record shape differs, mirror it.

- [ ] **Step 8: Run tests under race**

Run: `go test -race ./gateway/core/ -run 'Pipelined|Executor' -v`
Expected: PASS (new pipelined + existing serial tests).

- [ ] **Step 9: Run the whole package under race**

Run: `go test -race ./gateway/core/...`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add gateway/core/api.go gateway/core/executor.go gateway/core/executor_test.go
git commit -m "feat(gateway): opt-in warm/auth pipelined executor loop

runExecutorPipelined overlaps warm(N+1) with auth(N)+submit(N), joining at a
barrier before boundary cache mutations (ApplyWrites/eviction/RO-maintenance) so
the single-executor-goroutine invariant holds with no lock. submitExecuted
extracts the shared boundary tail. SetPipelined (default false) selects it.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Perf `-pipeline` flag + harness wiring

**Files:**
- Modify: `gateway/config/config.go` (add `Pipelined bool` field)
- Modify: `integration/test_helpers.go` (call `SetPipelined` in pre-Start window)
- Modify: `integration/perf/replay_json_dataset_test.go` (add `-pipeline` flag → override)

**Interfaces:**
- Consumes: `config.Gateway`, `applyConfigOverrides` (dot-notation reflection), `buildTestHarnessWithExtraHandler` (contains `app.BuildGateway` at :233 and `gw.Start` at :397), `th.Gateways[0].SetMaxBatchSize` at replay_json_dataset_test.go:403, `SetPipelined` (Task 5).
- Produces: `cfg.Gateway.Pipelined bool`; `-pipeline` perf flag wired via `"Gateway.Pipelined"` override; `gw.SetPipelined(cfg.Gateway.Pipelined)` before Start.

- [ ] **Step 1: Add the config field** (`gateway/config/config.go`, in the `Gateway` struct)

```go
	Pipelined bool `mapstructure:"pipelined" yaml:"pipelined"` // enable the warm/auth execution pipeline (overlap warm(N+1) with auth(N)); default false. Distinct from the always-on in-flight commit pipeline (see max-inflight). Set via core.Gateway.SetPipelined before Start.
```

- [ ] **Step 2: Wire it in the harness pre-Start window** (`integration/test_helpers.go`)

Immediately BEFORE `gw.Start(t.Context())` (currently line 397), add:

```go
	// Select the warm/auth execution pipeline before Start (the executor
	// goroutine picks its loop at Start). Default false; perf sets it via the
	// "Gateway.Pipelined" config override.
	gw.SetPipelined(cfg.Gateway.Pipelined)
```

- [ ] **Step 3: Add the perf flag** (`integration/perf/replay_json_dataset_test.go`, near the other flags ~line 82)

```go
// pipeline enables the gateway's warm/auth execution pipeline (SetPipelined),
// overlapping the warm pass of the next batch with the authoritative pass +
// submit of the current one. Off by default (serial); the harness flips it on
// via the "Gateway.Pipelined" config override so only benchmarks exercise it.
var pipeline = flag.Bool("pipeline", false, "enable the gateway warm/auth execution pipeline")
```

- [ ] **Step 4: Pass it through the config overrides** (same file, in the `configOverrides` map passed to `NewFabricXTestHarnessWithNotifications` ~lines 389-395)

Add the entry alongside `"Gateway.SubmitterCount"` / `"Network.Namespace"`:

```go
			"Gateway.Pipelined": *pipeline,
```

(The existing `th.Gateways[0].SetMaxBatchSize(*maxBatchSize)` at :403 stays — it is atomic and post-Start-safe. `SetPipelined` must NOT be added there; it goes only through the pre-Start harness wiring in Step 2.)

- [ ] **Step 5: Build the perf tag + vet**

Run: `go build ./... && go vet -tags=perf ./integration/perf/...`
Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
git add gateway/config/config.go integration/test_helpers.go integration/perf/replay_json_dataset_test.go
git commit -m "feat(perf): -pipeline flag wiring for the warm/auth execution pipeline

Gateway.Pipelined config field + pre-Start SetPipelined in the harness; perf
-pipeline flag (default off) flows through the config-override map so benchmarks
can toggle the pipeline against the serial baseline on the same stack.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Full-suite verification (after all tasks)

- [ ] `go build ./...`
- [ ] `go test -race ./gateway/core/... ./endorser/execution/... ./endorser/core/... ./endorser/api/...`
- [ ] `go vet -tags=perf ./integration/perf/...`

## ec2 validation gates (must all pass before considering it shippable)

1. Fresh stack, **both datasets**, window 20000, batch 1024, code-default QS pool, `-pipeline`:
   - synthetic: 20000/20000 committed, 0 rolled back + throughput.
   - historic: 20000/20000 committed, 0 rolled back + throughput.
2. Control run, same stack, `-pipeline` OFF: confirm the serial baseline is unchanged; measure speedup.
3. Ship only if pipelined ≥ serial AND both datasets green. Parameter sweeps (batch size, prefetch is fixed depth-1) as needed to justify each chosen parameter in the final HTML report (both datasets shown separately, never consolidated).
