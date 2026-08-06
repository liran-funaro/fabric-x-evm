# Streaming Pipeline Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the three-phase streaming pipeline of [evm-design/optimization.md](../../../evm-design/optimization.md) — concurrent Warmup → serial Auth → ordered Submit, connected by queues — so a batch's I/O-bound state reads overlap the serial authoritative pass across batch boundaries, without the speculative-write cache that livelocks today's `-pipeline` executor.

**Architecture:** Three long-lived worker groups in `gateway/core` connected by buffered channels. The **warmup** phase forms numbered batches from the input queue and executes each batch's txs concurrently against a **committed-only** MFU cache (`WarmCache`) with a query-service nil-view pooled reader behind it, pushing the full read-write set of every tx onto the warm-batch queue. The **auth** phase is one serial worker over a lock-free MFU cache (`AuthCache`) whose entries carry a read/write state: a tx whose read keys are all in `read` state is accepted with its warmup read-write set verbatim; otherwise it is re-executed against the auth cache. Commit notifications flow back commit-notifier → warmup notification worker (refresh cache, then stamp with the next batch number) → auth worker, where a **watermark** holds each notification's write→read demotion until every batch formed before that refresh has been authed. The **submit** worker sends one committer tx at a time and waits for orderer block delivery, so commit order equals submission order.

The committed-only warmup cache is the design's answer to the unsolved trilemma in [findings §6](../findings.md): it never holds a speculative write, so there is no spec-version chain to mispredict; correctness moves into the auth phase's write-state check plus the watermark.

**Tech Stack:** Go 1.24+, `sync.Map`/`sync/atomic`, `github.com/hyperledger/fabric-x-sdk/blocks` + `/endorsement`, `github.com/hyperledger/fabric-x-common/api/committerpb`, `github.com/ethereum/go-ethereum/core/types`, standard `testing`.

---

## Global Constraints

- `evm-design/` is **READ-ONLY**. Never modify `evm-design/optimization.md`; it is the spec this plan implements. Write documentation only under `docs/evm-design-impl/`.
- **No `Co-Authored-By` trailer** on any commit in this repository.
- **Never `git push`** on branch `bft-redesign`. Commit locally only.
- Use targeted `git add <paths>` in every commit step. **Never `git add -A`** or `git add .`.
- **The serial executor stays untouched until the streaming pipeline is validated, then it goes.** Through Phase 9 it is the comparison baseline, so `runExecutor`'s `executeCycle` path stays byte-identical and every phase runs `go build ./... && go test ./gateway/... ./endorser/...` before commit. Once Task 26's measurement shows streaming ahead, Task 28 **deletes** the serial executor, the `PendingPool`, and the `VersionedCache`: there is no reason to carry two executors.
- **Use `wg.Go(func(){...})`, never `wg.Add(1)` + `go func(){ defer wg.Done() ... }()`.** Go 1.26 (this module's floor) has `sync.WaitGroup.Go`; the manual form is a hand-maintained counter with two ways to get it wrong. When converting an existing call site, delete the callee's own `defer wg.Done()` — `wg.Go` owns it, and leaving both panics with a negative counter (`runExecutor` is exactly this case, see Task 25).
- Experiment data lives **outside the repo** at `~/workspace/evm-perf-data` (`$EVM_PERF_DATA`). Never commit dataset files or raw run output.
- Every perf claim is measured on **both datasets**: `-dataset synthetic` and `-dataset historic`.
- Never edit code on the remote host. Edit locally, then sync local→remote; local/git always wins.
- Never `GOGC=off` for a long-lived service. Deploy with `GOGC=500` plus a high `GOMEMLIMIT`.
- Keep rate-limit configuration unchanged.
- Run `go test -race` on every new `gateway/core` unit test. The pre-existing `VersionedCache` RPC-query race (see [findings](../findings.md)) is on the serial path and out of scope; it must not be made worse.
- No new external dependencies.
- Every new exported symbol carries a doc comment in the style of the surrounding files (why, not what; concurrency contract stated explicitly).

## Scope

**In scope (this milestone — the happy path):**

| optimization.md section | Delivered by |
|---|---|
| Warmup: Config, Cache, TXs Worker, Batch Worker | Phases 0, 1, 4 |
| Warmup: Notification Worker (refresh → sample → stamp) | Phase 5 |
| Auth: Config, Cache, Auth Worker (both inputs, accept-as-is, re-execute) | Phases 2, 6 |
| Auth: notification-map + previous-batch-number + done-batch set watermark | Phases 3, 6 |
| Submitter: Submit Worker (register-then-submit, ordered-delivery wait) | Phase 7 |
| Submitter: Notification Worker | Phase 7 |
| Submitter: Notification Timeout (`GetTransactionStatus` adjudication) | Phase 7 |

**Deferred to a phase-2 plan (explicitly NOT built here):**

- **Rollback (optimization.md §158–177), all six steps.** In this milestone any abort — an abort notification, or an aborted status from timeout adjudication — **fails the pipeline fast**: the workers stop, the fatal error is recorded, and `Err()` surfaces it. The design's own note (§171) is that stop-the-world rollback does not preserve input order and needs revisiting for BFT; building it before the overlap speedup is validated would be building on an unmeasured base.
- **Adaptive submit batch sizing (optimization.md §124–126).** The submit batch equals the auth batch 1:1 in this milestone, so the merged endorsement the auth phase produces is submitted as-is. This keeps merge-and-sign where it already lives and removes a whole failure surface from the first measurement. High/low queue thresholds land with phase 2.
- **Multi-endorser (N > 1) warm/auth.** The accept-as-is decision is made on the gateway's own copy of the read-write set, which presumes one deterministic executor. `NewStreamPipeline` **fails fast** when `len(endorsers) != 1`. Today's deployed and test configurations are all N == 1.

**The milestone's exit criterion (Phase 9):** on a conflict-free replay of both datasets, the streaming pipeline's EVM tx/s is **higher than serial's on the same host and dataset**, with zero MVCC aborts. If it is not, the plan has produced a measurement, not a feature, and phase 2 is not started until the gap is explained.

Once that criterion is met, **Task 28 deletes the serial executor** along with the `PendingPool` and the speculative `VersionedCache`. The serial path exists in this plan to be the thing streaming is measured against, not to be kept.

## Assumptions

These fill gaps optimization.md leaves open. Each is called out where it is implemented, and each is a candidate for revision once measured.

1. **Warmup cache eviction runs on the TXs worker**, after each batch is formed, and only when the byte budget is exceeded. The design specifies MFU + a byte cap but not who sweeps. The TXs worker is serial, is already at a natural boundary, and is off the warm read path. (Task 15.)
2. **A warm result with a rejected status is always re-executed**, never accepted as-is. A nonce-gap or insufficient-funds rejection during warmup reflects pre-batch state that auth may have moved past. After re-execution: `StatusTxRejectedTerminal` → drop the tx; `StatusTxRejected` (retryable) → requeue to the **tail** of the input queue. Requeueing does not preserve input order — the same CFT reasoning the design gives for rollback (§171). (Task 19.)
3. **The commit notification payload is built at auth time, not parsed from the notification stream.** The auth cache already computes each written key's post-commit version (`readVersion + 1`, matching `VersionedCache.apply`), so the submitter stores `[]StreamKV{Key, Version, Value}` per committer tx and the commit notifier only needs the txID and its status. This is what the design means by "collect all the keys/versions/values of these txs from the read-write sets stored by the submitter" (§140), and it avoids depending on `TxNotification.NsRWS`.
4. **The accept-as-is fast path also rejects a stale read version**, not only a `write`-state key: if the auth cache holds a `read` entry for a read key at a *higher* version than the warm read recorded, the tx is re-executed. (Task 19.)

   Internal staleness cannot produce this. A write leaves the auth cache only when its commit notification demotes it, and the watermark holds that demotion until the last warm batch that could have read a pre-commit version has been authed. So a non-zero `StaleReadRejects` means **outside interference** — a writer to our namespace that is not this gateway. This milestone ignores that case (as the design does); the counter exists so that if it ever happens it shows up as a number rather than as an unexplained abort.

5. **The cache byte limits are exact, over the payload they are defined on:** key + value + version bytes, i.e. `len(key) + len(value) + 8`. Index overhead — map buckets, entry headers, the `sync.Map` internals — is deliberately **not** counted, because the limit is a budget on cached state, not a heap accounting.

6. **Two mechanism substitutions where the spec names a specific primitive.** Both are documented at their definitions:
   - The warmup phase's in-flight limit is a **buffered-channel semaphore**, not the bare atomic counter the design names. The limit has to *block* the TXs worker, which is the whole point of a limit, and a channel is that plus the counter.
   - The submit map is a **mutex-guarded map**, not a `sync.Map`. It is touched three times per *committer tx* — register, resolve, sweep — never per read, so contention is irrelevant, and the timeout sweeper has to range over it to batch every overdue txID into one status call.

## File Structure

**New — `gateway/core` (package `core`, one responsibility per file):**

| File | Responsibility |
|---|---|
| `stream_config.go` | `StreamConfig`: every knob from the design's three Config sections, with defaults and sanitization. |
| `stream_types.go` | The values that cross queue boundaries: `StreamKV`, `StreamTx`, `CommitNotification`, `WarmedBatchResult`, `submitBatch`. |
| `stream_warm_cache.go` | `WarmCache`: committed-only, `sync.Map`-indexed, MFU, byte-capped; fill / refresh / sweep. |
| `stream_auth_cache.go` | `AuthCache`: serial, unsynchronized, per-key read\|write state, write entries pinned, read half byte-capped; demotion. |
| `stream_watermark.go` | `watermark`: previous-batch-number, done-batch set, notification map, release ordering. |
| `stream_warm_worker.go` | `warmStore` read path + the TXs worker (batching, batch numbering, in-flight limit) + the batch workers. |
| `stream_warm_notifier.go` | The warmup notification worker: refresh cache → sample counter → stamp → forward. |
| `stream_auth_worker.go` | `authStore` read path + the auth worker: nested select, accept-as-is, re-execute, demotion, end-of-batch eviction. |
| `stream_submit_worker.go` | The submit worker: merge, endorse, register, submit, wait for ordered delivery. |
| `stream_commit_notifier.go` | The submit map, the commit notification worker, and `GetTransactionStatus` timeout adjudication. |
| `stream_pipeline.go` | `StreamPipeline`: dependencies, queues, `Start`/`Stop`/`Add`/`Err`. |

**New — elsewhere:**

| File | Responsibility |
|---|---|
| `endorser/query/direct.go` | `DirectReader`: nil-view query-service reads with **no** per-view memoization (the warmup/auth caches are the only coherent layers). |
| `endorser/query/txstatus.go` | `GRPCClient.GetTxStatus`: the unused `GetTransactionStatus` RPC, one call for many txIDs. |

**Modified:**

| File | Change |
|---|---|
| `endorser/api/service.go` | Add `StateReader` plus `WarmBatchOn` / `AuthTxOn` / `EndorseMerged` to `Service`; remove `WarmBatch` / `AuthBatch` / `WarmedBatch` (Phase 9). |
| `endorser/execution/executor.go` | (unchanged in Phase 0; Task 27 removes `AuthMergedBatch`'s caller.) |
| `endorser/execution/batch_executor.go` | Add `WarmBatchOn` (concurrent, caller-supplied store, **returns** results); remove `AuthMergedBatch` and the reopen fields (Phase 9). |
| `endorser/core/endorser.go` | Implement the three new `Service` methods; drop the two old ones (Phase 9). |
| `gateway/core/endorse.go` | `EndorsementClient.WarmBatchOn` / `AuthTxOn` / `EndorseMerged`; drop gateway `WarmBatch`/`AuthBatch`/`WarmedBatch` (Phase 9). |
| `gateway/core/executor.go` | Remove `runExecutorPipelined` / `drainAndWarm` / `pipelineIteration` / `warmResult` (Phase 8). |
| `gateway/core/api.go` | Route `AddPending` to the pipeline when streaming; `Start` selects it. |
| `gateway/config/config.go` | `Stream StreamConfig` block + validation. |
| `gateway/app/wiring.go` | Build the two nil-view readers, the tx-status adapter, and the pipeline; force one submitter + ordered delivery. |
| `endorser/query/view.go`, `endorser/storage/lightkvs.go`, `gateway/core/versioned_cache.go`, `gateway/core/cached_view.go` | Remove the dead reopen/inherit/capture machinery (Phase 9). |

---
## Phase 0 — Store-parameterized execution + query-service readers

The design's warmup and auth phases each execute against **their own** cache with the query service behind it. Today `EVMEngine.WarmBatch` opens its own snapshot and discards its results, and the auth pass is welded to it. Phase 0 gives the engine store-parameterized entry points and gives the gateway two non-memoizing query-service readers. Everything here is **additive**: no existing path changes behaviour.

### Task 1: Streaming config and queue value types

**Files:**
- Create: `gateway/core/stream_config.go`
- Create: `gateway/core/stream_types.go`
- Test: `gateway/core/stream_config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `StreamConfig` (all fields below), `DefaultStreamConfig() StreamConfig`, `(*StreamConfig).Sanitize()`, `StreamKV{Key string; Version uint64; Value []byte; Absent bool; IsDelete bool}`, `StreamTx{Tx *types.Transaction; Status int32; Event []byte; Reads []StreamKV; Writes []StreamKV}`, `CommitNotification{Committed []StreamKV; Batch uint64}`, `WarmedBatchResult{Batch uint64; Txs []*StreamTx}`, `submitBatch{txs []*StreamTx}`.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_config_test.go
package core

import (
	"testing"
	"time"
)

func TestDefaultStreamConfigIsUsable(t *testing.T) {
	c := DefaultStreamConfig()
	c.Sanitize()
	if c.WarmBatchSize <= 0 || c.WarmBatchTimeout <= 0 {
		t.Fatalf("warm batching unset: %+v", c)
	}
	if c.MaxInflightTxs < c.WarmBatchSize {
		t.Fatalf("MaxInflightTxs=%d must admit at least one full warm batch (%d)", c.MaxInflightTxs, c.WarmBatchSize)
	}
	if c.WarmCacheBytes <= 0 || c.AuthReadCacheBytes <= 0 {
		t.Fatalf("cache budgets unset: %+v", c)
	}
}

func TestSanitizeReplacesNonPositiveFields(t *testing.T) {
	c := StreamConfig{WarmBatchSize: -1, AuthBatchSize: 0, CommitTimeout: -time.Second}
	c.Sanitize()
	d := DefaultStreamConfig()
	if c.WarmBatchSize != d.WarmBatchSize {
		t.Errorf("WarmBatchSize = %d, want default %d", c.WarmBatchSize, d.WarmBatchSize)
	}
	if c.AuthBatchSize != d.AuthBatchSize {
		t.Errorf("AuthBatchSize = %d, want default %d", c.AuthBatchSize, d.AuthBatchSize)
	}
	if c.CommitTimeout != d.CommitTimeout {
		t.Errorf("CommitTimeout = %s, want default %s", c.CommitTimeout, d.CommitTimeout)
	}
}

func TestSanitizeRaisesInflightToOneBatch(t *testing.T) {
	c := DefaultStreamConfig()
	c.MaxInflightTxs = 1
	c.WarmBatchSize = 64
	c.Sanitize()
	if c.MaxInflightTxs != 64 {
		t.Fatalf("MaxInflightTxs = %d, want it raised to one warm batch (64) so the TXs worker cannot deadlock", c.MaxInflightTxs)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestDefaultStreamConfigIsUsable -v`
Expected: FAIL — `undefined: DefaultStreamConfig`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_config.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import "time"

// Streaming-pipeline defaults. Batch sizes and timeouts are starting points for
// a parameter sweep (evm-design/optimization.md: "Batch size and timeout should
// be decided using parameters sweep"), not tuned values.
//
// MaxInflightTxs is deliberately large: the warm phase is I/O-bound on the query
// service, so its workers are parked on network reads rather than burning CPU,
// and the design wants maximal parallelism until a resource is actually
// exhausted. Lower it if memory or query-service load becomes the limit.
const (
	DefaultWarmBatchSize      = 128
	DefaultWarmBatchTimeout   = 5 * time.Millisecond
	DefaultWarmQueueSize      = 65536
	DefaultWarmBatchQueueSize = 64
	DefaultMaxInflightTxs     = 8192
	DefaultWarmCacheBytes     = 512 << 20 // 512 MiB
	DefaultAuthBatchSize      = 128
	DefaultAuthBatchTimeout   = 5 * time.Millisecond
	DefaultAuthReadCacheBytes = 256 << 20 // 256 MiB
	DefaultSubmitQueueSize    = 64
	DefaultNotifyQueueSize    = 1024
	DefaultStreamCommitTimeout = 60 * time.Second
)

// StreamConfig is the streaming pipeline's full knob set, one field per line of
// the three Config sections in evm-design/optimization.md. It is read once at
// Start and never mutated afterwards, so no field needs synchronization.
type StreamConfig struct {
	// Warmup phase.
	WarmBatchSize      int           // txs per warm batch
	WarmBatchTimeout   time.Duration // flush a short batch after this long
	WarmQueueSize      int           // input queue capacity, in txs
	WarmBatchQueueSize int           // warm -> auth batch queue capacity, in batches
	MaxInflightTxs     int           // txs concurrently inside the warm phase
	WarmCacheBytes     int64         // warmup cache budget

	// Auth phase.
	AuthBatchSize      int
	AuthBatchTimeout   time.Duration
	AuthReadCacheBytes int64 // auth cache READ half only; write entries are pinned

	// Submit phase.
	SubmitQueueSize int           // auth -> submit queue capacity, in batches
	NotifyQueueSize int           // commit-notifier -> warm -> auth notification queues
	CommitTimeout   time.Duration // per-committer-tx notification timeout
}

// DefaultStreamConfig returns the documented defaults.
func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		WarmBatchSize:      DefaultWarmBatchSize,
		WarmBatchTimeout:   DefaultWarmBatchTimeout,
		WarmQueueSize:      DefaultWarmQueueSize,
		WarmBatchQueueSize: DefaultWarmBatchQueueSize,
		MaxInflightTxs:     DefaultMaxInflightTxs,
		WarmCacheBytes:     DefaultWarmCacheBytes,
		AuthBatchSize:      DefaultAuthBatchSize,
		AuthBatchTimeout:   DefaultAuthBatchTimeout,
		AuthReadCacheBytes: DefaultAuthReadCacheBytes,
		SubmitQueueSize:    DefaultSubmitQueueSize,
		NotifyQueueSize:    DefaultNotifyQueueSize,
		CommitTimeout:      DefaultStreamCommitTimeout,
	}
}

// Sanitize replaces every non-positive field with its default and raises
// MaxInflightTxs to at least one full warm batch. The latter is a liveness
// requirement, not a preference: the TXs worker reserves WarmBatchSize slots
// before dispatching a batch, so a smaller limit would block forever.
func (c *StreamConfig) Sanitize() {
	d := DefaultStreamConfig()
	if c.WarmBatchSize <= 0 {
		c.WarmBatchSize = d.WarmBatchSize
	}
	if c.WarmBatchTimeout <= 0 {
		c.WarmBatchTimeout = d.WarmBatchTimeout
	}
	if c.WarmQueueSize <= 0 {
		c.WarmQueueSize = d.WarmQueueSize
	}
	if c.WarmBatchQueueSize <= 0 {
		c.WarmBatchQueueSize = d.WarmBatchQueueSize
	}
	if c.WarmCacheBytes <= 0 {
		c.WarmCacheBytes = d.WarmCacheBytes
	}
	if c.AuthBatchSize <= 0 {
		c.AuthBatchSize = d.AuthBatchSize
	}
	if c.AuthBatchTimeout <= 0 {
		c.AuthBatchTimeout = d.AuthBatchTimeout
	}
	if c.AuthReadCacheBytes <= 0 {
		c.AuthReadCacheBytes = d.AuthReadCacheBytes
	}
	if c.SubmitQueueSize <= 0 {
		c.SubmitQueueSize = d.SubmitQueueSize
	}
	if c.NotifyQueueSize <= 0 {
		c.NotifyQueueSize = d.NotifyQueueSize
	}
	if c.CommitTimeout <= 0 {
		c.CommitTimeout = d.CommitTimeout
	}
	if c.MaxInflightTxs < c.WarmBatchSize {
		c.MaxInflightTxs = c.WarmBatchSize
	}
}
```

```go
// gateway/core/stream_types.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// StreamKV is one key a tx touched, with everything about it: the full key, the
// full value, and the version. Read sets and write sets are both made of these,
// and so is a commit notification's payload.
//
// Carrying the value everywhere, not just the version, is what keeps every stage
// off the caches for lookups. The warmup refresh needs the VALUE (attaching a
// fresh version to a stale value would pass MVCC validation and commit a wrong
// result); the auth demotion needs only the VERSION; a re-execution's prefill
// needs both. All three read them from a StreamKV that has been in memory since
// warmup.
type StreamKV struct {
	Key     string
	Version uint64
	Value   []byte
	// Absent marks a key that had no committed value when it was read. Version is
	// then meaningless and the committer records the read with a nil MVCC version.
	Absent bool
	// IsDelete marks a write that removes the key.
	IsDelete bool
}

// StreamTx is the pipeline's internal transaction: one EVM tx plus its COMPLETE
// read-write set. The warmup phase creates it, each later stage modifies it in
// place (the auth phase may replace its read-write set wholesale by re-executing),
// and the submitter holds it until the committer tx carrying it commits.
//
// This is the single value that moves between stages, and it is deliberately NOT
// a committer tx: the committer read-write set carries read versions but no read
// values and no write versions, which is not enough to prefill a re-execution or
// to refresh the warmup cache. The submitter converts to the committer form once,
// at aggregation.
//
// The consequence worth stating: no stage ever asks a cache what a value was. The
// values are in memory, owned by the tx, until it commits.
type StreamTx struct {
	Tx     *types.Transaction
	Status int32  // endorsement outcome: OK, revert, exec failure, or a rejection
	Event  []byte // the per-tx event payload the merged endorsement carries
	Reads  []StreamKV
	Writes []StreamKV
}

// CommitNotification travels commit-notifier -> warmup notification worker ->
// auth worker. The commit notifier fills Committed; the warmup notification
// worker refreshes its cache from it and only THEN stamps Batch with the number
// the TXs worker will assign to the next batch it forms. That order is what
// makes the auth watermark sound (see runWarmNotifier).
type CommitNotification struct {
	Committed []StreamKV
	Batch     uint64
}

// WarmedBatchResult is one finished warm batch on the warm->auth queue: its batch
// number and its txs in batch order, each carrying its complete read-write set.
type WarmedBatchResult struct {
	Batch uint64
	Txs   []*StreamTx
}

// submitBatch is one committer tx's worth of settled txs, still in internal form.
// The submitter -- not the auth phase -- aggregates them into a committer
// read-write set and derives the per-key committed payload (see
// submitPhase.aggregate).
type submitBatch struct {
	txs []*StreamTx
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestDefaultStreamConfig|TestSanitize' -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_config.go gateway/core/stream_types.go gateway/core/stream_config_test.go
git commit -m "feat(stream): streaming pipeline config and queue value types"
```

---

### Task 2: `EVMEngine.WarmBatchOn` — concurrent execution against a caller-supplied store

**Files:**
- Modify: `endorser/execution/batch_executor.go` (add after `WarmBatch`, around line 297)
- Test: `endorser/execution/batch_executor_test.go` (append)

**Interfaces:**
- Consumes: `ReadStore`, `EVMEngine.newReusableExecutor`, `EVMEngine.newState`, `EVMEngine.classify`, `EVMEngine.runOn`, `excludedResult` — all already in this package.
- Produces:
  - `type ExecutedTx struct { Result endorsement.ExecutionResult; Reads []blocks.WriteRecord }`
  - `type captureReader` — a per-tx pass-through `ReadStore` that records the records it serves
  - `func (e *EVMEngine) WarmBatchOn(ctx context.Context, txs []*types.Transaction, store ReadStore) ([]ExecutedTx, error)` — one slot per input tx, index = sub-index

**Why `ExecutedTx` and not just the result.** `blocks.KVRead` carries a key and a version but **no value**, so a tx's read *values* cannot be recovered from `ExecutionResult` at all. The streaming pipeline needs them: its internal `StreamTx` carries the complete read-write set precisely so that no later stage has to ask a cache what a value was. `captureReader` records them on the way through, one instance per tx — so it needs no synchronization even though the warm pass runs txs concurrently.

- [ ] **Step 1: Write the failing test**

```go
// endorser/execution/batch_executor_test.go (append)

// TestWarmBatchOnReturnsFullReadWriteSets is the streaming pipeline's warm-phase
// contract: every tx runs CONCURRENTLY against the caller's store (no snapshot is
// opened here) and its FULL read-write set comes back. The old WarmBatch discarded
// results; the streaming auth phase decides per tx whether to accept this set
// verbatim, so it must be complete.
func TestWarmBatchOnReturnsFullReadWriteSets(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:warm_batch_on?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	keyA, addrA := newTestKey(t)
	keyB, addrB := newTestKey(t)
	_, addrC := newTestKey(t)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: 1_000, addrB: 1_000})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	store, err := kvs.NewSnapshot(0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Independent txs: A->C and B->C. Concurrent warm execution must not make
	// either observe the other (each runs on its own StateDB over the same store).
	tx1 := newTransferTx(t, cfg.ChainConfig, keyA, addrC, big.NewInt(100), 0)
	tx2 := newTransferTx(t, cfg.ChainConfig, keyB, addrC, big.NewInt(200), 0)

	results, err := engine.WarmBatchOn(context.Background(), []*types.Transaction{tx1, tx2}, store)
	if err != nil {
		t.Fatalf("WarmBatchOn: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want one slot per input tx (2)", len(results))
	}
	for i, ex := range results {
		if ex.Result.Status != 200 {
			t.Errorf("tx%d: status = %d (%q), want 200", i, ex.Result.Status, ex.Result.Message)
		}
		if len(ex.Result.RWS.Reads) == 0 {
			t.Errorf("tx%d: RWS.Reads is empty; warm must return the full read set", i)
		}
		if len(ex.Result.RWS.Writes) == 0 {
			t.Errorf("tx%d: RWS.Writes is empty; warm must return the full write set", i)
		}
		if len(ex.Reads) == 0 {
			t.Errorf("tx%d: Reads is empty; warm must return the read VALUES too -- blocks.KVRead has none", i)
		}
	}
	// Each sender's own balance read must come back with both a version and a
	// value: the version is what the committer validates, and the value is what a
	// later re-execution is prefilled from.
	balKey := accKey(addrA, "bal")
	var sawVersionedRead bool
	for _, rd := range results[0].Result.RWS.Reads {
		if rd.Key == balKey && rd.Version != nil {
			sawVersionedRead = true
		}
	}
	if !sawVersionedRead {
		t.Errorf("tx0's read of A's balance must carry an MVCC version; reads = %+v", results[0].Result.RWS.Reads)
	}
	var sawReadValue bool
	for _, rec := range results[0].Reads {
		if rec.Key == balKey && len(rec.Value) > 0 {
			sawReadValue = true
		}
	}
	if !sawReadValue {
		t.Errorf("tx0's captured reads must include A's balance WITH its value; reads = %+v", results[0].Reads)
	}
}

// TestWarmBatchOnExcludesRejectedTxWithoutFailingBatch mirrors the authoritative
// pass: a tx rejected before execution (nonce gap) gets a sentinel slot so the
// caller still sees one result per input tx and can classify the exclusion.
func TestWarmBatchOnExcludesRejectedTxWithoutFailingBatch(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:warm_batch_on_reject?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	keyA, addrA := newTestKey(t)
	_, addrC := newTestKey(t)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: 1_000})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)
	store, err := kvs.NewSnapshot(0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ok := newTransferTx(t, cfg.ChainConfig, keyA, addrC, big.NewInt(10), 0)
	gap := newTransferTx(t, cfg.ChainConfig, keyA, addrC, big.NewInt(10), 7) // nonce too high

	results, err := engine.WarmBatchOn(context.Background(), []*types.Transaction{ok, gap}, store)
	if err != nil {
		t.Fatalf("a client rejection must not fail the batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].Result.Status != 200 {
		t.Errorf("ok tx: status = %d, want 200", results[0].Result.Status)
	}
	if results[1].Result.Status != common.StatusTxRejected {
		t.Errorf("nonce-gap tx: status = %d, want StatusTxRejected (%d)", results[1].Result.Status, common.StatusTxRejected)
	}
	if len(results[1].Result.RWS.Writes) != 0 {
		t.Errorf("an excluded tx must carry an empty RWS; got %+v", results[1].Result.RWS)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/execution/ -run TestWarmBatchOn -v`
Expected: FAIL — `engine.WarmBatchOn undefined (type *EVMEngine has no field or method WarmBatchOn)`.

- [ ] **Step 3: Write minimal implementation**

```go
// endorser/execution/batch_executor.go (insert after WarmBatch)

// WarmBatchOn runs the concurrent warm pass of a batch against a store the
// CALLER owns and returns one full ExecutionResult per tx (index = sub-index).
// It is the streaming pipeline's warm phase (see evm-design/optimization.md
// "Batch Worker"), and it differs from WarmBatch in the two ways that phase
// needs:
//
//   - It opens no snapshot. The store is the pipeline's committed-only warmup
//     cache over a nil-view query-service reader, shared by every warm batch, so
//     there is no per-batch view to open, pin, or close. store MUST be safe for
//     concurrent Get; its lifecycle belongs to the caller.
//   - It discards nothing. The auth phase decides per tx whether to accept this
//     read-write set verbatim or re-execute, and a discarded result makes both
//     impossible. Reads carry their MVCC versions, writes their values.
//
// Concurrency is one worker per tx (capped by EVMConfig.WarmWorkers), each with
// its own StateDB, drawing from a work-stealing atomic index so a free worker
// picks up a slow one's remaining txs. Reads block on query-service round-trips,
// so the count is not tied to GOMAXPROCS.
//
// A tx the EVM rejects before execution (nonce gap, bad signature, insufficient
// funds) gets excludedResult(rej) in its slot rather than failing the batch,
// exactly as the authoritative pass does. Only a genuine server-side fault (or a
// panic, which is recovered and converted) returns an error.
func (e *EVMEngine) WarmBatchOn(ctx context.Context, txs []*types.Transaction, store ReadStore) ([]ExecutedTx, error) {
	if len(txs) == 0 {
		return nil, nil
	}

	out := make([]ExecutedTx, len(txs))
	errs := make([]error, len(txs)) // indexed -- deterministic error order
	fast := e.stateDecorator == nil && !e.evmConfig.DebugLogs

	workers := e.evmConfig.WarmWorkers
	if workers <= 0 || workers > len(txs) {
		workers = len(txs)
	}

	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			// Fast path: one StateDB+Executor per worker, reset in place between
			// txs. Each worker owns its own, so concurrent execution never shares a
			// journal. Built lazily so a construction failure is attributable to a
			// real tx index rather than lost in a silent return.
			var sdb *StateDB
			var ex *Executor
			for {
				i := int(next.Add(1)) - 1
				if i >= len(txs) {
					return
				}
				// One capture per TX: it records the records this tx read, and being
				// per-tx it needs no synchronization despite the concurrent pass.
				capt := &captureReader{under: store}
				if fast {
					var err error
					if sdb, ex, err = e.newReusableExecutor(capt); err != nil {
						errs[i] = err
						continue
					}
				}
				// A warm-pass panic must never crash the endorser, but it must not
				// silently yield a zero-value result either: convert it to an error
				// for this slot and rebuild the reusable state, which the panic may
				// have left inconsistent.
				func() {
					defer func() {
						if r := recover(); r != nil {
							errs[i] = fmt.Errorf("warm tx %d: %v", i, r)
							sdb, ex = nil, nil
						}
					}()
					var res endorsement.ExecutionResult
					var err error
					if fast {
						sdb.reset(capt)
						res, err = e.classify(ex, sdb, txs[i])
					} else {
						var state ExtendedStateDB
						if state, err = e.newState(capt); err == nil {
							res, err = e.runOn(state, txs[i])
						}
					}
					if err != nil {
						if rej, ok := errors.AsType[*TxRejected](err); ok {
							out[i] = ExecutedTx{Result: excludedResult(rej)}
							return
						}
						errs[i] = err
						return
					}
					out[i] = ExecutedTx{Result: res, Reads: capt.reads}
				}()
			}
		})
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ExecutedTx is one tx's execution result plus every committed record it READ.
//
// The reads are a separate field because they cannot be recovered from the result:
// blocks.KVRead carries a key and a version, never a value. The streaming
// pipeline's internal transaction carries the complete read-write set -- values
// included -- so that no later stage has to ask a cache what a value was, and this
// is where those values come from.
//
// Reads holds one record per key that had a committed value when the tx read it,
// first read wins. A key read while absent is not here; it appears in
// Result.RWS.Reads with a nil version.
type ExecutedTx struct {
	Result endorsement.ExecutionResult
	Reads  []blocks.WriteRecord
}

// captureReader is a pass-through ReadStore that records the records it serves.
// ONE PER TX -- which is what lets it hold an unsynchronized map and slice while
// the warm pass runs every tx of a batch concurrently. Absent keys are not
// recorded: there is no value to carry.
type captureReader struct {
	under ReadStore
	seen  map[string]struct{}
	reads []blocks.WriteRecord
}

func (c *captureReader) Get(namespace, key string) (*blocks.WriteRecord, error) {
	rec, err := c.under.Get(namespace, key)
	if err != nil || rec == nil {
		return rec, err
	}
	if _, dup := c.seen[key]; !dup {
		if c.seen == nil {
			c.seen = make(map[string]struct{}, 8)
		}
		c.seen[key] = struct{}{}
		c.reads = append(c.reads, *rec)
	}
	return rec, nil
}

// Close is a no-op: the wrapped store's lifecycle belongs to the caller.
func (c *captureReader) Close() error { return nil }

var _ ReadStore = (*captureReader)(nil)
```

Add `"fmt"` to the file's imports if it is not already there. 

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/execution/ -run TestWarmBatchOn -race -v && go test ./endorser/... `
Expected: PASS (both new tests, and the whole package still green).

- [ ] **Step 5: Commit**

```bash
git add endorser/execution/batch_executor.go endorser/execution/batch_executor_test.go
git commit -m "feat(exec): WarmBatchOn -- concurrent warm pass over a caller-supplied store"
```

---

### Task 3: `EVMEngine.AuthTxOn`

**Files:**
- Modify: `endorser/execution/batch_executor.go` (add `AuthTxOn` after `WarmBatchOn`)
- Test: `endorser/execution/batch_executor_test.go` (append)

**Interfaces:**
- Consumes: `WarmBatchOn` (Task 2) for the test's setup; `ReadStore`.
- Produces:
  - `func (e *EVMEngine) AuthTxOn(ctx context.Context, tx *types.Transaction, store ReadStore) (ExecutedTx, error)` — one tx, serially, against the caller's store, with its read records captured the same way `WarmBatchOn` captures them (a re-executed tx replaces its `StreamTx`'s read-write set wholesale, values included).


- [ ] **Step 1: Write the failing test**

```go
// endorser/execution/batch_executor_test.go (append)

// TestAuthTxOnSeesStoreWritesFromEarlierTx is the streaming auth phase's
// contract: the caller (the pipeline's auth worker) owns the store and layers
// earlier txs' writes into it, so a re-executed tx observes them. Here the store
// is a plain overlayReader standing in for the pipeline's auth cache.
func TestAuthTxOnSeesStoreWritesFromEarlierTx(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:auth_tx_on?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	keyA, addrA := newTestKey(t)
	keyB, addrB := newTestKey(t)
	_, addrC := newTestKey(t)
	// B alone cannot afford 100; it can only after tx1 delivers 300.
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: 1_000, addrB: 50})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)
	base, err := kvs.NewSnapshot(0)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	store := &overlayReader{under: base, writes: map[string]*blocks.WriteRecord{}}

	tx1 := newTransferTx(t, cfg.ChainConfig, keyA, addrB, big.NewInt(300), 0)
	ex1, err := engine.AuthTxOn(context.Background(), tx1, store)
	if err != nil {
		t.Fatalf("AuthTxOn tx1: %v", err)
	}
	if ex1.Result.Status != 200 {
		t.Fatalf("tx1: status = %d (%q), want 200", ex1.Result.Status, ex1.Result.Message)
	}
	if len(ex1.Reads) == 0 {
		t.Error("AuthTxOn must capture the read records, values included")
	}
	store.apply(ex1.Result.RWS)

	tx2 := newTransferTx(t, cfg.ChainConfig, keyB, addrC, big.NewInt(100), 0)
	ex2, err := engine.AuthTxOn(context.Background(), tx2, store)
	if err != nil {
		t.Fatalf("AuthTxOn tx2: %v", err)
	}
	if ex2.Result.Status != 200 {
		t.Fatalf("tx2: status = %d (%q), want 200 -- it must observe tx1's write through the store", ex2.Result.Status, ex2.Result.Message)
	}
}

// TestAuthTxOnRejectionIsExcludedNotAnError keeps AuthTxOn's failure model
// identical to the authoritative pass's: a client rejection is a sentinel
// result, not a Go error, so the pipeline can classify it.
func TestAuthTxOnRejectionIsExcludedNotAnError(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:auth_tx_on_reject?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	keyA, addrA := newTestKey(t)
	_, addrC := newTestKey(t)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: 1_000})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)
	store, err := kvs.NewSnapshot(0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	gap := newTransferTx(t, cfg.ChainConfig, keyA, addrC, big.NewInt(10), 9)
	ex, err := engine.AuthTxOn(context.Background(), gap, store)
	if err != nil {
		t.Fatalf("a client rejection must not be a Go error: %v", err)
	}
	if ex.Result.Status != common.StatusTxRejected {
		t.Fatalf("status = %d, want StatusTxRejected (%d)", ex.Result.Status, common.StatusTxRejected)
	}
}

```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/execution/ -run TestAuthTxOn -v`
Expected: FAIL — `engine.AuthTxOn undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// endorser/execution/batch_executor.go (insert after WarmBatchOn)

// AuthTxOn runs ONE tx authoritatively against a store the caller owns, and is
// the streaming pipeline's re-execution path (evm-design/optimization.md, Auth
// Worker: "it is re executed using the auth phase cache, using the query service
// as a fallback"). The caller is a single serial worker that layers each tx's
// writes into the store before the next tx, so this method needs no batching, no
// overlay of its own, and no snapshot lifecycle.
//
// Its failure model matches the authoritative pass exactly: a client rejection
// comes back as excludedResult(rej), not a Go error, so the caller can tell a
// terminal exclusion (evict) from a retryable one (requeue). Only a server-side
// fault returns an error.
func (e *EVMEngine) AuthTxOn(ctx context.Context, tx *types.Transaction, store ReadStore) (ExecutedTx, error) {
	// Capture this re-execution's reads with their values: the caller replaces the
	// tx's whole read-write set with what comes back, and a set without read values
	// could not prefill a later re-execution.
	capt := &captureReader{under: store}
	var res endorsement.ExecutionResult
	var err error
	if e.stateDecorator == nil && !e.evmConfig.DebugLogs {
		res, err = e.executeReusing(capt, tx)
	} else {
		var state ExtendedStateDB
		if state, err = e.newState(capt); err == nil {
			res, err = e.runOn(state, tx)
		}
	}
	if err != nil {
		if rej, ok := errors.AsType[*TxRejected](err); ok {
			return ExecutedTx{Result: excludedResult(rej)}, nil
		}
		return ExecutedTx{}, err
	}
	return ExecutedTx{Result: res, Reads: capt.reads}, nil
}
```

`mergeOutcomes` stays private and unchanged. The streaming pipeline does not use it: the submitter aggregates the read-write set itself from the internal transactions (Task 21), which is the only place that knows which txs share a committer tx.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/... -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add endorser/execution/batch_executor.go endorser/execution/batch_executor_test.go
git commit -m "feat(exec): AuthTxOn for single-tx re-execution against a caller store"
```

---

### Task 4: Endorser service contract — `StateReader`, `WarmBatchOn`, `AuthTxOn`, `EndorseMerged`

**Files:**
- Modify: `endorser/api/service.go` (add `StateReader` + three methods to `Service`)
- Modify: `endorser/core/endorser.go` (implement them)
- Test: `endorser/core/endorser_test.go` (append; extend `stubEngine` with the new methods)

**Interfaces:**
- Consumes: `execution.EVMEngine.WarmBatchOn` / `AuthTxOn` (Tasks 2–3); the existing `endorsement.Builder`.
- Produces, on `api.Service`:
  - `type StateReader interface { Get(namespace, key string) (*blocks.WriteRecord, error); Close() error }`
  - `WarmBatchOn(ctx context.Context, txs []*types.Transaction, store StateReader) ([]execution.ExecutedTx, error)`
  - `AuthTxOn(ctx context.Context, tx *types.Transaction, store StateReader) (execution.ExecutedTx, error)`
  - `EndorseMerged(inv endorsement.Invocation, merged endorsement.ExecutionResult) (*peer.ProposalResponse, error)`
- Also on `core.EVMEngineInterface`: the same first two methods.

- [ ] **Step 1: Write the failing test**

```go
// endorser/core/endorser_test.go (append)

// TestEndorseMergedSignsWhatTheCallerBuilt is the streaming pipeline's signing
// seam: the submitter aggregates the read-write set and folds the per-tx outcomes
// into Event, and the endorser signs exactly that, unmodified.
func TestEndorseMergedSignsWhatTheCallerBuilt(t *testing.T) {
	builder := &stubBuilder{}
	end, err := New(nil, builder)
	if err != nil {
		t.Fatal(err)
	}

	merged := endorsement.ExecutionResult{
		Status: 200,
		Event:  []byte(`[{"Status":200}]`),
		RWS: blocks.ReadWriteSet{
			Reads:  []blocks.KVRead{{Key: "a", Version: &blocks.Version{BlockNum: 3}}},
			Writes: []blocks.KVWrite{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}},
		},
	}
	resp, err := end.EndorseMerged(endorsement.Invocation{}, merged)
	if err != nil {
		t.Fatalf("EndorseMerged: %v", err)
	}
	if resp == nil {
		t.Fatal("EndorseMerged returned a nil response")
	}
	if got := len(builder.lastResult.RWS.Writes); got != 2 {
		t.Errorf("signed RWS has %d writes, want the 2 it was handed", got)
	}
	if string(builder.lastResult.Event) != `[{"Status":200}]` {
		t.Errorf("Event = %q, want it passed through untouched", builder.lastResult.Event)
	}
}

// A merge with no writes would become a committer tx the committer rejects as
// MALFORMED_NO_WRITES after a full round-trip. Catch it here instead.
func TestEndorseMergedRejectsAWriteLessMerge(t *testing.T) {
	end, err := New(nil, &stubBuilder{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := end.EndorseMerged(endorsement.Invocation{}, endorsement.ExecutionResult{}); err == nil {
		t.Fatal("EndorseMerged must reject a read-write set with no writes")
	}
}
```

`stubBuilder` already exists at `endorser/core/endorser_test.go:135`; add a `lastResult endorsement.ExecutionResult` field to it and record the argument in `Endorse`. `stubEngine` (line 98) must gain the two new `EVMEngineInterface` methods; both may return zero values since these tests do not execute.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/core/ -run TestEndorseMerged -v`
Expected: FAIL — `end.EndorseMerged undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// endorser/api/service.go -- add near the top of the type declarations

// StateReader is the read side a caller supplies for a store-parameterized
// warm or auth pass. It is structurally identical to execution.ReadStore, and
// declared here so this package stays independent of the execution package (see
// the package doc): an execution.ReadStore satisfies it without conversion.
//
// Implementations passed to WarmBatchOn MUST be safe for concurrent Get.
type StateReader interface {
	Get(namespace, key string) (*blocks.WriteRecord, error)
	Close() error
}
```

```go
// endorser/api/service.go -- add to the Service interface, replacing the
// WarmBatch/AuthBatch pair (which Phase 9 deletes; both may coexist until then)

	// WarmBatchOn executes every tx of a batch CONCURRENTLY against store and
	// returns one full read-write set per tx, index = sub-index. It opens no
	// snapshot and signs nothing: the caller owns store (in the streaming
	// pipeline, a committed-only cache over a nil-view query-service reader) and
	// decides per tx whether to accept the result verbatim or re-execute it.
	WarmBatchOn(ctx context.Context, txs []*types.Transaction, store StateReader) ([]execution.ExecutedTx, error)

	// AuthTxOn executes ONE tx against store. The caller is a single serial
	// worker that layers earlier txs' writes into store before calling again, so
	// there is no batching or overlay here. A client rejection rides in the
	// result's Status; only a server fault is an error.
	AuthTxOn(ctx context.Context, tx *types.Transaction, store StateReader) (execution.ExecutedTx, error)

	// EndorseMerged signs one already-merged execution result. The caller owns the
	// merge -- it aggregated the read-write set and folded the per-tx outcomes into
	// merged.Event itself -- so this is a pure signing step. A batch signed this way
	// is indistinguishable on the wire from one signed by ExecuteBatch.
	EndorseMerged(inv endorsement.Invocation, merged endorsement.ExecutionResult) (*peer.ProposalResponse, error)
```

Add `"github.com/hyperledger/fabric-x-sdk/blocks"` to `endorser/api/service.go`'s imports.

```go
// endorser/core/endorser.go -- add after AuthBatch

// WarmBatchOn forwards the streaming pipeline's concurrent warm pass to the
// engine. It produces no endorsement: signing belongs to EndorseMerged, once
// the caller has decided which results survive.
func (f *Endorser) WarmBatchOn(ctx context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error) {
	return f.Engine.WarmBatchOn(ctx, txs, store)
}

// AuthTxOn forwards a single-tx re-execution to the engine.
func (f *Endorser) AuthTxOn(ctx context.Context, tx *types.Transaction, store api.StateReader) (execution.ExecutedTx, error) {
	return f.Engine.AuthTxOn(ctx, tx, store)
}

// EndorseMerged signs an already-merged execution result. The caller aggregated
// the read-write set and folded the per-tx outcomes into merged.Event, so nothing
// is left to decide here -- which is the point: the merge belongs to whoever chose
// which txs go into this committer tx, and that is the submitter.
//
// A merge with no writes is rejected: the committer would reject it as
// MALFORMED_NO_WRITES after a full round-trip.
func (f *Endorser) EndorseMerged(inv endorsement.Invocation, merged endorsement.ExecutionResult) (*peer.ProposalResponse, error) {
	if len(merged.RWS.Writes) == 0 {
		return nil, fmt.Errorf("endorse merged: read-write set has no writes")
	}
	resp, err := f.builder.Endorse(inv, merged)
	if err != nil {
		return response(nil, fmt.Errorf("endorse merged: %w", err)), nil
	}
	return resp, nil
}
```

Add the two new methods to `EVMEngineInterface` (`endorser/core/endorser.go:35`), typed with `execution.ReadStore` so `*execution.EVMEngine` satisfies it unchanged:

```go
	WarmBatchOn(ctx context.Context, txs []*types.Transaction, store execution.ReadStore) ([]execution.ExecutedTx, error)
	AuthTxOn(ctx context.Context, tx *types.Transaction, store execution.ReadStore) (execution.ExecutedTx, error)
```

**No adapter is needed at the call site.** `api.StateReader` and `execution.ReadStore` declare the identical method set, and Go permits assigning one interface value to another whose method set it contains — so `f.Engine.WarmBatchOn(ctx, txs, store)` with `store api.StateReader` compiles directly. That is the whole reason `StateReader` is declared in `api` rather than importing `execution` there: two identical declarations cost nothing and keep the published contract free of the implementation package.

- [ ] **Step 3b: Verify the interface assignment compiles rather than assuming it**

Run: `go build ./endorser/...`
Expected: success. If the compiler rejects the assignment, the two interfaces have drifted — diff `api.StateReader` against `execution.ReadStore` and make them identical again rather than adding a wrapper type.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/... -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add endorser/api/service.go endorser/core/endorser.go endorser/core/endorser_test.go
git commit -m "feat(endorser): store-parameterized warm/auth plus EndorseMerged signing seam"
```

---

### Task 5: `EndorsementClient` pass-throughs and the single-endorser guard

**Files:**
- Modify: `gateway/core/endorse.go` (add after `AuthBatch`, line 250)
- Test: `gateway/core/endorse_test.go` (create if absent; otherwise append)

**Interfaces:**
- Consumes: `api.Service.WarmBatchOn` / `AuthTxOn` / `EndorseMerged` (Task 4); the existing `createInvocation`, `fanOut`, `statusOnly`.
- Produces:
  - `func (e *EndorsementClient) WarmBatchOn(ctx context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error)`
  - `func (e *EndorsementClient) AuthTxOn(ctx context.Context, tx *types.Transaction, store api.StateReader) (execution.ExecutedTx, error)`
  - `func (e *EndorsementClient) EndorseMerged(ctx context.Context, txs []*types.Transaction, merged endorsement.ExecutionResult) (sdk.Endorsement, error)` — build the invocation from `txs`, sign `merged`, return the endorsement. No included/terminal/RWS tuple: the pipeline already knows every tx's outcome from its own `StreamTx`, and it built the read-write set itself.
  - `func (e *EndorsementClient) EndorserCount() int`

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/endorse_test.go (append)

// stubStreamService is the minimum api.Service the streaming pass-throughs need.
type stubStreamService struct {
	api.Service
	warmCalls  int
	authCalls  int
	warmResult []endorsement.ExecutionResult
}

func (s *stubStreamService) WarmBatchOn(_ context.Context, txs []*types.Transaction, _ api.StateReader) ([]endorsement.ExecutionResult, error) {
	s.warmCalls++
	return s.warmResult, nil
}

func (s *stubStreamService) AuthTxOn(_ context.Context, _ *types.Transaction, _ api.StateReader) (endorsement.ExecutionResult, error) {
	s.authCalls++
	return endorsement.ExecutionResult{Status: 200}, nil
}

func (s *stubStreamService) EndorseMerged(_ endorsement.Invocation, _ endorsement.ExecutionResult) (*peer.ProposalResponse, error) {
	return &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusOK}}, nil
}

func TestWarmBatchOnAndAuthTxOnUseTheSingleEndorser(t *testing.T) {
	svc := &stubStreamService{warmResult: []endorsement.ExecutionResult{{Status: 200}}}
	ec, err := NewEndorsementClient([]api.Service{svc}, nil, "ch", "ns", "1")
	if err != nil {
		t.Fatal(err)
	}
	if got := ec.EndorserCount(); got != 1 {
		t.Fatalf("EndorserCount = %d, want 1", got)
	}
	if _, err := ec.WarmBatchOn(context.Background(), []*types.Transaction{nil}, nil); err != nil {
		t.Fatalf("WarmBatchOn: %v", err)
	}
	if _, err := ec.AuthTxOn(context.Background(), nil, nil); err != nil {
		t.Fatalf("AuthTxOn: %v", err)
	}
	if svc.warmCalls != 1 || svc.authCalls != 1 {
		t.Errorf("warmCalls=%d authCalls=%d, want 1 and 1", svc.warmCalls, svc.authCalls)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestWarmBatchOnAndAuthTxOn -v`
Expected: FAIL — `ec.EndorserCount undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/endorse.go (insert after AuthBatch)

// EndorserCount reports how many endorsers this client fans out to. The
// streaming pipeline requires exactly one (see NewStreamPipeline): its
// accept-as-is fast path decides on the gateway's own copy of a read-write set,
// which presumes a single deterministic executor.
func (e *EndorsementClient) EndorserCount() int { return len(e.endorsers) }

// WarmBatchOn runs the streaming pipeline's concurrent warm pass on the single
// endorser, against a store the pipeline owns (its committed-only warmup cache).
// It returns one full read-write set per tx, in batch order. No fan-out: with one
// endorser there is nothing to fan out to, and with more than one the pipeline
// would have to reconcile divergent read-write sets before deciding accept-as-is
// -- a BFT question, deliberately out of this milestone.
func (e *EndorsementClient) WarmBatchOn(ctx context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error) {
	return e.endorsers[0].WarmBatchOn(ctx, txs, store)
}

// AuthTxOn re-executes one tx on the single endorser against the pipeline's auth
// cache. A client rejection rides in the result's Status, not in the error.
func (e *EndorsementClient) AuthTxOn(ctx context.Context, tx *types.Transaction, store api.StateReader) (execution.ExecutedTx, error) {
	return e.endorsers[0].AuthTxOn(ctx, tx, store)
}

// EndorseMerged signs one committer tx over a read-write set the CALLER built.
// txs are the transactions it covers, in aggregation order, and they build the
// invocation exactly as ExecuteBatch does (Args[0]=ProposalTypeEVMBatch,
// Args[1..N]=the marshaled txs), so the resulting committer tx and its FabricTxID
// are indistinguishable from the serial path's.
//
// Unlike ExecuteBatch it returns no included/terminal/RWS tuple. There is nothing
// to decode back: the pipeline knows each tx's outcome from its own StreamTx, and
// it aggregated the read-write set itself (see submitPhase.aggregate).
func (e *EndorsementClient) EndorseMerged(ctx context.Context, txs []*types.Transaction, merged endorsement.ExecutionResult) (sdk.Endorsement, error) {
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
	res, errs := e.fanOut(ctx, func(_ context.Context, _ int, endorser api.Service) (*peer.ProposalResponse, error) {
		return statusOnly(endorser.EndorseMerged(inv, merged))
	})
	for _, err := range errs {
		if err != nil {
			return sdk.Endorsement{}, err
		}
	}
	return sdk.Endorsement{Proposal: inv.Proposal, Responses: res}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestWarmBatchOnAndAuthTxOn -v && go build ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway/core/endorse.go gateway/core/endorse_test.go
git commit -m "feat(gateway): endorsement-client pass-throughs for the streaming pipeline"
```

---

### Task 6: `query.DirectReader` — nil-view reads with no memoization

**Files:**
- Create: `endorser/query/direct.go`
- Test: `endorser/query/direct_test.go`

**Interfaces:**
- Consumes: `query.QueryClient`, `query.Store`.
- Produces: `func (s *Store) NewDirectReader() *DirectReader`, `type DirectReader` implementing `Get(ns, key string) (*blocks.WriteRecord, error)` and `Close() error`.

Why not reuse `View`: `View` memoizes every key it fetches, for the lifetime of the view. That is correct for a per-batch snapshot and fatal for the streaming pipeline's long-lived fallback reader — a memoized key would serve its first-seen version forever, behind a warmup cache whose whole job is to be the coherent layer. `DirectReader` therefore caches nothing and begins no view: every `Get` reads current committed state on a pooled connection.

- [ ] **Step 1: Write the failing test**

```go
// endorser/query/direct_test.go
package query

import (
	"context"
	"testing"
)

// directSpy counts GetRows calls and asserts BeginView is never used.
type directSpy struct {
	beganView bool
	getRows   int
	version   uint64
}

func (d *directSpy) BeginView(context.Context) (string, error) { d.beganView = true; return "v1", nil }
func (d *directSpy) EndView(context.Context, string) error     { return nil }
func (d *directSpy) Close() error                              { return nil }
func (d *directSpy) GetRows(_ context.Context, viewID, _ string, keys [][]byte) ([]Row, error) {
	d.getRows++
	if viewID != "" {
		panic("DirectReader must read with an EMPTY viewID (nil-view current-committed path), got " + viewID)
	}
	d.version++
	return []Row{{Key: keys[0], Value: []byte("v"), Version: d.version}}, nil
}

func TestDirectReaderNeverBeginsAViewAndNeverMemoizes(t *testing.T) {
	spy := &directSpy{}
	r := NewStore(spy, "ns").NewDirectReader()

	first, err := r.Get("ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Get("ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	if spy.beganView {
		t.Error("DirectReader must not call BeginView")
	}
	if spy.getRows != 2 {
		t.Errorf("GetRows called %d times, want 2 -- DirectReader must not memoize", spy.getRows)
	}
	if first.Version == second.Version {
		t.Errorf("both reads returned version %d; the second must observe the newer committed version", first.Version)
	}
}

func TestDirectReaderReportsAbsentKeyAsNilRecord(t *testing.T) {
	r := NewStore(&emptyRowsClient{}, "ns").NewDirectReader()
	rec, err := r.Get("ns", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if rec != nil {
		t.Fatalf("absent key must read back as a nil record, got %+v", rec)
	}
}

type emptyRowsClient struct{ directSpy }

func (e *emptyRowsClient) GetRows(context.Context, string, string, [][]byte) ([]Row, error) {
	return nil, nil
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/query/ -run TestDirectReader -v`
Expected: FAIL — `NewDirectReader undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// endorser/query/direct.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// DirectReader reads CURRENT committed state, one key per call, with no view and
// no memoization. It is the streaming pipeline's query-service fallback (see
// evm-design/optimization.md, Batch Worker: "Fallback to the query service - no
// need for views (nil view)").
//
// It is deliberately NOT a View. A View memoizes every key it fetches for the
// life of the view, which is right for a per-batch snapshot and wrong for a
// long-lived fallback: a memoized key would serve its first-seen version
// forever, underneath a warmup cache whose entire purpose is to be the one
// coherent layer (refreshed from commit notifications). Caching here would
// silently defeat that refresh.
//
// Safe for concurrent use: it holds no mutable state, and the client's GetRows
// round-robins across the connection pool, which is exactly what the concurrent
// warm workers need (a single HTTP/2 connection serializes concurrent reads --
// a sweep measured 4212 tx/s at 1 connection rising to a ~5040 tx/s plateau at
// 4-16 and regressing at 32).
type DirectReader struct {
	client    QueryClient
	namespace string
}

// NewDirectReader returns a non-memoizing, view-less reader over the store's
// client and namespace. Unlike NewSnapshot it performs no BeginView, so there is
// nothing to end; Close is a no-op and the store owns the client's lifecycle.
func (s *Store) NewDirectReader() *DirectReader {
	return &DirectReader{client: s.client, namespace: s.namespace}
}

// Get returns the committed record for (namespace, key), or (nil, nil) when the
// key has no committed value. The empty viewID selects the query service's
// current-committed read path (see GRPCClient.GetRows).
func (r *DirectReader) Get(namespace, key string) (*blocks.WriteRecord, error) {
	rows, err := r.client.GetRows(context.Background(), "", namespace, [][]byte{[]byte(key)})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if string(row.Key) != key {
			continue
		}
		return &blocks.WriteRecord{
			Namespace: namespace,
			Key:       key,
			Version:   row.Version,
			Value:     row.Value,
		}, nil
	}
	return nil, nil
}

// Close is a no-op: there is no view to end, and the Store owns the client.
func (r *DirectReader) Close() error { return nil }

var _ execution.ReadStore = (*DirectReader)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/query/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add endorser/query/direct.go endorser/query/direct_test.go
git commit -m "feat(query): DirectReader -- non-memoizing nil-view committed reads"
```

---

### Task 7: `GRPCClient.GetTxStatus` — one call adjudicates many txIDs

**Files:**
- Create: `endorser/query/txstatus.go`
- Test: `endorser/query/txstatus_test.go`

**Interfaces:**
- Consumes: `GRPCClient.pick()`, `GRPCClient.viewTimeout`, `committerpb.QueryServiceClient.GetTransactionStatus`.
- Produces:
  - `type TxStatusClient interface { GetTxStatus(ctx context.Context, txIDs []string) (map[string]committerpb.Status, error) }`
  - `func (c *GRPCClient) GetTxStatus(ctx context.Context, txIDs []string) (map[string]committerpb.Status, error)`

A separate optional interface, not an addition to `QueryClient`: every existing test double implements `QueryClient`, and widening it would break all of them for a method none of them need.

- [ ] **Step 1: Write the failing test**

```go
// endorser/query/txstatus_test.go
package query

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
)

func TestGetTxStatusMapsEveryQueriedID(t *testing.T) {
	stub := &txStatusStub{statuses: []*committerpb.TxStatus{
		{Ref: &committerpb.TxRef{TxId: "committed"}, Status: committerpb.Status_COMMITTED},
		{Ref: &committerpb.TxRef{TxId: "aborted"}, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}}
	c := &GRPCClient{cls: []committerpb.QueryServiceClient{stub}}

	got, err := c.GetTxStatus(context.Background(), []string{"committed", "aborted", "unknown"})
	if err != nil {
		t.Fatal(err)
	}
	if got["committed"] != committerpb.Status_COMMITTED {
		t.Errorf("committed -> %v, want COMMITTED", got["committed"])
	}
	if got["aborted"] != committerpb.Status_ABORTED_MVCC_CONFLICT {
		t.Errorf("aborted -> %v, want ABORTED_MVCC_CONFLICT", got["aborted"])
	}
	// A txID the query service does not report has not been validated yet. It must
	// map to STATUS_UNSPECIFIED -- "still in flight" -- never be missing, or the
	// caller cannot tell "unknown" from "not asked".
	if s, ok := got["unknown"]; !ok || s != committerpb.Status_STATUS_UNSPECIFIED {
		t.Errorf("unknown -> (%v, present=%v), want (STATUS_UNSPECIFIED, true)", s, ok)
	}
	if len(stub.queried) != 3 {
		t.Errorf("query carried %d txIDs, want all 3 in ONE call", len(stub.queried))
	}
}

func TestGetTxStatusEmptyInputMakesNoCall(t *testing.T) {
	stub := &txStatusStub{}
	c := &GRPCClient{cls: []committerpb.QueryServiceClient{stub}}
	got, err := c.GetTxStatus(context.Background(), nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("GetTxStatus(nil) = (%v, %v), want (empty, nil)", got, err)
	}
	if stub.calls != 0 {
		t.Errorf("made %d RPC calls for an empty input, want 0", stub.calls)
	}
}
```

`txStatusStub` embeds `committerpb.QueryServiceClient` (so unimplemented methods panic if ever called), records `queried []string` and `calls int`, and returns `&committerpb.TxStatusResponse{Statuses: s.statuses}`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/query/ -run TestGetTxStatus -v`
Expected: FAIL — `c.GetTxStatus undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// endorser/query/txstatus.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
)

// TxStatusClient adjudicates the outcome of already-submitted committer txs.
// It is a separate, optional interface rather than part of QueryClient because
// every existing QueryClient test double would otherwise have to implement a
// method it never uses.
type TxStatusClient interface {
	// GetTxStatus returns one status per queried txID. A txID the query service
	// does not report back is mapped to Status_STATUS_UNSPECIFIED.
	GetTxStatus(ctx context.Context, txIDs []string) (map[string]committerpb.Status, error)
}

// GetTxStatus asks the query service for the validation outcome of txIDs in a
// SINGLE call, which is what makes it usable as the streaming pipeline's
// notification-timeout adjudicator: every tx that timed out together is
// adjudicated together (see evm-design/optimization.md, Notification Timeout).
//
// Every queried txID appears in the result. A txID the query service omits has
// not been validated yet, which is reported as Status_STATUS_UNSPECIFIED --
// "still in flight" -- and NOT as an absent entry, so a caller can never confuse
// "unknown" with "not asked". That distinction is the whole point of asking: a
// slow commit and a lost notification are indistinguishable from the gateway's
// side, and rolling back a tx that is merely slow costs a re-execution plus, if
// it then commits, a spurious rollback of everything behind it.
//
// No view is set: the status of a tx is not a snapshot read.
func (c *GRPCClient) GetTxStatus(ctx context.Context, txIDs []string) (map[string]committerpb.Status, error) {
	out := make(map[string]committerpb.Status, len(txIDs))
	if len(txIDs) == 0 {
		return out, nil
	}
	if c.viewTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.viewTimeout)
		defer cancel()
	}
	for _, id := range txIDs {
		out[id] = committerpb.Status_STATUS_UNSPECIFIED
	}
	res, err := c.pick().GetTransactionStatus(ctx, &committerpb.TxStatusQuery{TxIds: txIDs})
	if err != nil {
		return nil, err
	}
	for _, st := range res.GetStatuses() {
		if ref := st.GetRef(); ref != nil {
			if _, asked := out[ref.GetTxId()]; asked {
				out[ref.GetTxId()] = st.GetStatus()
			}
		}
	}
	return out, nil
}

var _ TxStatusClient = (*GRPCClient)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/query/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add endorser/query/txstatus.go endorser/query/txstatus_test.go
git commit -m "feat(query): GetTxStatus -- batch tx-status adjudication for notification timeouts"
```

---
## Phase 1 — The warmup cache: committed state only

This is the single change that dissolves the trilemma in [findings §6](../findings.md). Today's `VersionedCache` caches **speculative uncommitted writes** at predicted versions, so a mispredicted chain serves an over-read and the batch aborts — repeatably. The warmup cache holds **committed state only**: no prediction, nothing to mispredict. The cost is that a warm read of a key our own in-flight tx has written is stale, and the auth phase must catch it — which is what Phases 2, 3 and 6 build.

### Task 8: `WarmCache` — MFU, byte-capped, version never lowered

**Files:**
- Create: `gateway/core/stream_warm_cache.go`
- Test: `gateway/core/stream_warm_cache_test.go`

**Interfaces:**
- Consumes: `StreamKV` (Task 1); `blocks.WriteRecord`.
- Produces: `func NewWarmCache(namespace string, maxBytes int64) *WarmCache`, and on `*WarmCache`: `Get(key string) (*blocks.WriteRecord, bool)`, `Fill(key string, rec *blocks.WriteRecord)`, `Sweep()`, `Len() int`, `Bytes() int64`. (`Refresh` arrives in Task 9.)

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_warm_cache_test.go
package core

import (
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func warmRec(key string, version uint64, value string) *blocks.WriteRecord {
	return &blocks.WriteRecord{Key: key, Version: version, Value: []byte(value)}
}

func TestWarmCacheFillThenGet(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	if _, ok := c.Get("k"); ok {
		t.Fatal("empty cache must miss")
	}
	c.Fill("k", warmRec("k", 7, "v7"))
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("Fill then Get must hit")
	}
	if got.Version != 7 || string(got.Value) != "v7" {
		t.Fatalf("got version=%d value=%q, want 7/\"v7\"", got.Version, got.Value)
	}
	if got.Namespace != "ns" {
		t.Errorf("Namespace = %q, want the cache's namespace %q", got.Namespace, "ns")
	}
}

// A cache fill must never LOWER a cached version. A warm worker's read can race
// ahead of the query service's commit visibility and come back with the
// pre-commit version of a key a notification refresh has already advanced;
// letting it win would reintroduce exactly the stale read the refresh exists to
// remove.
func TestWarmCacheFillNeverLowersVersion(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Fill("k", warmRec("k", 9, "new"))
	c.Fill("k", warmRec("k", 4, "stale"))
	got, _ := c.Get("k")
	if got.Version != 9 || string(got.Value) != "new" {
		t.Fatalf("got version=%d value=%q, want the newer 9/\"new\" to survive", got.Version, got.Value)
	}
}

func TestWarmCacheDoesNotCacheAbsentKeys(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Fill("gone", nil)
	if _, ok := c.Get("gone"); ok {
		t.Fatal("an absent key has no version to compare and must not be cached")
	}
	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0", c.Len())
	}
}

// MFU retention: when over budget, the LEAST-used entries go first, so the hot
// set stays resident. This is the opposite of an LRU's recency bias and is what
// makes the notification refresh necessary (a hot key is preferentially RETAINED,
// so without a refresh it would serve its pre-commit version indefinitely).
func TestWarmCacheSweepEvictsLeastUsedFirst(t *testing.T) {
	// Three entries, budget for roughly two.
	c := NewWarmCache("ns", 3*warmEntryBytes("cold", warmRec("cold", 1, "0123456789"))/2)
	for _, k := range []string{"hot", "warm", "cold"} {
		c.Fill(k, warmRec(k, 1, "0123456789"))
	}
	for range 100 {
		c.Get("hot")
	}
	for range 10 {
		c.Get("warm")
	}
	c.Sweep()

	if _, ok := c.Get("hot"); !ok {
		t.Error("the most-used entry must survive the sweep")
	}
	if _, ok := c.Get("cold"); ok {
		t.Error("the least-used entry must be evicted first")
	}
	if c.Bytes() > c.MaxBytes() {
		t.Errorf("Bytes = %d after Sweep, want <= MaxBytes = %d", c.Bytes(), c.MaxBytes())
	}
}

func TestWarmCacheSweepUnderBudgetIsANoOp(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Fill("a", warmRec("a", 1, "v"))
	c.Fill("b", warmRec("b", 1, "v"))
	before := c.Len()
	c.Sweep()
	if c.Len() != before {
		t.Fatalf("Len = %d after an under-budget sweep, want %d unchanged", c.Len(), before)
	}
}

// Concurrent Fill/Get/Sweep is the real access pattern (many warm workers plus
// the TXs worker's boundary sweep). Run under -race.
func TestWarmCacheConcurrentAccessIsRaceFree(t *testing.T) {
	c := NewWarmCache("ns", 4096)
	done := make(chan struct{})
	for w := range 8 {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := range 500 {
				k := string(rune('a' + (i+w)%16))
				c.Fill(k, warmRec(k, uint64(i), "value"))
				c.Get(k)
			}
		}(w)
	}
	go func() {
		defer func() { done <- struct{}{} }()
		for range 50 {
			c.Sweep()
		}
	}()
	for range 9 {
		<-done
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestWarmCache -v`
Expected: FAIL — `undefined: NewWarmCache`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_warm_cache.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// cachedBytes is the size of one cached entry: key + value + version. That is
// exactly what the budget is defined over -- the cached STATE -- so the accounting
// is exact, not an estimate. Index overhead (map buckets, entry headers, sync.Map
// internals) is deliberately excluded: it is not cached state, and counting it
// would make the same budget mean different things on different map growth stages.
func cachedBytes(key string, value []byte) int64 {
	return int64(len(key) + len(value) + 8) // 8 = the uint64 version
}

// warmEntryBytes is cachedBytes for a record.
func warmEntryBytes(key string, rec *blocks.WriteRecord) int64 {
	return cachedBytes(key, rec.Value)
}

// warmEntry is one cached committed record with its frequency counter.
//
// rec is an atomic pointer to an IMMUTABLE record: an update never mutates the
// record in place, it publishes a NEW one (see evm-design/optimization.md,
// Warmup Cache: "Key, value, version are immutable"). That immutability is what
// lets Get hand the record to a concurrently-running EVM execution without a
// lock, and what lets the auth cache share the very same key and value bytes
// without copying them.
type warmEntry struct {
	rec   atomic.Pointer[blocks.WriteRecord]
	uses  atomic.Uint64
	bytes atomic.Int64
}

// WarmCache is the warmup phase's cross-batch cache of COMMITTED state. It holds
// committed records only -- never an uncommitted transaction write -- which is
// the property that removes speculative version prediction from the read path
// entirely, and with it the chain-misprediction abort livelock of the previous
// pipelined executor (see docs/evm-design-impl/findings.md section 6).
//
// Retention is frequency-based (MFU): entries carry a monotonic use counter and,
// when the byte budget is exceeded, the LEAST-used entries are evicted so the
// hottest keys stay resident. That bias is exactly why WarmCache.Refresh exists:
// MFU preferentially RETAINS a hot key, so without a refresh from commit
// notifications a hot committed key would keep serving its pre-commit version
// indefinitely.
//
// Concurrency contract: every method is safe for concurrent use. The index is a
// sync.Map because all of a batch's warm workers read and fill it at once; the
// design's own note is that this is affordable because the phase is bounded by
// network I/O, not by cache latency. Sweep runs concurrently with Get/Fill (on
// the TXs worker, at a batch-formation boundary) and needs no barrier: an entry
// evicted while a worker is reading it simply becomes a query-service read, and
// since nothing here is authoritative, losing an entry can only cost time.
//
// Staleness: a read served from this cache is a committed read at the cached
// version. It can be stale in exactly one way -- our own in-flight tx has
// written the key and not yet committed -- and catching that is the auth phase's
// job (AuthCache write state plus the watermark), not this cache's.
//
// Known limitation, inherited from ReadOnlyCache: the use counter is monotonic,
// so a key that was hot and goes permanently cold keeps its rank. The target
// workload's hot set is stable; if that changes, halve every counter in Sweep.
type WarmCache struct {
	namespace string
	maxBytes  int64
	size      atomic.Int64
	entries   sync.Map // string -> *warmEntry
}

// NewWarmCache returns an empty cache. maxBytes is an exact budget over cached
// key+value+version bytes (see cachedBytes); <= 0 falls back to
// DefaultWarmCacheBytes so a mis-wire cannot silently make the cache unbounded.
func NewWarmCache(namespace string, maxBytes int64) *WarmCache {
	if maxBytes <= 0 {
		maxBytes = DefaultWarmCacheBytes
	}
	return &WarmCache{namespace: namespace, maxBytes: maxBytes}
}

// MaxBytes reports the configured budget (test/observability helper).
func (c *WarmCache) MaxBytes() int64 { return c.maxBytes }

// Bytes reports the accounted size of the resident set.
func (c *WarmCache) Bytes() int64 { return c.size.Load() }

// Len reports the number of resident entries. O(n): test/observability only.
func (c *WarmCache) Len() int {
	n := 0
	c.entries.Range(func(any, any) bool { n++; return true })
	return n
}

// Get returns a copy of the cached record for key and records a use, or false on
// a miss. The copy preserves Version/BlockNum/TxNum so the caller's MVCC read-set
// records the same version it would have recorded reading through the query
// service; the Value slice is shared, which is safe because a cached record is
// never mutated in place.
func (c *WarmCache) Get(key string) (*blocks.WriteRecord, bool) {
	v, ok := c.entries.Load(key)
	if !ok {
		return nil, false
	}
	e := v.(*warmEntry)
	rec := e.rec.Load()
	if rec == nil {
		return nil, false
	}
	e.uses.Add(1)
	copied := *rec
	return &copied, true
}

// Fill admits or advances key from a query-service read. A nil record (the key
// has no committed value) is NOT cached: an absent key has no version to compare
// a later read against, so caching absence would turn a cheap re-read into a
// silent staleness window.
func (c *WarmCache) Fill(key string, rec *blocks.WriteRecord) {
	if rec == nil || rec.IsDelete {
		return
	}
	c.put(key, rec.Version, rec.Value, false)
}

// put publishes (version, value) for key. residentOnly makes it a refresh: a key
// that is not already resident is left alone (see Refresh).
//
// The version guard is the invariant: a put never LOWERS a cached version. A warm
// read can race ahead of the query service's commit visibility and return the
// pre-commit version of a key a notification refresh has already advanced;
// without the guard that read would overwrite the refreshed entry with a stale
// one, which is precisely the failure the refresh exists to prevent.
func (c *WarmCache) put(key string, version uint64, value []byte, residentOnly bool) {
	fresh := &blocks.WriteRecord{Namespace: c.namespace, Key: key, Version: version, Value: value}
	for {
		v, ok := c.entries.Load(key)
		if !ok {
			if residentOnly {
				return // a refresh never admits: the next read fetches it fresh anyway
			}
			e := &warmEntry{}
			e.rec.Store(fresh)
			nb := warmEntryBytes(key, fresh)
			e.bytes.Store(nb)
			if _, loaded := c.entries.LoadOrStore(key, e); !loaded {
				c.size.Add(nb)
				return
			}
			continue // lost the admission race; retry as an update on the winner
		}
		e := v.(*warmEntry)
		cur := e.rec.Load()
		if cur != nil && cur.Version >= version {
			return // never lower a cached version
		}
		if e.rec.CompareAndSwap(cur, fresh) {
			nb := warmEntryBytes(key, fresh)
			c.size.Add(nb - e.bytes.Swap(nb))
			return
		}
		// Another writer advanced the entry first; re-evaluate the guard.
	}
}

// Sweep enforces the byte budget by evicting the least-used entries (MFU
// retention). It is a no-op while under budget, so the O(n) scan is paid only
// when the budget is actually exceeded.
//
// Called on the TXs worker at a batch-formation boundary, concurrently with warm
// workers' Get/Fill. That is safe by construction: evicting an entry a worker is
// mid-read on costs a query-service read and nothing else.
func (c *WarmCache) Sweep() {
	if c.size.Load() <= c.maxBytes {
		return
	}
	type ranked struct {
		key  string
		uses uint64
	}
	rows := make([]ranked, 0, 256)
	c.entries.Range(func(k, v any) bool {
		rows = append(rows, ranked{k.(string), v.(*warmEntry).uses.Load()})
		return true
	})
	sort.Slice(rows, func(i, j int) bool { return rows[i].uses < rows[j].uses })

	over := c.size.Load() - c.maxBytes
	for _, r := range rows {
		if over <= 0 {
			return
		}
		v, ok := c.entries.LoadAndDelete(r.key)
		if !ok {
			continue
		}
		freed := v.(*warmEntry).bytes.Load()
		c.size.Add(-freed)
		over -= freed
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestWarmCache -race -v`
Expected: PASS (6 tests, no race reports).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_warm_cache.go gateway/core/stream_warm_cache_test.go
git commit -m "feat(stream): committed-only MFU warmup cache"
```

---

### Task 9: `WarmCache.Refresh` — the coherence mechanism

**Files:**
- Modify: `gateway/core/stream_warm_cache.go` (add `Refresh`)
- Test: `gateway/core/stream_warm_cache_test.go` (append)

**Interfaces:**
- Consumes: `WarmCache.put` (Task 8), `StreamKV` (Task 1).
- Produces: `func (c *WarmCache) Refresh(committed []StreamKV)`.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_warm_cache_test.go (append)

func TestRefreshUpdatesResidentKeyInPlace(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Fill("k", warmRec("k", 3, "old"))
	c.Refresh([]StreamKV{{Key: "k", Version: 4, Value: []byte("committed")}})

	got, ok := c.Get("k")
	if !ok {
		t.Fatal("refresh must not remove a resident key")
	}
	if got.Version != 4 || string(got.Value) != "committed" {
		t.Fatalf("got version=%d value=%q, want 4/\"committed\"", got.Version, got.Value)
	}
}

// A key that is not resident is IGNORED, not admitted: the next read fetches it
// fresh from the query service anyway, and admitting it would pull cold keys into
// a cache whose budget is reserved for the hot set.
func TestRefreshIgnoresNonResidentKey(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Refresh([]StreamKV{{Key: "cold", Version: 9, Value: []byte("v")}})
	if _, ok := c.Get("cold"); ok {
		t.Fatal("refresh must not admit a non-resident key")
	}
	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0", c.Len())
	}
}

// The refresh must preserve the frequency statistics, or refreshing a hot key
// would reset its rank and hand it to the next sweep.
func TestRefreshPreservesFrequencyStatistics(t *testing.T) {
	c := NewWarmCache("ns", 3*warmEntryBytes("cold", warmRec("cold", 1, "0123456789"))/2)
	for _, k := range []string{"hot", "cold"} {
		c.Fill(k, warmRec(k, 1, "0123456789"))
	}
	for range 100 {
		c.Get("hot")
	}
	c.Refresh([]StreamKV{{Key: "hot", Version: 2, Value: []byte("0123456789")}})
	c.Fill("filler", warmRec("filler", 1, "0123456789"))
	c.Sweep()

	if _, ok := c.Get("hot"); !ok {
		t.Fatal("a refreshed hot key must keep its rank and survive the sweep")
	}
}

func TestRefreshNeverLowersVersion(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Fill("k", warmRec("k", 8, "newer"))
	c.Refresh([]StreamKV{{Key: "k", Version: 5, Value: []byte("older")}})
	got, _ := c.Get("k")
	if got.Version != 8 || string(got.Value) != "newer" {
		t.Fatalf("got version=%d value=%q, want the newer 8/\"newer\" to survive", got.Version, got.Value)
	}
}

// A committed delete has no value to cache. Caching an empty value at a fresh
// version would let a later read see "present, empty" where the truth is
// "absent", so the entry is dropped and the next read learns the absence from
// the query service.
func TestRefreshOfDeleteDropsTheEntry(t *testing.T) {
	c := NewWarmCache("ns", 1<<20)
	c.Fill("k", warmRec("k", 3, "v"))
	c.Refresh([]StreamKV{{Key: "k", Version: 4, IsDelete: true}})
	if _, ok := c.Get("k"); ok {
		t.Fatal("a committed delete must drop the cached entry")
	}
	if c.Bytes() != 0 {
		t.Errorf("Bytes = %d after dropping the only entry, want 0", c.Bytes())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestRefresh -v`
Expected: FAIL — `c.Refresh undefined (type *WarmCache has no field or method Refresh)`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_warm_cache.go (append)

// Refresh applies a commit notification's committed keys to the cache: for every
// key ALREADY RESIDENT it publishes the committed value and version in place,
// keeping the frequency statistics. A key that is not resident is ignored (the
// next read fetches it fresh anyway), and if a refreshed key is evicted later,
// that is the MFU policy's decision to make.
//
// This is the only thing keeping the warmup cache coherent, and it is required
// precisely BECAUSE retention is MFU: eviction preferentially keeps a hot key, so
// without a refresh a hot committed key would serve its pre-commit version
// indefinitely -- and a warm batch reading it would be accepted by the auth
// phase's fast path (its key having been demoted to read by then) and abort at
// the committer, repeatably, because the retry re-warms from the same
// never-refreshed entry.
//
// Called only from the warmup notification worker, which MUST complete this
// refresh BEFORE it samples the batch counter for the watermark stamp (see
// runWarmNotifier). Concurrent with warm workers' Get/Fill.
func (c *WarmCache) Refresh(committed []StreamKV) {
	for _, kv := range committed {
		if kv.IsDelete {
			// A delete's committed state is absence, and absence is not cacheable
			// here (see Fill): drop the entry so the next read learns the truth from
			// the query service.
			if v, ok := c.entries.LoadAndDelete(kv.Key); ok {
				c.size.Add(-v.(*warmEntry).bytes.Load())
			}
			continue
		}
		c.put(kv.Key, kv.Version, kv.Value, true)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestWarmCache|TestRefresh' -race -v`
Expected: PASS (11 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_warm_cache.go gateway/core/stream_warm_cache_test.go
git commit -m "feat(stream): warmup cache refresh from commit notifications"
```

---
## Phase 2 — The auth cache: per-key read/write state

The auth cache is used by exactly one goroutine and therefore takes **no locks at all** — the design is explicit that this matters because the auth worker processes txs one at a time and cannot afford per-access synchronization. It shares the warmup cache's key and value bytes without copying them, because those are immutable.

**One version field, and one rule: a key's version steps exactly once per aggregate.** A write entry's `rec.Version` is the version the key will hold once the committer tx carrying it commits, which is also the version a reader in the *next* aggregate must record (`VersionedCache.apply`'s rule). It steps on the first write to the key since the last `NextAggregate` and not again, because the committer tx writes that key once.

Within one aggregate the entry therefore reports a version one ahead of what the committer validates against. That is harmless, and it is why the submitter keeps the **first** read version per key: the first reader of a written key is the tx that wrote it, which read it before the step. So the aggregate's read version is the pre-aggregate one (`overlayReader`'s rule) while its write version is the stepped one — both correct, neither computed by the auth phase.

The auth cache does not build the committer read-write set and does not produce the commit payload. It stamps each write's version onto the internal transaction, and the **submitter** derives both from the transactions themselves.

### Task 10: `AuthCache` read side and read-half eviction

**Files:**
- Create: `gateway/core/stream_auth_cache.go`
- Test: `gateway/core/stream_auth_cache_test.go`

**Interfaces:**
- Consumes: `blocks.WriteRecord`; `DefaultAuthReadCacheBytes` (Task 1).
- Produces: `func NewAuthCache(namespace string, maxReadBytes int64) *AuthCache`, and on `*AuthCache`: `Get(key string) (*blocks.WriteRecord, bool)`, `PutRead(key string, rec *blocks.WriteRecord)`, `Evict()`, `Len() int`, `ReadBytes() int64`, `MaxReadBytes() int64`.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_cache_test.go
package core

import (
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func authRec(key string, version uint64, value string) *blocks.WriteRecord {
	return &blocks.WriteRecord{Key: key, Version: version, Value: []byte(value)}
}

func TestAuthCachePutReadThenGet(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	if _, ok := c.Get("k"); ok {
		t.Fatal("empty cache must miss")
	}
	c.PutRead("k", authRec("k", 5, "v5"))
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("PutRead then Get must hit")
	}
	if got.Version != 5 || string(got.Value) != "v5" {
		t.Fatalf("got version=%d value=%q, want 5/\"v5\"", got.Version, got.Value)
	}
}

func TestAuthCachePutReadNeverLowersVersionAndSkipsAbsent(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	c.PutRead("k", authRec("k", 9, "newer"))
	c.PutRead("k", authRec("k", 2, "older"))
	got, _ := c.Get("k")
	if got.Version != 9 {
		t.Errorf("Version = %d, want the newer 9 to survive", got.Version)
	}
	c.PutRead("absent", nil)
	if _, ok := c.Get("absent"); ok {
		t.Error("a nil record must not be cached")
	}
}

func TestAuthCacheEvictKeepsMostUsedReads(t *testing.T) {
	one := authEntryBytes("cold", authRec("cold", 1, "0123456789"))
	c := NewAuthCache("ns", 3*one/2) // budget for roughly two entries
	for _, k := range []string{"hot", "warm", "cold"} {
		c.PutRead(k, authRec(k, 1, "0123456789"))
	}
	for range 100 {
		c.Get("hot")
	}
	for range 10 {
		c.Get("warm")
	}
	c.Evict()

	if _, ok := c.Get("hot"); !ok {
		t.Error("the most-used read entry must survive")
	}
	if _, ok := c.Get("cold"); ok {
		t.Error("the least-used read entry must be evicted first")
	}
	if c.ReadBytes() > c.MaxReadBytes() {
		t.Errorf("ReadBytes = %d after Evict, want <= %d", c.ReadBytes(), c.MaxReadBytes())
	}
}

func TestAuthCacheEvictUnderBudgetIsANoOp(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	c.PutRead("a", authRec("a", 1, "v"))
	c.Evict()
	if c.Len() != 1 {
		t.Fatalf("Len = %d, want 1", c.Len())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestAuthCache -v`
Expected: FAIL — `undefined: NewAuthCache`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_cache.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"sort"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// authEntryBytes is cachedBytes for a record: key + value + version, exactly (see
// cachedBytes in stream_warm_cache.go).
func authEntryBytes(key string, rec *blocks.WriteRecord) int64 {
	return cachedBytes(key, rec.Value)
}

// authEntry is one auth-cache entry with its read|write state.
//
// rec.Version is the version a READER must record for MVCC. For a read entry it is
// the key's committed version. For a write entry it is the version the key will
// hold once the committer tx carrying that write commits.
//
// dirty marks a key already stepped in the aggregate being assembled. It is what
// makes the version advance exactly once per aggregate no matter how many txs in it
// write the key -- see ApplyWrites.
type authEntry struct {
	rec   blocks.WriteRecord
	write bool
	dirty bool
	uses  uint64
	bytes int64
}

// AuthCache is the auth phase's cross-batch cache. Two properties define it:
//
// 1. NO SYNCHRONIZATION. It is touched only by the single auth worker, which
// never spawns sub-workers, so every method assumes exclusive access. That is
// not an optimization detail but the reason the phase can afford a cache at all:
// it processes txs one at a time, and a mutex per key access would show up
// directly in the serial floor. Calling any method from another goroutine is a
// programming error, and `go test -race` on the pipeline tests is what keeps that
// honest.
//
// 2. PER-KEY READ|WRITE STATE. A key an in-flight tx has written is in `write`
// state and is PINNED: it cannot be evicted until its commit notification demotes
// it (see Demote). Only the read half is size-limited, which is sound because the
// write half is bounded by construction -- (auth-queue + submit-queue +
// submitted-but-not-yet-notified) txs times writes-per-tx.
//
// It shares the warmup cache's key and value bytes rather than copying them: both
// caches treat records as immutable, so the same backing arrays are safe to hold
// in two indexes.
type AuthCache struct {
	namespace    string
	maxReadBytes int64
	readBytes    int64
	entries      map[string]*authEntry
	dirty        []string // keys already stepped in the aggregate being assembled

	// staleReadRejects counts accept-as-is rejections caused by a read entry
	// holding a NEWER version than the warm read recorded -- a warm read of a key
	// the warmup cache had evicted, resolved against a query service that still
	// trailed our own commit. A health metric (the warm cache is too small for the
	// working set), not a bug signal. See Stale.
	staleReadRejects uint64
}

// NewAuthCache returns an empty cache. maxReadBytes <= 0 falls back to
// DefaultAuthReadCacheBytes.
func NewAuthCache(namespace string, maxReadBytes int64) *AuthCache {
	if maxReadBytes <= 0 {
		maxReadBytes = DefaultAuthReadCacheBytes
	}
	return &AuthCache{
		namespace:    namespace,
		maxReadBytes: maxReadBytes,
		entries:      make(map[string]*authEntry),
	}
}

// MaxReadBytes reports the read-half budget.
func (c *AuthCache) MaxReadBytes() int64 { return c.maxReadBytes }

// ReadBytes reports the accounted size of the READ half only. Write entries are
// excluded because they are pinned and unbounded by design.
func (c *AuthCache) ReadBytes() int64 { return c.readBytes }

// Len reports the total number of entries, read and write.
func (c *AuthCache) Len() int { return len(c.entries) }

// StaleReadRejects reports how many times the accept-as-is fast path was refused
// because the cache held a newer version than the warm read (see Stale).
func (c *AuthCache) StaleReadRejects() uint64 { return c.staleReadRejects }

// Get returns a copy of the entry's record and records a use, or false on a miss.
// The copy keeps Version/BlockNum/TxNum so the caller's MVCC read-set records the
// same version a query-service read would have.
func (c *AuthCache) Get(key string) (*blocks.WriteRecord, bool) {
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	e.uses++
	rec := e.rec
	return &rec, true
}

// PutRead admits or advances a committed read. It is a no-op for a nil record (an
// absent key has no version to compare against later, exactly as in
// WarmCache.Fill), it never DOWNGRADES a write entry to a read -- only a commit
// notification may do that, via Demote -- and it never lowers a cached version.
func (c *AuthCache) PutRead(key string, rec *blocks.WriteRecord) {
	if rec == nil {
		return
	}
	e, ok := c.entries[key]
	if !ok {
		ne := &authEntry{rec: *rec, uses: 1, bytes: authEntryBytes(key, rec)}
		ne.rec.Namespace = c.namespace
		c.entries[key] = ne
		c.readBytes += ne.bytes
		return
	}
	if e.write || e.rec.Version >= rec.Version {
		return
	}
	nb := authEntryBytes(key, rec)
	c.readBytes += nb - e.bytes
	e.bytes = nb
	e.rec = *rec
	e.rec.Namespace = c.namespace
}

// Evict enforces the read-half budget by dropping the least-used READ entries
// (MFU retention). Write entries are skipped unconditionally: they are pinned
// until demoted, and dropping one would erase an in-flight value a later tx must
// observe.
//
// Called at the END of a batch, never during execution -- the design is explicit
// that eviction mid-execution costs performance for no benefit, and it would also
// let a key vanish between two txs of the same batch.
func (c *AuthCache) Evict() {
	if c.readBytes <= c.maxReadBytes {
		return
	}
	type ranked struct {
		key  string
		uses uint64
	}
	rows := make([]ranked, 0, len(c.entries))
	for k, e := range c.entries {
		if e.write {
			continue // pinned until its commit notification demotes it
		}
		rows = append(rows, ranked{k, e.uses})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].uses < rows[j].uses })

	over := c.readBytes - c.maxReadBytes
	for _, r := range rows {
		if over <= 0 {
			return
		}
		e := c.entries[r.key]
		delete(c.entries, r.key)
		c.readBytes -= e.bytes
		over -= e.bytes
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestAuthCache -race -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_cache.go gateway/core/stream_auth_cache_test.go
git commit -m "feat(stream): auth cache read side with MFU read-half eviction"
```

---
### Task 11: `AuthCache` write state — `ApplyWrites`, `NextAggregate`, `IsWrite`, `Stale`

**Files:**
- Modify: `gateway/core/stream_auth_cache.go` (append)
- Test: `gateway/core/stream_auth_cache_test.go` (append)

**Interfaces:**
- Consumes: `authEntry`, `AuthCache` (Task 10); `StreamTx` / `StreamKV` (Task 1).
- Produces, on `*AuthCache`:
  - `ApplyWrites(tx *StreamTx)` — mark every written key `write` with its new value, and **stamp each write's version onto the tx**.
  - `NextAggregate()` — close the aggregate being assembled. Clears the per-key "already stepped" marks; computes and returns nothing.
  - `IsWrite(key string) bool`
  - `Stale(key string, read StreamKV) bool` — the accept-as-is guard.
  - `WriteLen() int`

**The one rule that makes the versions come out right: a key's version steps exactly once per aggregate.** The committer tx writes each key once, so its committed version advances once, no matter how many txs in that aggregate wrote it. `ApplyWrites` steps a key on the *first* write to it since the last `NextAggregate` and not again; the `dirty` flag is that marker.

After the step the entry reports the stepped version to readers. Across aggregates that is exactly right — the next committer tx's reads must record the version this one produces. Within the same aggregate it is one ahead of what the committer will validate against, and that is harmless because the submitter's aggregation keeps the **first** read version per key, and the first reader of a written key is the tx that wrote it, which read it *before* the step.

Because the step boundary must be the boundary the submitter aggregates on, and in this milestone the submit batch is the auth batch 1:1, the auth worker calls `NextAggregate` at its flush. **When adaptive submit batching lands in phase 2 the two diverge, and this call has to move to whoever decides the aggregate.**

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_cache_test.go (append)

// streamTxOf builds an internal tx with the given reads (key -> version) and
// writes (key -> value). Write versions are left zero: ApplyWrites stamps them.
func streamTxOf(reads map[string]uint64, writes map[string]string) *StreamTx {
	tx := &StreamTx{Status: 200}
	for k, v := range reads {
		tx.Reads = append(tx.Reads, StreamKV{Key: k, Version: v, Value: []byte("read")})
	}
	for k, v := range writes {
		tx.Writes = append(tx.Writes, StreamKV{Key: k, Value: []byte(v)})
	}
	return tx
}

// The first write to a key in an aggregate steps its version once, and the step is
// stamped back onto the tx so the submitter never has to ask the cache.
func TestApplyWritesStepsOnceAndStampsTheTx(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	tx := streamTxOf(map[string]uint64{"k": 5}, map[string]string{"k": "v1"})
	c.ApplyWrites(tx)

	if !c.IsWrite("k") {
		t.Fatal("a written key must be in write state")
	}
	if tx.Writes[0].Version != 6 {
		t.Errorf("stamped write version = %d, want 6 (read 5 + 1)", tx.Writes[0].Version)
	}
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("a written key must be readable")
	}
	if string(got.Value) != "v1" || got.Version != 6 {
		t.Fatalf("got version=%d value=%q, want 6/\"v1\"", got.Version, got.Value)
	}
}

// Two txs in the SAME aggregate writing one key: the version steps once, because
// the committer tx writes that key once. Both writes are stamped with it, which is
// what makes "the last write's version" the aggregate's version.
func TestApplyWritesStepsOnlyOncePerAggregate(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	first := streamTxOf(map[string]uint64{"k": 5}, map[string]string{"k": "v1"})
	c.ApplyWrites(first)
	// The second tx reads the key through the cache, so it sees 6.
	second := streamTxOf(map[string]uint64{"k": 6}, map[string]string{"k": "v2"})
	c.ApplyWrites(second)

	if first.Writes[0].Version != 6 || second.Writes[0].Version != 6 {
		t.Fatalf("stamped versions = %d and %d, want 6 and 6 -- one step per aggregate",
			first.Writes[0].Version, second.Writes[0].Version)
	}
	got, _ := c.Get("k")
	if got.Version != 6 || string(got.Value) != "v2" {
		t.Fatalf("got version=%d value=%q, want 6/\"v2\" (last write wins, version unchanged)", got.Version, got.Value)
	}
}

// A new aggregate steps the key again: that committer tx produces the next version.
func TestNextAggregateAllowsTheNextStep(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	c.ApplyWrites(streamTxOf(map[string]uint64{"k": 5}, map[string]string{"k": "a"}))
	c.NextAggregate()
	next := streamTxOf(nil, map[string]string{"k": "b"})
	c.ApplyWrites(next)

	if next.Writes[0].Version != 7 {
		t.Fatalf("stamped version = %d, want 7 (6 from the first aggregate + 1)", next.Writes[0].Version)
	}
	got, _ := c.Get("k")
	if got.Version != 7 {
		t.Errorf("cache version = %d, want 7", got.Version)
	}
}

// A blind write of a key with no committed value starts at 0, so its first
// version is 1 -- matching VersionedCache.apply's rule for an absent key.
func TestApplyWritesBlindWriteOfAbsentKeyStartsAtOne(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	tx := streamTxOf(nil, map[string]string{"fresh": "v"})
	c.ApplyWrites(tx)
	if tx.Writes[0].Version != 1 {
		t.Fatalf("stamped version = %d, want 1 (absent base 0 + 1)", tx.Writes[0].Version)
	}
}

// A read recorded as ABSENT is a base of 0, not of its (meaningless) version field.
func TestApplyWritesTreatsAnAbsentReadAsBaseZero(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	tx := &StreamTx{
		Status: 200,
		Reads:  []StreamKV{{Key: "k", Version: 999, Absent: true}},
		Writes: []StreamKV{{Key: "k", Value: []byte("v")}},
	}
	c.ApplyWrites(tx)
	if tx.Writes[0].Version != 1 {
		t.Fatalf("stamped version = %d, want 1: an absent read carries no usable version", tx.Writes[0].Version)
	}
}

// The write half is unbounded: a pinned entry survives eviction even when the read
// budget is exhausted, because a later tx must still observe its value.
func TestEvictNeverDropsAWriteEntry(t *testing.T) {
	c := NewAuthCache("ns", 1) // read budget of 1 byte: everything evictable goes
	c.ApplyWrites(streamTxOf(map[string]uint64{"pinned": 1}, map[string]string{"pinned": "0123456789"}))
	c.PutRead("plain", authRec("plain", 1, "0123456789"))
	c.Evict()

	if _, ok := c.Get("pinned"); !ok {
		t.Error("a write entry must never be evicted; it is pinned until demoted")
	}
	if _, ok := c.Get("plain"); ok {
		t.Error("a read entry must be evicted when over budget")
	}
	if c.WriteLen() != 1 {
		t.Errorf("WriteLen = %d, want 1", c.WriteLen())
	}
}

func TestStaleRejectsWriteKeysAndNewerReads(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	c.ApplyWrites(streamTxOf(map[string]uint64{"w": 1}, map[string]string{"w": "v"}))
	c.PutRead("r", authRec("r", 7, "v"))

	if !c.Stale("w", StreamKV{Key: "w", Version: 1}) {
		t.Error("a key in write state must always refuse the accept-as-is fast path")
	}
	if c.Stale("r", StreamKV{Key: "r", Version: 7}) {
		t.Error("a read at the cached version is fresh")
	}
	if !c.Stale("r", StreamKV{Key: "r", Version: 6}) {
		t.Error("a read BELOW the cached committed version is stale")
	}
	if c.Stale("r", StreamKV{Key: "r", Version: 8}) {
		t.Error("a read ABOVE the cached version is not stale (the cache is merely behind)")
	}
	if c.Stale("unknown", StreamKV{Key: "unknown", Version: 3}) {
		t.Error("a key the cache knows nothing about cannot be judged stale")
	}
	if !c.Stale("r", StreamKV{Key: "r", Absent: true}) {
		t.Error("warm recording the key as ABSENT while the cache holds a committed value is stale")
	}
	if c.StaleReadRejects() == 0 {
		t.Error("stale rejections must be counted: they are the signal that a warm read was caught")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestApplyWrites|TestNextAggregate|TestEvictNever|TestStale' -v`
Expected: FAIL — `c.ApplyWrites undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_cache.go (append)

// WriteLen reports how many entries are currently pinned in write state.
func (c *AuthCache) WriteLen() int {
	n := 0
	for _, e := range c.entries {
		if e.write {
			n++
		}
	}
	return n
}

// IsWrite reports whether an in-flight tx has written key.
func (c *AuthCache) IsWrite(key string) bool {
	e, ok := c.entries[key]
	return ok && e.write
}

// ApplyWrites records tx's writes: every written key enters (or stays in) write
// state carrying the new value, and every write is STAMPED with the version the key
// will hold once the committer tx carrying it commits. The stamp is why no later
// stage has to consult this cache -- the submitter derives the whole committed
// payload from the transactions themselves.
//
// A key's version steps EXACTLY ONCE per aggregate, on the first write to it since
// the last NextAggregate. The committer tx writes that key once, so it advances
// once; the dirty flag is the "already stepped" marker. Every write to the key in
// the same aggregate is stamped with that same version, which is what makes the
// last write's version the aggregate's version.
//
// After the step the entry reports the stepped version to readers. Across
// aggregates that is exactly what MVCC needs. Within one aggregate it is one ahead
// of what the committer validates against, which is harmless: the submitter keeps
// the FIRST read version per key, and the first reader of a written key is the tx
// that wrote it -- which read it before the step.
//
// A key leaving the read half leaves the read budget: write entries are pinned and
// deliberately unbounded.
func (c *AuthCache) ApplyWrites(tx *StreamTx) {
	for i := range tx.Writes {
		w := &tx.Writes[i]
		e, ok := c.entries[w.Key]
		if !ok {
			// Base version: what the key held before this aggregate touched it. The
			// tx's own read is the authority; a key it read as ABSENT, or never read
			// at all, starts at 0 -- matching VersionedCache.apply's rule for a first
			// write of an absent key. Reads and writes per tx are both small (tens),
			// so the scan is cheaper than building a map.
			var base uint64
			for _, rd := range tx.Reads {
				if rd.Key != w.Key {
					continue
				}
				if !rd.Absent {
					base = rd.Version
				}
				break
			}
			e = &authEntry{rec: blocks.WriteRecord{Namespace: c.namespace, Key: w.Key, Version: base}}
			c.entries[w.Key] = e
		} else if !e.write {
			c.readBytes -= e.bytes // leaving the read half
		}

		if !e.dirty {
			e.dirty = true
			e.rec.Version++ // one step per key per aggregate
			c.dirty = append(c.dirty, w.Key)
		}
		e.write = true
		e.rec.Value = w.Value
		e.rec.IsDelete = w.IsDelete
		e.bytes = authEntryBytes(w.Key, &e.rec)

		w.Version = e.rec.Version
	}
}

// NextAggregate closes the aggregate being assembled: it clears the per-key
// "already stepped" marks, so the next aggregate's first write to a key steps its
// version again.
//
// That is all it does. It computes nothing and returns nothing -- deriving the
// committer read-write set and the committed payload belongs to the submitter,
// because only the submitter knows which txs share a committer tx (see
// submitPhase.aggregate).
//
// It MUST be called on the same boundary the submitter aggregates on. In this
// milestone the submit batch is the auth batch 1:1, so the auth worker calls it at
// flush; when adaptive submit batching lands the two diverge and this call moves.
func (c *AuthCache) NextAggregate() {
	for _, k := range c.dirty {
		if e, ok := c.entries[k]; ok {
			e.dirty = false
		}
	}
	c.dirty = c.dirty[:0]
}

// Stale reports whether a warm read of key must NOT be accepted as-is. It is the
// auth phase's fast-path guard, and it refuses on two grounds:
//
//   - key is in WRITE state. An in-flight tx has written it, so the warm read saw
//     committed state the pipeline itself has already superseded. This is the
//     design's own condition ("it first check if the read set contains keys from
//     the write cache").
//   - the cache holds a NEWER committed version than the warm read recorded. Our
//     own writes cannot cause this -- a write leaves the cache only when its commit
//     notification demotes it, and the watermark holds that demotion until the last
//     warm batch that could carry a pre-commit read has been authed. So this branch
//     fires only on OUTSIDE INTERFERENCE: another writer to our namespace. The
//     milestone ignores that case, and the counter exists so that if it happens it
//     shows up as a number instead of an unexplained abort.
//
// A key the cache knows nothing about is never judged stale: there is no better
// information here, and the committer's MVCC check remains the backstop.
func (c *AuthCache) Stale(key string, read StreamKV) bool {
	e, ok := c.entries[key]
	if !ok {
		return false
	}
	if e.write {
		return true
	}
	cacheAbsent := e.rec.IsDelete
	if cacheAbsent || read.Absent {
		if cacheAbsent && read.Absent {
			return false // both say absent: agreed
		}
		c.staleReadRejects++
		return true
	}
	if e.rec.Version > read.Version {
		c.staleReadRejects++
		return true
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestAuthCache|TestApplyWrites|TestNextAggregate|TestEvict|TestStale' -race -v`
Expected: PASS (11 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_cache.go gateway/core/stream_auth_cache_test.go
git commit -m "feat(stream): auth cache write state with one version step per aggregate"
```

---

### Task 12: `AuthCache.Demote` — write → read on commit

**Files:**
- Modify: `gateway/core/stream_auth_cache.go` (append)
- Test: `gateway/core/stream_auth_cache_test.go` (append)

**Interfaces:**
- Consumes: `AuthCache.ApplyWrites` / `NextAggregate`, `authEntry` (Task 11).
- Produces: `func (c *AuthCache) Demote(committed []StreamKV)`, `func (c *AuthCache) UnexpectedDemotes() uint64`.

The version comparison is the whole mechanism, and it has exactly three cases:

| Entry version vs committed | Meaning | Action |
|---|---|---|
| equal | this commit IS the newest write to the key | demote to read |
| entry newer | a later, still in-flight aggregate wrote it again | keep pinned |
| entry older | committed state is ahead of anything we hold | take the committed record, demote, count it |

The committed version in a notification is the one `ApplyWrites` stamped and the submitter carried through, so "equal" is the normal case and the comparison is exact. The third case cannot happen under ordered single-submitter commit — each aggregate advances a key by exactly one version, in submission order — so it is handled defensively and counted.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_cache_test.go (append)

func TestDemoteOnVersionMatch(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	tx := streamTxOf(map[string]uint64{"k": 5}, map[string]string{"k": "v"})
	c.ApplyWrites(tx) // k -> version 6
	c.NextAggregate()

	c.Demote(tx.Writes)
	if c.IsWrite("k") {
		t.Fatal("a committed key whose version matches must be demoted to read")
	}
	// Demoted means evictable again: it rejoins the read half's accounting.
	if c.ReadBytes() == 0 {
		t.Error("a demoted entry must rejoin the read-half byte accounting")
	}
	got, _ := c.Get("k")
	if got.Version != 6 || string(got.Value) != "v" {
		t.Errorf("got version=%d value=%q, want 6/\"v\"", got.Version, got.Value)
	}
}

// A later in-flight tx has written the key again, so the arriving notification is
// for an OLDER write. The entry must stay pinned: its value is not committed, and
// readers of it must keep re-executing.
func TestDemoteKeepsPinnedWhenOverwrittenByALaterTx(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	first := streamTxOf(map[string]uint64{"k": 5}, map[string]string{"k": "b1"})
	c.ApplyWrites(first) // version 6
	c.NextAggregate()
	second := streamTxOf(nil, map[string]string{"k": "b2"})
	c.ApplyWrites(second) // version 7
	c.NextAggregate()

	c.Demote(first.Writes)
	if !c.IsWrite("k") {
		t.Fatal("a notification for an older write must leave the entry pinned")
	}
	got, _ := c.Get("k")
	if string(got.Value) != "b2" {
		t.Errorf("Value = %q, want the newer in-flight write %q", got.Value, "b2")
	}
}

func TestDemoteIgnoresUnknownAndAlreadyReadKeys(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	c.PutRead("r", authRec("r", 3, "v"))
	c.Demote([]StreamKV{
		{Key: "evicted-or-never-cached", Version: 1},
		{Key: "r", Version: 3},
	})
	if c.IsWrite("r") || c.Len() != 1 {
		t.Fatalf("demoting an already-read key must be a no-op; IsWrite=%v Len=%d", c.IsWrite("r"), c.Len())
	}
}

// Defensive branch: committed state ahead of the pinned entry cannot happen under
// ordered single-submitter commit, so it is taken as authoritative and counted.
func TestDemoteTakesCommittedRecordWhenAheadAndCountsIt(t *testing.T) {
	c := NewAuthCache("ns", 1<<20)
	c.ApplyWrites(streamTxOf(map[string]uint64{"k": 5}, map[string]string{"k": "ours"})) // version 6
	c.NextAggregate()

	c.Demote([]StreamKV{{Key: "k", Version: 9, Value: []byte("theirs")}})
	if c.IsWrite("k") {
		t.Fatal("a committed version ahead of ours must demote the entry")
	}
	got, _ := c.Get("k")
	if got.Version != 9 || string(got.Value) != "theirs" {
		t.Errorf("got version=%d value=%q, want the committed 9/\"theirs\"", got.Version, got.Value)
	}
	if c.UnexpectedDemotes() != 1 {
		t.Errorf("UnexpectedDemotes = %d, want 1 -- this branch must be visible", c.UnexpectedDemotes())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestDemote -v`
Expected: FAIL — `c.Demote undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_cache.go (append)

// UnexpectedDemotes counts demotions where committed state was AHEAD of the
// pinned entry. Under ordered single-submitter commit this is impossible, so a
// non-zero value means either submission stopped being ordered or another writer
// is touching our namespace.
func (c *AuthCache) UnexpectedDemotes() uint64 { return c.unexpectedDemotes }

// Demote applies a commit notification's committed keys, moving each from write
// state back to read state when -- and only when -- the notification is for the
// NEWEST write the pipeline has made to that key.
//
// The version comparison is the whole mechanism:
//
//   - equal: this commit is the newest write. The pinned value is now committed
//     state, so demote it: it becomes evictable, and readers of the key stop
//     re-executing.
//   - entry newer: a later, still in-flight tx has written the key again ("it was
//     overwritten by an active tx"). Keep it pinned -- its value is not committed.
//   - entry older: committed state is ahead of anything we hold. Impossible under
//     ordered single-submitter commit; handled by taking the committed record,
//     which is strictly newer and authoritative, and counting the event.
//
// MUST be called only after the watermark says every batch formed before this
// notification's refresh has been authed (see watermark.Advance). Demoting early
// is precisely the bug the watermark exists to prevent: a batch still queued may
// carry a pre-commit read of the key, and once the key reads `read` the fast path
// accepts that stale version and the committer aborts it -- repeatably, because
// the retry re-warms from the same entry.
func (c *AuthCache) Demote(committed []StreamKV) {
	for _, kv := range committed {
		e, ok := c.entries[kv.Key]
		if !ok || !e.write {
			continue // evicted, never cached, or already read: nothing to demote
		}
		switch {
		case e.rec.Version == kv.Version:
			c.demote(kv.Key, e)
		case e.rec.Version > kv.Version:
			// A later in-flight tx wrote it again; stay pinned.
		default:
			e.rec = blocks.WriteRecord{
				Namespace: c.namespace,
				Key:       kv.Key,
				Version:   kv.Version,
				Value:     kv.Value,
				IsDelete:  kv.IsDelete,
			}
			c.unexpectedDemotes++
			c.demote(kv.Key, e)
		}
	}
}

// demote flips one entry to read state and returns it to the read half's byte
// accounting, which makes it evictable again.
func (c *AuthCache) demote(key string, e *authEntry) {
	e.write = false
	e.dirty = false
	e.bytes = authEntryBytes(key, &e.rec)
	c.readBytes += e.bytes
}
```

Add the `unexpectedDemotes uint64` field to the `AuthCache` struct next to `staleReadRejects`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -race -run 'TestAuthCache|TestApplyWrites|TestNextAggregate|TestEvict|TestStale|TestDemote' -v`
Expected: PASS (14 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_cache.go gateway/core/stream_auth_cache_test.go
git commit -m "feat(stream): auth cache write-to-read demotion on commit"
```

---
## Phase 3 — The watermark

The watermark is what makes the accept-as-is fast path safe. It is worth restating in the exact terms the implementation uses, because every off-by-one here is a silent MVCC abort:

A commit notification is stamped with **S**, the number the TXs worker will assign to the **next** batch it forms, sampled *after* the warmup cache refresh. Then:

- Every batch numbered **below S** was formed before the refresh, so it may carry a pre-commit read of a committed key. Holding the demotion until all of them have been authed guarantees each is validated while the key is still `write`, so it re-executes instead of accepting its stale warm read.
- Every batch numbered **at or above S** was formed after the refresh, so it read the key at its committed version and needs no protection.

So the notification becomes safe to apply once every batch `< S` has been authed — i.e. once `previous-batch-number >= S - 1`. The implementation therefore files each notification under `readyAfter = S - 1` and releases it when the watermark passes that number, which is literally the design's "go over all notification items from notification-map starting from the previous-batch-number we started with before the update, up to the one we landed on".

Batch numbers start at 0, so `previous-batch-number` starts at **-1** ("no batch authed yet") and is a signed integer. `S = 0` yields `readyAfter = -1`: nothing was formed before the refresh, so the notification is safe immediately.

Release order is commit order, and it comes out right for free: the warmup notification worker is a single goroutine processing notifications in commit order and sampling a monotonically non-decreasing counter, so `readyAfter` is non-decreasing in commit order. Releasing in ascending `readyAfter` (and, within one value, in insertion order) is therefore exactly commit order.

### Task 13: `watermark` — previous-batch-number, done-batch set, notification map

**Files:**
- Create: `gateway/core/stream_watermark.go`
- Test: `gateway/core/stream_watermark_test.go`

**Interfaces:**
- Consumes: `StreamKV` (Task 1).
- Produces: `func newWatermark() *watermark`, and on `*watermark`:
  - `Hold(stamp uint64, committed []StreamKV) []StreamKV` — file a stamped notification; returns the keys if already safe.
  - `Advance(batch uint64) []StreamKV` — record that `batch` has been authed; returns every held notification that just became safe, in commit order.
  - `Prev() int64`, `HeldLen() int`, `DoneLen() int`.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_watermark_test.go
package core

import "testing"

func keysOf(committed []StreamKV) []string {
	out := make([]string, 0, len(committed))
	for _, kv := range committed {
		out = append(out, kv.Key)
	}
	return out
}

func kv(key string) StreamKV { return StreamKV{Key: key, Version: 1} }

func TestWatermarkStartsBeforeBatchZero(t *testing.T) {
	w := newWatermark()
	if w.Prev() != -1 {
		t.Fatalf("Prev = %d, want -1 (no batch authed yet, and batch numbers start at 0)", w.Prev())
	}
}

// A notification stamped 0 was refreshed before ANY batch was formed, so nothing
// below it can carry a stale read: it is safe immediately.
func TestHoldReleasesImmediatelyWhenStampIsZero(t *testing.T) {
	w := newWatermark()
	got := w.Hold(0, []StreamKV{kv("a")})
	if len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("Hold(0) = %v, want it released immediately", keysOf(got))
	}
	if w.HeldLen() != 0 {
		t.Errorf("HeldLen = %d, want 0", w.HeldLen())
	}
}

// Stamp S is safe once every batch below S has been authed, i.e. once Prev
// reaches S-1. Not before.
func TestHoldWaitsForEveryBatchBelowTheStamp(t *testing.T) {
	w := newWatermark()
	if got := w.Hold(2, []StreamKV{kv("a")}); got != nil {
		t.Fatalf("stamp 2 must wait for batches 0 and 1; got %v", keysOf(got))
	}
	if got := w.Advance(0); got != nil {
		t.Fatalf("after batch 0 (Prev=0), stamp 2 still needs batch 1; got %v", keysOf(got))
	}
	got := w.Advance(1)
	if len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("after batch 1 (Prev=1 = stamp-1), stamp 2 must release; got %v", keysOf(got))
	}
	if w.HeldLen() != 0 {
		t.Errorf("HeldLen = %d after release, want 0", w.HeldLen())
	}
}

// Warm batches complete out of numeric order, so the auth worker authes them out
// of order. A batch more than one past Prev goes into the done set and is folded
// in when the gap closes.
func TestAdvanceOutOfOrderFillsTheGapFromTheDoneSet(t *testing.T) {
	w := newWatermark()
	w.Hold(4, []StreamKV{kv("a")}) // needs batches 0..3

	if got := w.Advance(3); got != nil || w.Prev() != -1 {
		t.Fatalf("batch 3 arriving first must be parked: got %v Prev=%d", keysOf(got), w.Prev())
	}
	if got := w.Advance(1); got != nil || w.Prev() != -1 {
		t.Fatalf("batch 1 still leaves a gap at 0: got %v Prev=%d", keysOf(got), w.Prev())
	}
	if w.DoneLen() != 2 {
		t.Fatalf("DoneLen = %d, want 2 (batches 1 and 3 parked)", w.DoneLen())
	}
	if got := w.Advance(0); got != nil || w.Prev() != 1 {
		t.Fatalf("batch 0 must fold in batch 1 and stop at the gap: got %v Prev=%d", keysOf(got), w.Prev())
	}
	got := w.Advance(2)
	if w.Prev() != 3 {
		t.Fatalf("batch 2 must fold in batch 3: Prev = %d, want 3", w.Prev())
	}
	if len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("reaching Prev=3 must release stamp 4; got %v", keysOf(got))
	}
	if w.DoneLen() != 0 {
		t.Errorf("DoneLen = %d, want 0 once the gap is closed", w.DoneLen())
	}
}

// Several notifications can share a readyAfter, and several readyAfter values can
// come due in one Advance. Both must release in commit order.
func TestAdvanceReleasesInCommitOrder(t *testing.T) {
	w := newWatermark()
	w.Hold(1, []StreamKV{kv("first")})  // readyAfter 0
	w.Hold(1, []StreamKV{kv("second")}) // readyAfter 0, later in commit order
	w.Hold(2, []StreamKV{kv("third")})  // readyAfter 1
	w.Advance(1)                           // parked: gap at 0

	got := w.Advance(0) // Prev jumps -1 -> 1, releasing readyAfter 0 and 1
	want := []string{"first", "second", "third"}
	if len(got) != 3 {
		t.Fatalf("released %v, want all three", keysOf(got))
	}
	for i, k := range want {
		if got[i].Key != k {
			t.Fatalf("released %v, want %v (commit order)", keysOf(got), want)
		}
	}
}

// A batch number at or below Prev has already been counted. Re-advancing must not
// move the watermark or release anything.
func TestAdvanceIsIdempotentBelowPrev(t *testing.T) {
	w := newWatermark()
	w.Advance(0)
	if got := w.Advance(0); got != nil || w.Prev() != 0 {
		t.Fatalf("re-advancing batch 0 changed state: got %v Prev=%d", keysOf(got), w.Prev())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestWatermark|TestHold|TestAdvance' -v`
Expected: FAIL — `undefined: newWatermark`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_watermark.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import "sort"

// watermark decides WHEN a commit notification's write->read demotion is safe to
// apply, and it is the reason the auth phase's accept-as-is fast path cannot
// accept a stale read.
//
// A notification carries a stamp S: the number the warmup TXs worker will give
// the NEXT batch it forms, sampled AFTER the warmup cache was refreshed with this
// notification's committed keys. That splits every batch in the system in two:
//
//   - numbered BELOW S: formed before the refresh, so it may carry a pre-commit
//     read of one of these keys. Holding the demotion until all of them have been
//     authed guarantees each is validated while the key is still in write state,
//     so it re-executes rather than accepting its stale read.
//   - numbered AT OR ABOVE S: formed after the refresh, so it read the committed
//     version and needs no protection.
//
// The notification is therefore safe once every batch below S has been authed,
// i.e. once prev >= S-1. Notifications are filed under readyAfter = S-1 and
// released when prev passes it.
//
// Auth completes batches OUT of numeric order (warm batches finish out of order
// and the auth worker takes whatever arrives), so prev cannot simply be the last
// batch authed: it is the highest number such that every batch up to and including
// it has been authed. Batches beyond prev+1 wait in done until the gap closes.
//
// Used only by the single auth worker: no synchronization.
type watermark struct {
	// prev is the highest batch number N such that every batch 0..N has been
	// authed. -1 means none has (batch numbers start at 0).
	prev int64
	// done holds authed batch numbers above prev+1, waiting for the gap to close.
	done map[uint64]struct{}
	// held maps readyAfter -> the committed keys waiting for prev to reach it, in
	// commit order within each entry.
	held map[int64][]StreamKV
}

func newWatermark() *watermark {
	return &watermark{
		prev: -1,
		done: make(map[uint64]struct{}),
		held: make(map[int64][]StreamKV),
	}
}

// Prev reports the watermark: every batch up to and including this number has
// been authed. -1 means none has.
func (w *watermark) Prev() int64 { return w.prev }

// HeldLen reports how many readyAfter buckets are waiting (observability).
func (w *watermark) HeldLen() int { return len(w.held) }

// DoneLen reports how many out-of-order authed batches are parked (observability).
func (w *watermark) DoneLen() int { return len(w.done) }

// Hold files a stamped notification and returns its committed keys immediately if
// they are already safe -- i.e. every batch below the stamp has been authed.
// Otherwise it returns nil and the keys are released by a later Advance.
//
// Callers MUST apply what Hold returns before any subsequent Hold/Advance, so
// demotions stay in commit order.
func (w *watermark) Hold(stamp uint64, committed []StreamKV) []StreamKV {
	readyAfter := int64(stamp) - 1
	if readyAfter <= w.prev {
		// Safe now. Nothing already held can be older: held entries all have
		// readyAfter > prev >= this one, so they were stamped later, i.e. they come
		// later in commit order.
		return committed
	}
	w.held[readyAfter] = append(w.held[readyAfter], committed...)
	return nil
}

// Advance records that batch has been authed, folds in any contiguous run parked
// in the done set, and returns every held notification that just became safe, in
// commit order (ascending readyAfter, insertion order within each). A batch at or
// below prev has already been counted and is ignored.
func (w *watermark) Advance(batch uint64) []StreamKV {
	b := int64(batch)
	if b <= w.prev {
		return nil
	}
	if b != w.prev+1 {
		w.done[batch] = struct{}{} // out of order: wait for the gap to close
		return nil
	}

	old := w.prev
	w.prev = b
	for {
		next := uint64(w.prev + 1)
		if _, ok := w.done[next]; !ok {
			break
		}
		delete(w.done, next)
		w.prev++
	}

	if len(w.held) == 0 {
		return nil
	}
	// Collect the readyAfter values that just came due and release them in
	// ascending order, which is commit order.
	due := make([]int64, 0, len(w.held))
	for r := range w.held {
		if r > old && r <= w.prev {
			due = append(due, r)
		}
	}
	if len(due) == 0 {
		return nil
	}
	sort.Slice(due, func(i, j int) bool { return due[i] < due[j] })
	var out []StreamKV
	for _, r := range due {
		out = append(out, w.held[r]...)
		delete(w.held, r)
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestWatermark|TestHold|TestAdvance' -race -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_watermark.go gateway/core/stream_watermark_test.go
git commit -m "feat(stream): auth-phase watermark for commit-notification demotion"
```

---
## Phase 4 — The warmup phase workers

### Task 14: `warmStore` — the warm read path

**Files:**
- Create: `gateway/core/stream_warm_worker.go`
- Test: `gateway/core/stream_warm_worker_test.go`

**Interfaces:**
- Consumes: `WarmCache` (Tasks 8–9); `execution.ReadStore`.
- Produces: `func newWarmStore(cache *WarmCache, under execution.ReadStore) *warmStore`, with `Get(namespace, key string) (*blocks.WriteRecord, error)` and `Close() error`. Satisfies `api.StateReader` and `execution.ReadStore`.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_warm_worker_test.go
package core

import (
	"errors"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// streamFakeReader is a ReadStore stand-in for the query service: it counts Gets,
// can fail, and is safe for the concurrent access warm workers make.
type streamFakeReader struct {
	mu     sync.Mutex
	recs   map[string]*blocks.WriteRecord
	gets   int
	closed bool
	err    error
}

func newStreamFakeReader(recs map[string]*blocks.WriteRecord) *streamFakeReader {
	if recs == nil {
		recs = map[string]*blocks.WriteRecord{}
	}
	return &streamFakeReader{recs: recs}
}

func (f *streamFakeReader) Get(_, key string) (*blocks.WriteRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.err != nil {
		return nil, f.err
	}
	rec, ok := f.recs[key]
	if !ok {
		return nil, nil
	}
	copied := *rec
	return &copied, nil
}

func (f *streamFakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *streamFakeReader) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets
}

func TestWarmStoreFillsOnMissAndServesFromCacheAfterwards(t *testing.T) {
	under := newStreamFakeReader(map[string]*blocks.WriteRecord{"k": {Key: "k", Version: 3, Value: []byte("v")}})
	cache := NewWarmCache("ns", 1<<20)
	s := newWarmStore(cache, under)

	first, err := s.Get("ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != 3 {
		t.Fatalf("Version = %d, want 3", first.Version)
	}
	second, err := s.Get("ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != 3 {
		t.Fatalf("Version = %d, want 3", second.Version)
	}
	if under.getCount() != 1 {
		t.Errorf("query service was read %d times, want 1 -- the second read must hit the cache", under.getCount())
	}
}

func TestWarmStoreDoesNotCacheAbsentKeys(t *testing.T) {
	under := newStreamFakeReader(nil)
	s := newWarmStore(NewWarmCache("ns", 1<<20), under)
	for range 3 {
		rec, err := s.Get("ns", "missing")
		if err != nil || rec != nil {
			t.Fatalf("Get = (%v, %v), want (nil, nil)", rec, err)
		}
	}
	if under.getCount() != 3 {
		t.Errorf("query service was read %d times, want 3 -- absence is not cached", under.getCount())
	}
}

func TestWarmStorePropagatesReadErrors(t *testing.T) {
	boom := errors.New("query service down")
	s := newWarmStore(NewWarmCache("ns", 1<<20), &streamFakeReader{err: boom})
	if _, err := s.Get("ns", "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// Close must NOT close the shared query-service reader: one reader serves every
// warm batch for the pipeline's whole life, and each batch builds its own store.
func TestWarmStoreCloseLeavesTheSharedReaderOpen(t *testing.T) {
	under := newStreamFakeReader(nil)
	s := newWarmStore(NewWarmCache("ns", 1<<20), under)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	under.mu.Lock()
	closed := under.closed
	under.mu.Unlock()
	if closed {
		t.Fatal("warmStore.Close must not close the shared query-service reader")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestWarmStore -v`
Expected: FAIL — `undefined: newWarmStore`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_warm_worker.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// warmStore is the warmup phase's read path: the committed-only warmup cache,
// falling back to the query service on a miss and filling the cache with what it
// finds. One is built per warm batch; they all share the one cache and the one
// long-lived query-service reader.
//
// The fallback deliberately uses no view (see query.DirectReader): a pinned view
// would reintroduce the commit-visibility skew this design removes, and its
// per-view memoization would silently defeat the cache refresh. Reads go through a
// CONNECTION POOL rather than one connection because concurrent reads on a single
// HTTP/2 connection serialize -- a sweep measured 4212 tx/s at 1 connection rising
// to a ~5040 tx/s plateau at 4-16 and regressing at 32.
//
// Safe for concurrent use: every warm worker in a batch reads through the same
// instance.
type warmStore struct {
	cache *WarmCache
	under execution.ReadStore
}

func newWarmStore(cache *WarmCache, under execution.ReadStore) *warmStore {
	return &warmStore{cache: cache, under: under}
}

// Get serves key from the warmup cache, else reads it from the query service and
// fills the cache. An absent key is returned as (nil, nil) and is NOT cached (see
// WarmCache.Fill).
func (s *warmStore) Get(namespace, key string) (*blocks.WriteRecord, error) {
	if rec, ok := s.cache.Get(key); ok {
		return rec, nil
	}
	rec, err := s.under.Get(namespace, key)
	if err != nil {
		return nil, err
	}
	s.cache.Fill(key, rec)
	return rec, nil
}

// Close is a no-op. The underlying query-service reader is shared by every warm
// batch for the pipeline's lifetime and is closed by the pipeline, not by a
// per-batch store.
func (s *warmStore) Close() error { return nil }

var _ execution.ReadStore = (*warmStore)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestWarmStore -race -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_warm_worker.go gateway/core/stream_warm_worker_test.go
git commit -m "feat(stream): warm-phase read path over the committed-only cache"
```

---

### Task 15: The TXs worker and the batch workers

**Files:**
- Modify: `gateway/core/stream_warm_worker.go` (append)
- Test: `gateway/core/stream_warm_worker_test.go` (append)

**Interfaces:**
- Consumes: `warmStore` (Task 14), `WarmCache.Sweep` (Task 8), `StreamConfig` / `WarmedBatchResult` (Task 1), `RecordWarmPhaseDuration` (existing, `gateway/core/metrics_hooks.go:27`).
- Produces:
  - `type warmExecutor interface { WarmBatchOn(ctx context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error) }`
  - `func newStreamTx(tx *types.Transaction, ex execution.ExecutedTx) *StreamTx` and `func (t *StreamTx) setFrom(ex execution.ExecutedTx)`
  - `func newWarmPhase(cfg StreamConfig, cache *WarmCache, under execution.ReadStore, exec warmExecutor, in <-chan *types.Transaction, out chan<- WarmedBatchResult, fatal func(error)) *warmPhase`
  - `func (w *warmPhase) Start(ctx context.Context)` / `func (w *warmPhase) Wait()` / `func (w *warmPhase) NextBatchNumber() uint64`
  - `func resetTimer(t *time.Timer, d time.Duration)`

**Invariant this task establishes, and which every later phase depends on:** *every batch number the TXs worker assigns must eventually reach the auth worker.* The watermark advances only on batches the auth worker sees, so a dropped number stalls every demotion behind it forever. The only paths that do not deliver are a fatal warm error and shutdown, both of which stop the pipeline outright. Phase-2's rollback must re-establish it by resetting the watermark bookkeeping (optimization.md rollback step 5).

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_warm_worker_test.go (append)

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// streamTx builds an unsigned tx: the fakes below never execute it, they only
// carry it, so a signature would be noise.
func streamTx(nonce uint64) *types.Transaction {
	return types.NewTx(&types.LegacyTx{Nonce: nonce, Gas: 21000, GasPrice: big.NewInt(1)})
}

type fakeWarmExec struct {
	mu      sync.Mutex
	batches [][]*types.Transaction
	err     error
	block   chan struct{}                  // when non-nil, every call waits on it
	onStore func(store api.StateReader)    // called before blocking, with the batch's store
}

func (f *fakeWarmExec) WarmBatchOn(_ context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error) {
	if f.onStore != nil {
		f.onStore(store)
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	f.batches = append(f.batches, txs)
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return make([]execution.ExecutedTx, len(txs)), nil
}

func (f *fakeWarmExec) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func startWarmPhase(t *testing.T, cfg StreamConfig, exec warmExecutor, cache *WarmCache) (in chan *types.Transaction, out chan WarmedBatchResult, fatals chan error, stop func()) {
	t.Helper()
	cfg.Sanitize()
	in = make(chan *types.Transaction, cfg.WarmQueueSize)
	out = make(chan WarmedBatchResult, cfg.WarmBatchQueueSize)
	fatals = make(chan error, 4)
	if cache == nil {
		cache = NewWarmCache("ns", 1<<20)
	}
	w := newWarmPhase(cfg, cache, newStreamFakeReader(nil), exec, in, out,
		func(err error) { select { case fatals <- err: default: } })
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	return in, out, fatals, func() { cancel(); w.Wait() }
}

// Batch numbers must be assigned from 0, monotonically, with NO gaps: the auth
// watermark advances one increment at a time and a gap would stall it forever.
func TestTxsWorkerNumbersBatchesFromZeroWithoutGaps(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 2
	in, out, _, stop := startWarmPhase(t, cfg, &fakeWarmExec{}, nil)
	defer stop()

	for i := range 6 {
		in <- streamTx(uint64(i))
	}
	seen := map[uint64]bool{}
	for range 3 {
		select {
		case wb := <-out:
			if len(wb.Txs) != 2 {
				t.Errorf("batch %d has %d txs, want 2", wb.Batch, len(wb.Txs))
			}
			for _, tx := range wb.Txs {
				if tx == nil {
					t.Errorf("batch %d delivered a nil internal tx", wb.Batch)
				}
			}
			seen[wb.Batch] = true
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a warm batch")
		}
	}
	for n := range uint64(3) {
		if !seen[n] {
			t.Errorf("batch number %d was never delivered; seen = %v", n, seen)
		}
	}
}

func TestTxsWorkerFlushesAShortBatchOnTimeout(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 100
	cfg.WarmBatchTimeout = 20 * time.Millisecond
	in, out, _, stop := startWarmPhase(t, cfg, &fakeWarmExec{}, nil)
	defer stop()

	in <- streamTx(0)
	select {
	case wb := <-out:
		if len(wb.Txs) != 1 {
			t.Fatalf("flushed %d txs, want the single queued one", len(wb.Txs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a short batch must flush on the batch timeout")
	}
}

// MaxInflightTxs bounds txs inside the phase, so with a budget of exactly one
// batch a second batch cannot be dispatched until the first finishes.
func TestTxsWorkerRespectsTheInFlightLimit(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 2
	cfg.MaxInflightTxs = 2
	exec := &fakeWarmExec{block: make(chan struct{})}
	in, out, _, stop := startWarmPhase(t, cfg, exec, nil)
	defer stop()

	for i := range 4 {
		in <- streamTx(uint64(i))
	}
	time.Sleep(100 * time.Millisecond)
	if got := exec.batchCount(); got != 0 {
		t.Fatalf("batchCount = %d before release, want 0 (both batches blocked)", got)
	}
	close(exec.block)
	for range 2 {
		select {
		case <-out:
		case <-time.After(5 * time.Second):
			t.Fatal("both batches must complete once the in-flight slots are released")
		}
	}
}

// The cache sweep runs at the batch-formation boundary, not on the read path.
func TestTxsWorkerSweepsTheCacheAtTheBoundary(t *testing.T) {
	cache := NewWarmCache("ns", 4*warmEntryBytes("k0", warmRec("k0", 1, "0123456789")))
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 1
	filled := 0
	exec := &fakeWarmExec{onStore: func(store api.StateReader) {
		// Stand in for warm reads: fill several keys through the shared cache.
		for i := range 8 {
			k := "k" + string(rune('0'+i))
			cache.Fill(k, warmRec(k, 1, "0123456789"))
		}
		filled++
	}}
	in, out, _, stop := startWarmPhase(t, cfg, exec, cache)
	defer stop()

	for i := range 4 {
		in <- streamTx(uint64(i))
	}
	for range 4 {
		select {
		case <-out:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out")
		}
	}
	// Give the TXs worker a boundary after the last batch's fills.
	in <- streamTx(99)
	select {
	case <-out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if cache.Bytes() > cache.MaxBytes() {
		t.Errorf("cache Bytes = %d, want <= MaxBytes = %d after a boundary sweep", cache.Bytes(), cache.MaxBytes())
	}
}

func TestWarmBatchErrorIsFatal(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 1
	in, _, fatals, stop := startWarmPhase(t, cfg, &fakeWarmExec{err: errors.New("warm exploded")}, nil)
	defer stop()

	in <- streamTx(0)
	select {
	case err := <-fatals:
		if err == nil {
			t.Fatal("expected a non-nil fatal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a warm-batch failure must be reported as fatal: the batch number can never reach the auth worker, so the watermark would stall")
	}
}

// NextBatchNumber is what the warmup notification worker samples for the
// watermark stamp; it must be the number the NEXT batch will get.
func TestNextBatchNumberIsTheNumberTheNextBatchWillGet(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 1
	cache := NewWarmCache("ns", 1<<20)
	cfg.Sanitize()
	in := make(chan *types.Transaction, 8)
	out := make(chan WarmedBatchResult, 8)
	w := newWarmPhase(cfg, cache, newStreamFakeReader(nil), &fakeWarmExec{}, in, out, func(error) {})
	if w.NextBatchNumber() != 0 {
		t.Fatalf("NextBatchNumber = %d before any batch, want 0", w.NextBatchNumber())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); w.Wait() }()
	w.Start(ctx)
	in <- streamTx(0)
	wb := <-out
	if wb.Batch != 0 {
		t.Fatalf("first batch number = %d, want 0", wb.Batch)
	}
	if w.NextBatchNumber() != 1 {
		t.Fatalf("NextBatchNumber = %d after batch 0, want 1", w.NextBatchNumber())
	}
}
```

Merge the new imports into the existing import block of the test file rather than adding a second one.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestTxsWorker|TestWarmBatchError|TestNextBatchNumber' -v`
Expected: FAIL — `undefined: newWarmPhase`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_warm_worker.go (append)

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// warmExecutor is the slice of the endorsement client the warmup phase needs.
// Narrow by design: it keeps the phase unit-testable without an endorser, and it
// documents that warming is the only thing this phase asks of the endorser.
type warmExecutor interface {
	WarmBatchOn(ctx context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error)
}

// warmPhase is the warmup phase: one TXs worker forming numbered batches from the
// input queue, and one goroutine per dispatched batch executing it concurrently
// against the shared warmup cache.
//
// BATCH NUMBER INVARIANT: every number this phase assigns MUST eventually reach
// the auth worker. The auth watermark advances one increment at a time, so a
// number that never arrives stalls every commit-notification demotion behind it
// forever -- and with demotions stalled, every tx reading those keys re-executes
// indefinitely. The only paths that do not deliver are a fatal warm error and
// shutdown, and both stop the pipeline outright.
type warmPhase struct {
	cfg   StreamConfig
	cache *WarmCache
	under execution.ReadStore
	exec  warmExecutor

	in  <-chan *types.Transaction
	out chan<- WarmedBatchResult

	// batchIndex is the next batch number. The TXs worker claims one with
	// Add(1)-1, so a concurrent Load() is exactly "the number the next batch will
	// get" -- which is what the warmup notification worker stamps (see
	// NextBatchNumber).
	batchIndex atomic.Uint64

	// inflight is the in-flight-txs limit as a semaphore: one token per tx, taken
	// by the TXs worker before it dispatches a batch and returned by the batch
	// worker when it is done. A channel rather than a bare counter because the
	// limit has to BLOCK the TXs worker, which is the whole point of the limit.
	inflight chan struct{}

	fatal func(error)
	wg    sync.WaitGroup
}

func newWarmPhase(cfg StreamConfig, cache *WarmCache, under execution.ReadStore, exec warmExecutor,
	in <-chan *types.Transaction, out chan<- WarmedBatchResult, fatal func(error)) *warmPhase {
	return &warmPhase{
		cfg:      cfg,
		cache:    cache,
		under:    under,
		exec:     exec,
		in:       in,
		out:      out,
		inflight: make(chan struct{}, cfg.MaxInflightTxs),
		fatal:    fatal,
	}
}

// NextBatchNumber is the number the TXs worker will assign to the next batch it
// forms. The warmup notification worker samples it -- AFTER refreshing the cache
// -- as the watermark stamp; see runWarmNotifier and watermark.
func (w *warmPhase) NextBatchNumber() uint64 { return w.batchIndex.Load() }

// Start launches the TXs worker. Batch workers are spawned by it.
func (w *warmPhase) Start(ctx context.Context) {
	w.wg.Go(func() { w.runTxs(ctx) })
}

// Wait blocks until the TXs worker and every dispatched batch worker have exited.
func (w *warmPhase) Wait() { w.wg.Wait() }

// runTxs consumes the input queue, batches txs by size or timeout, numbers each
// batch and dispatches it to a batch worker.
func (w *warmPhase) runTxs(ctx context.Context) {
	timer := time.NewTimer(w.cfg.WarmBatchTimeout)
	defer timer.Stop()
	batch := make([]*types.Transaction, 0, w.cfg.WarmBatchSize)

	// flush numbers and dispatches the accumulated batch. It returns false only on
	// shutdown, when the caller must stop.
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		// Reserve one in-flight slot per TX before dispatching, so the limit bounds
		// transactions inside the phase (what the design specifies) rather than
		// batches. Blocking here is the backpressure that keeps memory and
		// query-service load bounded.
		for range batch {
			select {
			case w.inflight <- struct{}{}:
			case <-ctx.Done():
				return false
			}
		}
		// Claim the number, then dispatch. The claim must precede the batch's first
		// READ, which it does: reads happen inside the batch worker. That ordering is
		// what makes the notification worker's "refresh, then sample" stamp sound.
		num := w.batchIndex.Add(1) - 1
		txs := batch
		batch = make([]*types.Transaction, 0, w.cfg.WarmBatchSize)

		w.wg.Go(func() { w.runBatch(ctx, num, txs) })

		// Enforce the warmup cache's byte budget here, at a boundary, and never on
		// the read path. The design specifies MFU plus a byte cap but not who
		// sweeps; the TXs worker is serial, already at a boundary, and off the hot
		// path. Sweep is a no-op while under budget.
		w.cache.Sweep()
		return true
	}

	for {
		select {
		case tx := <-w.in:
			batch = append(batch, tx)
			if len(batch) < w.cfg.WarmBatchSize {
				continue
			}
			if !flush() {
				return
			}
			resetTimer(timer, w.cfg.WarmBatchTimeout)
		case <-timer.C:
			if !flush() {
				return
			}
			timer.Reset(w.cfg.WarmBatchTimeout)
		case <-ctx.Done():
			return
		}
	}
}

// runBatch executes one numbered batch concurrently against the warmup cache and
// pushes the result -- batch number, txs, and one full read-write set per tx -- to
// the auth phase. It releases the batch's in-flight slots on every exit path.
func (w *warmPhase) runBatch(ctx context.Context, num uint64, txs []*types.Transaction) {
	defer func() {
		for range txs {
			<-w.inflight
		}
	}()

	start := time.Now()
	executed, err := w.exec.WarmBatchOn(ctx, txs, newWarmStore(w.cache, w.under))
	if RecordWarmPhaseDuration != nil {
		RecordWarmPhaseDuration(time.Since(start))
	}
	if err != nil {
		// This batch number can never reach the auth worker, so the watermark would
		// stall behind it: the failure is fatal to the pipeline, not to this batch.
		w.fatal(fmt.Errorf("warm batch %d (%d txs): %w", num, len(txs), err))
		return
	}

	// This is where the pipeline's internal transactions are born. From here on
	// every stage passes THESE along -- complete read-write sets, values included --
	// so nothing downstream has to ask a cache what a value was.
	out := make([]*StreamTx, len(txs))
	for i, ex := range executed {
		out[i] = newStreamTx(txs[i], ex)
	}

	select {
	case w.out <- WarmedBatchResult{Batch: num, Txs: out}:
	case <-ctx.Done():
	}
}

// newStreamTx builds an internal transaction from one execution outcome.
func newStreamTx(tx *types.Transaction, ex execution.ExecutedTx) *StreamTx {
	st := &StreamTx{Tx: tx}
	st.setFrom(ex)
	return st
}

// setFrom replaces the tx's outcome and read-write set with a fresh execution's.
// The auth phase calls it when it re-executes, so a re-executed tx carries the
// authoritative set and nothing of the discarded warm one.
//
// Read VERSIONS come from the result's read set (which is what the committer
// validates) and read VALUES from the captured records; a read with no version is
// marked Absent. Write versions are left zero here and stamped by
// AuthCache.ApplyWrites, which is the only place that knows them.
func (t *StreamTx) setFrom(ex execution.ExecutedTx) {
	t.Status = ex.Result.Status
	t.Event = ex.Result.Event

	values := make(map[string][]byte, len(ex.Reads))
	for i := range ex.Reads {
		values[ex.Reads[i].Key] = ex.Reads[i].Value
	}
	t.Reads = make([]StreamKV, 0, len(ex.Result.RWS.Reads))
	for _, rd := range ex.Result.RWS.Reads {
		kv := StreamKV{Key: rd.Key}
		if rd.Version == nil {
			kv.Absent = true
		} else {
			kv.Version = rd.Version.BlockNum
			kv.Value = values[rd.Key]
		}
		t.Reads = append(t.Reads, kv)
	}

	t.Writes = make([]StreamKV, 0, len(ex.Result.RWS.Writes))
	for _, w := range ex.Result.RWS.Writes {
		t.Writes = append(t.Writes, StreamKV{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete})
	}
}

// resetTimer restarts t for d, draining a already-fired channel so the next
// select does not see a stale tick.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
```

Merge these imports into the file's existing import block.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestWarmStore|TestTxsWorker|TestWarmBatchError|TestNextBatchNumber' -race -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_warm_worker.go gateway/core/stream_warm_worker_test.go
git commit -m "feat(stream): warmup TXs worker and concurrent batch workers"
```

---
## Phase 5 — The warmup notification worker

Two statements, in this order, and the order is the entire correctness argument:

```
cache.Refresh(committed)          // 1. make the committed value visible to warm reads
n.Batch = warm.NextBatchNumber()  // 2. THEN sample the counter
```

The worker runs concurrently with the TXs worker, so if the counter were sampled first, the batch **at** the stamp could be formed and read the key before the refresh landed — a stale read that is at-or-above the watermark, which the auth phase's fast path would accept. That abort is not self-correcting: the retry re-warms from the same never-refreshed entry.

The test for this task asserts the *ordering*, not just the result, because a refactor that swaps two adjacent lines would otherwise pass silently and fail only under load.

### Task 16: `runWarmNotifier` — refresh, then stamp

**Files:**
- Create: `gateway/core/stream_warm_notifier.go`
- Test: `gateway/core/stream_warm_notifier_test.go`

**Interfaces:**
- Consumes: `CommitNotification` / `StreamKV` (Task 1); `WarmCache.Refresh` (Task 9); `warmPhase.NextBatchNumber` (Task 15).
- Produces:
  - `type warmRefresher interface { Refresh(committed []StreamKV) }`
  - `type batchCounter interface { NextBatchNumber() uint64 }`
  - `func runWarmNotifier(ctx context.Context, cache warmRefresher, counter batchCounter, in <-chan CommitNotification, out chan<- CommitNotification)`

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_warm_notifier_test.go
package core

import (
	"context"
	"testing"
	"time"
)

// orderSpy records the sequence of the two operations whose order is the
// watermark's soundness requirement.
type orderSpy struct {
	calls     []string
	refreshed [][]StreamKV
	next      uint64
}

func (s *orderSpy) Refresh(committed []StreamKV) {
	s.calls = append(s.calls, "refresh")
	s.refreshed = append(s.refreshed, committed)
}

func (s *orderSpy) NextBatchNumber() uint64 {
	s.calls = append(s.calls, "sample")
	return s.next
}

func TestWarmNotifierRefreshesBeforeSamplingTheCounter(t *testing.T) {
	spy := &orderSpy{next: 7}
	in := make(chan CommitNotification, 1)
	out := make(chan CommitNotification, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runWarmNotifier(ctx, spy, spy, in, out)

	in <- CommitNotification{Committed: []StreamKV{{Key: "k", Version: 3, Value: []byte("v")}}}
	select {
	case got := <-out:
		if got.Batch != 7 {
			t.Errorf("Batch = %d, want the sampled next-batch-number 7", got.Batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notification was never forwarded")
	}

	if len(spy.calls) != 2 || spy.calls[0] != "refresh" || spy.calls[1] != "sample" {
		t.Fatalf("call order = %v, want [refresh sample]. Sampling first leaves a window where the batch AT the stamp reads the key before the refresh lands -- stale, above the watermark, and accepted by the fast path", spy.calls)
	}
	if len(spy.refreshed) != 1 || spy.refreshed[0][0].Key != "k" {
		t.Errorf("refreshed = %+v, want the notification's committed keys", spy.refreshed)
	}
}

// Commit order must survive the hop: the auth worker's watermark releases
// notifications in stamp order, which is only commit order if this worker
// forwards them in the order it received them.
func TestWarmNotifierPreservesCommitOrder(t *testing.T) {
	spy := &orderSpy{}
	in := make(chan CommitNotification, 3)
	out := make(chan CommitNotification, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runWarmNotifier(ctx, spy, spy, in, out)

	for _, k := range []string{"first", "second", "third"} {
		in <- CommitNotification{Committed: []StreamKV{{Key: k}}}
	}
	for _, want := range []string{"first", "second", "third"} {
		select {
		case got := <-out:
			if got.Committed[0].Key != want {
				t.Fatalf("got %q, want %q -- commit order must be preserved", got.Committed[0].Key, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}

func TestWarmNotifierExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runWarmNotifier(ctx, &orderSpy{}, &orderSpy{}, make(chan CommitNotification), make(chan CommitNotification))
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runWarmNotifier must return when the context is cancelled")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestWarmNotifier -v`
Expected: FAIL — `undefined: runWarmNotifier`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_warm_notifier.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import "context"

// warmRefresher is the warmup cache as this worker sees it.
type warmRefresher interface {
	Refresh(committed []StreamKV)
}

// batchCounter is the warmup TXs worker's batch counter as this worker sees it:
// the number the next batch will be given.
type batchCounter interface {
	NextBatchNumber() uint64
}

// runWarmNotifier is the warmup phase's notification worker. For each commit
// notification it does exactly two things, and THE ORDER IS THE CONTRACT:
//
//  1. refresh the warmup cache with the committed keys, so every batch formed
//     from now on reads the committed value;
//  2. THEN sample the batch counter and stamp the notification with the number
//     the next batch will get.
//
// This worker runs concurrently with the TXs worker, which is why the order
// matters. Sampling first would leave a window in which the batch AT the stamp is
// formed and reads the key before the refresh lands: a stale read, at-or-above the
// watermark, which the auth phase's fast path accepts because the key has been
// demoted by then -- and the committer aborts it repeatably, because the retry
// re-warms from the same never-refreshed entry.
//
// With the refresh first, the split is clean: every batch numbered at or above the
// stamp is formed after the refresh and reads committed state, and every batch
// below it is covered by the auth phase's write-state check (see watermark).
//
// Notifications are forwarded in arrival order, which is commit order. The auth
// watermark releases them in stamp order, and that is only commit order if this
// worker does not reorder them.
func runWarmNotifier(ctx context.Context, cache warmRefresher, counter batchCounter,
	in <-chan CommitNotification, out chan<- CommitNotification) {
	for {
		select {
		case n := <-in:
			cache.Refresh(n.Committed)
			n.Batch = counter.NextBatchNumber()
			select {
			case out <- n:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestWarmNotifier -race -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_warm_notifier.go gateway/core/stream_warm_notifier_test.go
git commit -m "feat(stream): warmup notification worker -- refresh then stamp"
```

---
## Phase 6 — The auth worker

One serial goroutine, two inputs, and the serial floor of the whole pipeline. Everything it touches — the auth cache, the watermark, the batch being assembled — is single-goroutine state with no synchronization, which is the point: this phase runs per tx, so a mutex per key access would show up directly in the throughput number.

### Task 17: `authStore` — the auth read path

**Files:**
- Create: `gateway/core/stream_auth_worker.go`
- Test: `gateway/core/stream_auth_worker_test.go`

**Interfaces:**
- Consumes: `AuthCache` (Tasks 10–12); `execution.ReadStore`.
- Produces: `func newAuthStore(cache *AuthCache, under execution.ReadStore) *authStore` with `Get`/`Close`.

The fallback here uses a **dedicated** query-service connection, not the warm phase's shared pool: the auth worker issues its reads one at a time, so it gains nothing from a pool and would instead have its latency perturbed by the warm phase's bursts sharing the same transports.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_worker_test.go
package core

import (
	"errors"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func TestAuthStoreCachesCommittedReads(t *testing.T) {
	under := newStreamFakeReader(map[string]*blocks.WriteRecord{"k": {Key: "k", Version: 4, Value: []byte("v")}})
	cache := NewAuthCache("ns", 1<<20)
	s := newAuthStore(cache, under)

	for range 3 {
		rec, err := s.Get("ns", "k")
		if err != nil {
			t.Fatal(err)
		}
		if rec.Version != 4 {
			t.Fatalf("Version = %d, want 4", rec.Version)
		}
	}
	if under.getCount() != 1 {
		t.Errorf("query service was read %d times, want 1", under.getCount())
	}
}

// An in-flight write shadows the query service entirely: that is what lets a later
// tx observe an earlier one's value without waiting for it to commit.
func TestAuthStoreServesInFlightWritesWithoutReadingTheQueryService(t *testing.T) {
	under := newStreamFakeReader(map[string]*blocks.WriteRecord{"k": {Key: "k", Version: 4, Value: []byte("committed")}})
	cache := NewAuthCache("ns", 1<<20)
	cache.ApplyWrites(streamTxOf(map[string]uint64{"k": 4}, map[string]string{"k": "inflight"}))
	s := newAuthStore(cache, under)

	rec, err := s.Get("ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Value) != "inflight" {
		t.Errorf("Value = %q, want the in-flight write %q", rec.Value, "inflight")
	}
	if under.getCount() != 0 {
		t.Errorf("query service was read %d times, want 0", under.getCount())
	}
}

func TestAuthStorePropagatesReadErrors(t *testing.T) {
	boom := errors.New("dedicated connection down")
	s := newAuthStore(NewAuthCache("ns", 1<<20), &streamFakeReader{err: boom})
	if _, err := s.Get("ns", "k"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestAuthStore -v`
Expected: FAIL — `undefined: newAuthStore`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_worker.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// authStore is the auth phase's read path: the auth cache -- which serves both
// committed reads and the pipeline's own in-flight writes -- falling back to a
// DEDICATED query-service connection, filling the cache with what it finds.
//
// Dedicated, not the warm phase's pool: the auth worker reads one key at a time,
// so it gains nothing from round-robining across connections, and sharing the warm
// phase's transports would let a warm burst perturb the latency of the pipeline's
// serial floor.
//
// NOT safe for concurrent use. It is touched only by the single auth worker, and
// that is what lets AuthCache take no locks at all.
type authStore struct {
	cache *AuthCache
	under execution.ReadStore
}

func newAuthStore(cache *AuthCache, under execution.ReadStore) *authStore {
	return &authStore{cache: cache, under: under}
}

// Get serves key from the auth cache, else reads it from the query service and
// admits it as a committed read. An absent key is (nil, nil) and is not cached.
func (s *authStore) Get(namespace, key string) (*blocks.WriteRecord, error) {
	if rec, ok := s.cache.Get(key); ok {
		return rec, nil
	}
	rec, err := s.under.Get(namespace, key)
	if err != nil {
		return nil, err
	}
	s.cache.PutRead(key, rec)
	return rec, nil
}

// Close is a no-op: the dedicated reader lives for the pipeline's lifetime and is
// closed by the pipeline.
func (s *authStore) Close() error { return nil }

var _ execution.ReadStore = (*authStore)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestAuthStore -race -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_worker.go gateway/core/stream_auth_worker_test.go
git commit -m "feat(stream): auth-phase read path over the write-state cache"
```

---

### Task 18: The auth worker loop — notification priority and submit batching

**Files:**
- Modify: `gateway/core/stream_auth_worker.go` (append)
- Test: `gateway/core/stream_auth_worker_test.go` (append)

**Interfaces:**
- Consumes: `authStore` (Task 17), `AuthCache.Seal` (Task 11), `watermark` (Task 13), `resetTimer` (Task 15), `WarmedBatchResult` / `CommitNotification` / `submitBatch` (Task 1).
- Produces:
  - `type authExecutor interface { AuthTxOn(ctx context.Context, tx *types.Transaction, store api.StateReader) (execution.ExecutedTx, error) }`
  - `type authDeps struct { Cfg StreamConfig; Namespace string; Cache *AuthCache; Under execution.ReadStore; Exec authExecutor; WarmIn <-chan WarmedBatchResult; NotifyIn <-chan CommitNotification; Out chan<- submitBatch; Requeue func(*types.Transaction); Fatal func(error); Trace func(kind string) }`
  - `func newAuthPhase(d authDeps) *authPhase`, `func (a *authPhase) Start(ctx context.Context)`, `func (a *authPhase) Wait()`
  - Counters: `Accepted() uint64`, `Reexecuted() uint64`

**Why strict notification priority is safe here.** Go's `select` picks uniformly at random among ready cases, so serving one queue first requires a nested select: try it with a `default`, and only then block on both. Strict priority normally risks starving the other case — but not here, because the notification stream is *downstream* of this worker. Notifications can only arrive for batches this worker already authed and submitted, so they can never arrive faster than it processes batches.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_worker_test.go (append)

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// fakeAuthExec records every re-execution and returns a canned result.
type fakeAuthExec struct {
	mu     sync.Mutex
	calls  []*types.Transaction
	result *StreamTx // the outcome to return, in internal form
	err    error
	// storeGets, when set, is invoked with the store so a test can observe what
	// the re-execution would read.
	storeGets func(store api.StateReader)
}

func (f *fakeAuthExec) AuthTxOn(_ context.Context, tx *types.Transaction, store api.StateReader) (execution.ExecutedTx, error) {
	f.mu.Lock()
	f.calls = append(f.calls, tx)
	f.mu.Unlock()
	if f.storeGets != nil {
		f.storeGets(store)
	}
	if f.err != nil {
		return execution.ExecutedTx{}, f.err
	}
	return executedFrom(f.result), nil
}

// executedFrom turns an internal tx back into an execution outcome, so a fake can
// be written in the same terms as the rest of these tests.
func executedFrom(st *StreamTx) execution.ExecutedTx {
	if st == nil {
		return execution.ExecutedTx{}
	}
	ex := execution.ExecutedTx{Result: endorsement.ExecutionResult{Status: st.Status, Event: st.Event}}
	for _, rd := range st.Reads {
		kv := blocks.KVRead{Key: rd.Key}
		if !rd.Absent {
			kv.Version = &blocks.Version{BlockNum: rd.Version}
			ex.Reads = append(ex.Reads, blocks.WriteRecord{Key: rd.Key, Version: rd.Version, Value: rd.Value})
		}
		ex.Result.RWS.Reads = append(ex.Result.RWS.Reads, kv)
	}
	for _, w := range st.Writes {
		ex.Result.RWS.Writes = append(ex.Result.RWS.Writes, blocks.KVWrite{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete})
	}
	return ex
}

func (f *fakeAuthExec) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// okTx is an internal tx the accept-as-is path will take: status OK, one versioned
// read (with its value, as warm always records) and one write.
func okTx(key string, readVer uint64, value string) *StreamTx {
	return &StreamTx{
		Tx:     streamTx(readVer),
		Status: 200,
		Reads:  []StreamKV{{Key: key, Version: readVer, Value: []byte("read")}},
		Writes: []StreamKV{{Key: key, Value: []byte(value)}},
	}
}

type authHarness struct {
	warmIn   chan WarmedBatchResult
	notifyIn chan CommitNotification
	out      chan submitBatch
	fatals   chan error
	requeued chan *types.Transaction
	trace    chan string
	cache    *AuthCache
	exec     *fakeAuthExec
	under    *streamFakeReader
	phase    *authPhase
	stop     func()
}

func startAuthPhase(t *testing.T, cfg StreamConfig, exec *fakeAuthExec) *authHarness {
	t.Helper()
	cfg.Sanitize()
	h := &authHarness{
		warmIn:   make(chan WarmedBatchResult, 8),
		notifyIn: make(chan CommitNotification, 8),
		out:      make(chan submitBatch, 8),
		fatals:   make(chan error, 4),
		requeued: make(chan *types.Transaction, 8),
		trace:    make(chan string, 32),
		cache:    NewAuthCache("ns", 1<<20),
		exec:     exec,
		under:    newStreamFakeReader(nil),
	}
	h.phase = newAuthPhase(authDeps{
		Cfg: cfg, Namespace: "ns", Cache: h.cache, Under: h.under, Exec: exec,
		WarmIn: h.warmIn, NotifyIn: h.notifyIn, Out: h.out,
		Requeue: func(tx *types.Transaction) { h.requeued <- tx },
		Fatal:   func(err error) { select { case h.fatals <- err: default: } },
		Trace:   func(kind string) { select { case h.trace <- kind: default: } },
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.phase.Start(ctx)
	h.stop = func() { cancel(); h.phase.Wait() }
	t.Cleanup(h.stop)
	return h
}

// With both queues ready, the notification queue must be served first. A plain
// select would pick at random, so this asserts the nested-select priority.
func TestAuthWorkerServesNotificationsBeforeBatches(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour // only the size rule may flush
	h := startAuthPhase(t, cfg, &fakeAuthExec{result: okTx("k", 1, "v")})

	// Queue both before the worker can possibly have drained either.
	for range 4 {
		h.notifyIn <- CommitNotification{Batch: 0, Committed: []StreamKV{{Key: "n", Version: 1}}}
		h.warmIn <- warmBatchOf(0, okTx("k", 1, "v"))
	}
	// The first four handled inputs must all be notifications.
	for i := range 4 {
		select {
		case kind := <-h.trace:
			if kind != "notification" {
				t.Fatalf("input %d handled as %q, want \"notification\" -- the notification queue must have strict priority", i, kind)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a traced input")
		}
	}
}

func TestAuthWorkerFlushesASubmitBatchBySize(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 3
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{result: okTx("k", 1, "v")})

	for n := range uint64(3) {
		h.warmIn <- warmBatchOf(n, okTx("k"+string(rune('a'+n)), 1, "v"))
	}
	select {
	case sb := <-h.out:
		if len(sb.txs) != 3 {
			t.Fatalf("submit batch has %d txs, want 3", len(sb.txs))
		}
		for i, tx := range sb.txs {
			if len(tx.Writes) != 1 || tx.Writes[0].Version == 0 {
				t.Fatalf("tx %d must reach the submitter with its write version stamped; writes = %+v", i, tx.Writes)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a full submit batch must be flushed")
	}
}

func TestAuthWorkerFlushesASubmitBatchOnTimeout(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 100
	cfg.AuthBatchTimeout = 20 * time.Millisecond
	h := startAuthPhase(t, cfg, &fakeAuthExec{result: okTx("k", 1, "v")})

	h.warmIn <- warmBatchOf(0, okTx("k", 1, "v"))
	select {
	case sb := <-h.out:
		if len(sb.txs) != 1 {
			t.Fatalf("flushed %d txs, want 1", len(sb.txs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a short submit batch must flush on the auth batch timeout")
	}
}

func TestAuthWorkerDoesNotFlushAnEmptyBatch(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchTimeout = 10 * time.Millisecond
	h := startAuthPhase(t, cfg, &fakeAuthExec{})
	select {
	case sb := <-h.out:
		t.Fatalf("an empty batch must never be submitted; got %+v", sb)
	case <-time.After(100 * time.Millisecond):
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestAuthWorker -v`
Expected: FAIL — `undefined: newAuthPhase`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_worker.go (append)

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// authExecutor is the slice of the endorsement client the auth phase needs: one
// tx, re-executed against a caller-supplied store.
type authExecutor interface {
	AuthTxOn(ctx context.Context, tx *types.Transaction, store api.StateReader) (execution.ExecutedTx, error)
}

// authDeps is authPhase's wiring, as a struct because a nine-parameter
// constructor is unreadable and easy to mis-order.
type authDeps struct {
	Cfg       StreamConfig
	Namespace string
	Cache     *AuthCache
	Under     execution.ReadStore // dedicated query-service connection
	Exec      authExecutor

	WarmIn   <-chan WarmedBatchResult
	NotifyIn <-chan CommitNotification
	Out      chan<- submitBatch

	// Requeue takes a tx whose rejection is retryable back to the tail of the
	// input queue.
	Requeue func(*types.Transaction)
	Fatal   func(error)
	// Trace, when non-nil, is called with "notification" or "batch" as each input
	// is handled. Test observability, in the same nil-guarded style as the metric
	// hooks; production leaves it nil.
	Trace func(kind string)
}

// authPhase is the auth worker: ONE goroutine consuming two queues, deciding per
// tx whether the warm read-write set can be accepted as-is or must be
// re-executed, assembling the submit batch, and applying commit notifications
// under the watermark.
//
// Every field is single-goroutine state after Start. That is deliberate and it is
// what lets AuthCache and watermark take no locks: the phase runs per TX, so
// synchronization here would land directly on the serial floor.
type authPhase struct {
	d     authDeps
	store *authStore
	mark  *watermark

	pend       []*StreamTx
	accepted   uint64
	reexecuted uint64

	wg sync.WaitGroup
}

func newAuthPhase(d authDeps) *authPhase {
	d.Cfg.Sanitize()
	return &authPhase{
		d:     d,
		store: newAuthStore(d.Cache, d.Under),
		mark:  newWatermark(),
		pend:  make([]*StreamTx, 0, d.Cfg.AuthBatchSize),
	}
}

// Accepted counts txs whose warm read-write set was taken verbatim.
func (a *authPhase) Accepted() uint64 { return a.accepted }

// Reexecuted counts txs the auth phase had to run again. Accepted/(Accepted+
// Reexecuted) is the pipeline's headline ratio: the overlap only pays off to the
// extent warm work survives.
func (a *authPhase) Reexecuted() uint64 { return a.reexecuted }

// Start launches the worker.
func (a *authPhase) Start(ctx context.Context) {
	a.wg.Go(func() { a.run(ctx) })
}

// Wait blocks until the worker has exited.
func (a *authPhase) Wait() { a.wg.Wait() }

func (a *authPhase) run(ctx context.Context) {
	timer := time.NewTimer(a.d.Cfg.AuthBatchTimeout)
	defer timer.Stop()

	for {
		// Notification-first. A plain select over both queues would NOT honour case
		// order -- Go picks uniformly at random among ready cases -- so serving
		// notifications first takes a nested select: try that queue with a default,
		// and only then block on both.
		//
		// Strict priority cannot starve the batch queue here, because the
		// notification stream is DOWNSTREAM of this worker: a notification can only
		// exist for a batch this worker already authed and submitted, so
		// notifications can never arrive faster than batches are processed.
		select {
		case n := <-a.d.NotifyIn:
			a.trace("notification")
			a.handleNotification(n)
			continue
		default:
		}

		select {
		case n := <-a.d.NotifyIn:
			a.trace("notification")
			a.handleNotification(n)
		case wb := <-a.d.WarmIn:
			a.trace("batch")
			if !a.handleBatch(ctx, wb) {
				return
			}
			if len(a.pend) >= a.d.Cfg.AuthBatchSize {
				if !a.flush(ctx) {
					return
				}
			}
			resetTimer(timer, a.d.Cfg.AuthBatchTimeout)
		case <-timer.C:
			if !a.flush(ctx) {
				return
			}
			timer.Reset(a.d.Cfg.AuthBatchTimeout)
		case <-ctx.Done():
			return
		}
	}
}

func (a *authPhase) trace(kind string) {
	if a.d.Trace != nil {
		a.d.Trace(kind)
	}
}

// flush hands the assembled aggregate to the submit queue and tells the cache a new
// one starts now.
//
// In this milestone the submit batch IS the auth batch, so this boundary and the
// submitter's aggregation boundary are the same one. That is what makes it correct
// to close the cache's aggregate here; when adaptive submit batching lands the two
// diverge and the NextAggregate call has to move with it.
//
// Returns false only on shutdown.
func (a *authPhase) flush(ctx context.Context) bool {
	if len(a.pend) == 0 {
		return true
	}
	txs := a.pend
	a.pend = make([]*StreamTx, 0, a.d.Cfg.AuthBatchSize)

	// Close the aggregate in the cache: the next write to any of these keys belongs
	// to the NEXT committer tx and must step its version again. Nothing is computed
	// here -- the submitter derives the committer read-write set and the committed
	// payload from the transactions themselves.
	a.d.Cache.NextAggregate()

	select {
	case a.d.Out <- submitBatch{txs: txs}:
		return true
	case <-ctx.Done():
		return false
	}
}
```

`handleBatch` and `handleNotification` arrive in Tasks 19 and 20. To keep this task's tests green, add them now in the minimal form the tests require:

```go
// handleBatch settles every tx in a warm batch. Tasks 19 and 20 replace the body;
// this form accepts every tx so the loop and batching can be tested first.
func (a *authPhase) handleBatch(ctx context.Context, wb WarmedBatchResult) bool {
	for _, tx := range wb.Txs {
		a.d.Cache.ApplyWrites(tx)
		a.pend = append(a.pend, tx)
		a.accepted++
	}
	_ = ctx
	return true
}

// handleNotification files a stamped notification. Task 20 replaces the body.
func (a *authPhase) handleNotification(n CommitNotification) {
	if committed := a.mark.Hold(n.Batch, n.Committed); len(committed) > 0 {
		a.d.Cache.Demote(committed)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestAuthStore|TestAuthWorker' -race -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_worker.go gateway/core/stream_auth_worker_test.go
git commit -m "feat(stream): auth worker loop with notification priority and submit batching"
```

---
### Task 19: Accept-as-is, re-execution, and prefill

**Files:**
- Modify: `gateway/core/stream_auth_worker.go` (replace `handleBatch`; add `acceptable`, `prefill`)
- Modify: `gateway/core/stream_auth_cache.go` (add `Has`)
- Test: `gateway/core/stream_auth_worker_test.go` (append)

**Interfaces:**
- Consumes: `AuthCache.Stale` / `ApplyWrites` / `PutRead` / `Has` (Tasks 10–11), `StreamTx.setFrom` (Task 15), `authExecutor` (Task 18), `RecordAuthPhaseDuration` (existing, `metrics_hooks.go:37`).
- Produces: `func (c *AuthCache) Has(key string) bool` (does **not** bump the use counter); the real `handleBatch`; `func (a *authPhase) acceptable(tx *StreamTx) bool`; `func (a *authPhase) prefill(tx *StreamTx)`.

**Prefill comes from the transaction, not from a cache.** The design says to add missing read keys before re-executing so the query service is not asked again. Because `StreamTx` carries the full read set — key, version **and value** — prefill is a straight copy out of the tx. Nothing is looked up, so nothing can have been evicted in the meantime, and `blocks.KVRead`'s missing value field stops being a problem.

The prefilled record is current. If key K's committed version had advanced past what this tx read, the advance came from one of our own committed txs; that tx's write marked K `write` in the auth cache and write entries are never evicted, so K would either still be `write` (prefill skips it — `Has` is true) or already demoted, which the watermark permits only after every batch below the stamp has been authed, i.e. only for batches that read K *after* the refresh.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_worker_test.go (append)

func warmBatchOf(num uint64, txs ...*StreamTx) WarmedBatchResult {
	return WarmedBatchResult{Batch: num, Txs: txs}
}

// The fast path: no read key is shadowed by an in-flight write, so the warm
// read-write set is taken verbatim and the EVM never runs again.
func TestAuthWorkerAcceptsCleanWarmResultsWithoutReexecuting(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{})

	h.warmIn <- warmBatchOf(0, okTx("k", 3, "v"))
	select {
	case sb := <-h.out:
		if len(sb.txs) != 1 {
			t.Fatalf("submit batch has %d txs, want 1", len(sb.txs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if h.exec.callCount() != 0 {
		t.Errorf("AuthTxOn called %d times, want 0 -- a clean warm result must be accepted as-is", h.exec.callCount())
	}
	if h.phase.Accepted() != 1 || h.phase.Reexecuted() != 0 {
		t.Errorf("Accepted=%d Reexecuted=%d, want 1 and 0", h.phase.Accepted(), h.phase.Reexecuted())
	}
}

// A read key an in-flight tx has written forces re-execution: the warm read saw
// committed state the pipeline itself has already superseded.
func TestAuthWorkerReexecutesWhenAReadKeyIsInWriteState(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{result: okTx("shadowed", 3, "reexec")})
	h.cache.ApplyWrites(streamTxOf(map[string]uint64{"shadowed": 3}, map[string]string{"shadowed": "inflight"}))

	h.warmIn <- warmBatchOf(0, okTx("shadowed", 3, "stale"))
	select {
	case sb := <-h.out:
		if string(sb.txs[0].Writes[0].Value) != "reexec" {
			t.Fatalf("submitted value = %q, want the re-executed %q", sb.txs[0].Writes[0].Value, "reexec")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if h.exec.callCount() != 1 {
		t.Errorf("AuthTxOn called %d times, want 1", h.exec.callCount())
	}
	if h.phase.Reexecuted() != 1 {
		t.Errorf("Reexecuted = %d, want 1", h.phase.Reexecuted())
	}
}

// Prefill: a re-execution must not pay a query-service round-trip for a key the tx
// already carries -- key, version AND value -- in its own read set.
func TestAuthWorkerPrefillsReExecutionReadsFromTheTx(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	exec := &fakeAuthExec{result: okTx("shadowed", 1, "v")}
	h := startAuthPhase(t, cfg, exec)
	// Re-execution reads both keys through the store.
	exec.storeGets = func(store api.StateReader) {
		_, _ = store.Get("ns", "shadowed")
		_, _ = store.Get("ns", "other")
	}
	h.cache.ApplyWrites(streamTxOf(map[string]uint64{"shadowed": 1}, map[string]string{"shadowed": "inflight"}))

	tx := &StreamTx{
		Tx:     streamTx(0),
		Status: 200,
		Reads: []StreamKV{
			{Key: "shadowed", Version: 1, Value: []byte("stale")},
			{Key: "other", Version: 5, Value: []byte("carried")},
		},
		Writes: []StreamKV{{Key: "shadowed", Value: []byte("v")}},
	}
	h.warmIn <- warmBatchOf(0, tx)
	select {
	case <-h.out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if h.under.getCount() != 0 {
		t.Errorf("query service was read %d times during re-execution, want 0 -- both keys are prefilled from the tx", h.under.getCount())
	}
	if got, ok := h.cache.Get("other"); !ok || string(got.Value) != "carried" {
		t.Errorf("prefilled record = (%v, %v), want the value the tx carried", got, ok)
	}
}

// Prefill must not shadow an in-flight write with the committed value the tx read:
// a key already in the cache is skipped, and a write entry is exactly such a key.
func TestAuthWorkerPrefillDoesNotShadowAnInFlightWrite(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	exec := &fakeAuthExec{result: okTx("shadowed", 1, "v")}
	h := startAuthPhase(t, cfg, exec)
	h.cache.ApplyWrites(streamTxOf(map[string]uint64{"shadowed": 1}, map[string]string{"shadowed": "inflight"}))

	tx := &StreamTx{
		Tx:     streamTx(0),
		Status: 200,
		Reads:  []StreamKV{{Key: "shadowed", Version: 1, Value: []byte("committed")}},
		Writes: []StreamKV{{Key: "shadowed", Value: []byte("v")}},
	}
	h.warmIn <- warmBatchOf(0, tx)
	select {
	case <-h.out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if !h.cache.IsWrite("shadowed") {
		t.Fatal("the key must still be in write state")
	}
	if got, _ := h.cache.Get("shadowed"); string(got.Value) != "inflight" {
		t.Errorf("cached value = %q, want the in-flight write %q -- prefill must not overwrite it", got.Value, "inflight")
	}
}

// A rejected warm result is never accepted as-is: its rejection reflects
// pre-batch state the auth phase may have moved past.
func TestAuthWorkerAlwaysReexecutesARejectedWarmResult(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{result: okTx("k", 1, "v")})

	rejected := &StreamTx{Tx: streamTx(0), Status: common.StatusTxRejected}
	h.warmIn <- warmBatchOf(0, rejected)
	select {
	case <-h.out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if h.exec.callCount() != 1 {
		t.Errorf("AuthTxOn called %d times, want 1 -- a rejected warm result must be re-executed", h.exec.callCount())
	}
}

func TestAuthWorkerDropsTerminalRejectionsAndRequeuesRetryableOnes(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = 20 * time.Millisecond

	terminal := startAuthPhase(t, cfg, &fakeAuthExec{result: &StreamTx{Status: common.StatusTxRejectedTerminal}})
	terminal.warmIn <- warmBatchOf(0, &StreamTx{Tx: streamTx(0), Status: common.StatusTxRejected})
	select {
	case sb := <-terminal.out:
		t.Fatalf("a terminally rejected tx must not be submitted; got %+v", sb.txs)
	case tx := <-terminal.requeued:
		t.Fatalf("a terminally rejected tx must not be requeued; got %v", tx.Hash())
	case <-time.After(200 * time.Millisecond):
	}

	retryable := startAuthPhase(t, cfg, &fakeAuthExec{result: &StreamTx{Status: common.StatusTxRejected}})
	retryable.warmIn <- warmBatchOf(0, &StreamTx{Tx: streamTx(0), Status: common.StatusTxRejected})
	select {
	case tx := <-retryable.requeued:
		if tx == nil {
			t.Fatal("requeued a nil tx")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a retryably rejected tx must be requeued to the input queue")
	}
}

func TestAuthReexecutionErrorIsFatal(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{err: errors.New("auth exploded")})
	h.cache.ApplyWrites(streamTxOf(map[string]uint64{"k": 1}, map[string]string{"k": "inflight"}))

	h.warmIn <- warmBatchOf(0, okTx("k", 1, "stale"))
	select {
	case err := <-h.fatals:
		if err == nil {
			t.Fatal("expected a non-nil fatal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an auth re-execution failure must be fatal: the batch number can never advance the watermark")
	}
}
```

Import `"github.com/hyperledger/fabric-x-evm/common"` in the test file. Note that some files in `gateway/core` bind the identifier `common` to go-ethereum's package instead; this file needs the EVM one, so keep them apart per file (see `gateway/core/endorse.go`, which aliases the ethereum one to `ethcommon`).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestAuthWorkerAccepts|TestAuthWorkerReexecutes|TestAuthWorkerPrefill|TestAuthWorkerAlways|TestAuthWorkerDrops|TestAuthReexecutionError' -v`
Expected: FAIL — every warm result is currently accepted, so `AuthTxOn called 0 times, want 1`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_cache.go (append)

// Has reports whether key has an entry, WITHOUT recording a use. Callers that
// want the record (and the use) call Get; prefill only needs to know whether the
// cache already knows the key.
func (c *AuthCache) Has(key string) bool {
	_, ok := c.entries[key]
	return ok
}
```

```go
// gateway/core/stream_auth_worker.go -- replace handleBatch and add these

// handleBatch settles every tx of one warm batch, in batch order.
//
// For each tx: if no read key of its warm read-write set is shadowed or stale (see
// acceptable) the set is taken VERBATIM -- the entire point of the warm phase, and
// the only path on which its work is not thrown away. Otherwise the tx is
// re-executed against the auth cache, after prefilling what the warmup cache can
// supply so the re-execution does not pay for reads warm already made.
//
// Either way the outcome is classified: a terminal rejection is dropped (it can
// never succeed as this exact tx stands), a retryable one goes back to the tail of
// the input queue, and anything else joins the submit batch after its writes enter
// the auth cache.
//
// Returns false only when the pipeline must stop.
func (a *authPhase) handleBatch(ctx context.Context, wb WarmedBatchResult) bool {
	start := time.Now()
	for _, tx := range wb.Txs {
		if a.acceptable(tx) {
			a.accepted++
		} else {
			a.prefill(tx)
			ex, err := a.d.Exec.AuthTxOn(ctx, tx.Tx, a.store)
			if err != nil {
				// The batch cannot be settled, so its number can never advance the
				// watermark and every demotion behind it would stall: fatal.
				a.d.Fatal(fmt.Errorf("auth re-execute %s (warm batch %d): %w", tx.Tx.Hash(), wb.Batch, err))
				return false
			}
			// Replace the warm read-write set wholesale: the accepted one is gone and
			// nothing downstream may see a mix of the two.
			tx.setFrom(ex)
			a.reexecuted++
		}

		switch tx.Status {
		case common.StatusTxRejectedTerminal:
			continue // nonce too low: can never succeed as submitted
		case common.StatusTxRejected:
			// Retryable (nonce too high, insufficient funds, ...). Requeueing does
			// not preserve input order, which is acceptable under CFT where the
			// gateway owns the order -- the same reasoning the design gives for
			// rollback.
			a.d.Requeue(tx.Tx)
			continue
		}

		// Stamps each write's version onto tx, which is what lets the submitter
		// aggregate without consulting the cache.
		a.d.Cache.ApplyWrites(tx)
		a.pend = append(a.pend, tx)
	}
	if RecordAuthPhaseDuration != nil {
		RecordAuthPhaseDuration(time.Since(start))
	}
	return true
}

// acceptable reports whether a warm result can be taken verbatim.
//
// A result whose status is not a committed outcome is never accepted: a client
// rejection during warmup reflects pre-batch state that the auth phase may have
// moved past (a nonce gap its predecessor has since filled), so it is always
// re-executed and re-classified.
//
// Otherwise the test is the design's: no read key may be shadowed by an in-flight
// write. AuthCache.Stale additionally refuses a read below a newer committed
// version it holds, which is what protects a key the WARMUP cache has evicted:
// there warm reads the query service, and the query service can still trail a
// commit the pipeline has already been notified of.
func (a *authPhase) acceptable(tx *StreamTx) bool {
	switch tx.Status {
	case common.StatusOK, common.StatusEVMRevert, common.StatusExecFailure:
	default:
		return false
	}
	for _, rd := range tx.Reads {
		if a.d.Cache.Stale(rd.Key, rd) {
			return false
		}
	}
	return true
}

// prefill seeds the auth cache with the records this re-execution is about to read,
// so it does not re-fetch keys the warm pass already resolved.
//
// The records come from the TX ITSELF. That is the point of carrying full values in
// StreamTx: the read set has the key, the version AND the value, so there is
// nothing to look up and nothing that can have been evicted from under us. A key the
// cache already knows is skipped -- in particular a key in write state, whose
// in-flight value must not be shadowed by a committed read.
//
// A prefilled record is current. Had the key's committed version advanced past this
// read, the advance came from one of our own committed txs, whose write marked the
// key `write` in the auth cache -- and write entries are never evicted. So the key
// would either still be `write` (Has is true, prefill skips it) or already demoted,
// which the watermark permits only once every batch below the stamp has been
// authed, i.e. only for batches that read the key after the refresh.
func (a *authPhase) prefill(tx *StreamTx) {
	for i := range tx.Reads {
		rd := &tx.Reads[i]
		if rd.Absent || a.d.Cache.Has(rd.Key) {
			continue
		}
		a.d.Cache.PutRead(rd.Key, &blocks.WriteRecord{
			Namespace: a.d.Namespace,
			Key:       rd.Key,
			Version:   rd.Version,
			Value:     rd.Value,
		})
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestAuthStore|TestAuthWorker|TestAuthReexecution' -race -v`
Expected: PASS (14 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_worker.go gateway/core/stream_auth_cache.go gateway/core/stream_auth_worker_test.go
git commit -m "feat(stream): accept-as-is fast path, re-execution and warm-cache prefill"
```

---

### Task 20: Notification handling, demotion under the watermark, end-of-batch eviction

**Files:**
- Modify: `gateway/core/stream_auth_worker.go` (add `settleBatch`; call it from `handleBatch`; finalize `handleNotification`)
- Test: `gateway/core/stream_auth_worker_test.go` (append)

**Interfaces:**
- Consumes: `watermark.Hold` / `Advance` (Task 13), `AuthCache.Demote` / `Evict` (Tasks 10, 12).
- Produces: `func (a *authPhase) settleBatch(batch uint64)`; `Prev() int64` on `authPhase` for observability.

Order within `settleBatch` is fixed and matters: **advance the watermark and demote first, then evict.** Demotion returns entries to the read half's accounting, so evicting first would leave the budget overstated for a whole batch; and eviction must never run mid-batch, both because the design says so for performance and because a key vanishing between two txs of one batch changes what the second one reads.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_auth_worker_test.go (append)

// A notification is held until every batch below its stamp has been authed. Until
// then the key stays in write state, so a batch carrying a pre-commit read of it
// is re-executed rather than accepted.
func TestAuthWorkerHoldsDemotionUntilTheWatermarkPasses(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{result: okTx("x", 1, "v")})

	// "k" is written by an in-flight tx: its committed version will be 2.
	written := streamTxOf(map[string]uint64{"k": 1}, map[string]string{"k": "inflight"})
	h.cache.ApplyWrites(written)
	h.cache.NextAggregate()
	if written.Writes[0].Version != 2 {
		t.Fatalf("stamped version = %d, want 2", written.Writes[0].Version)
	}

	// Its notification is stamped 2: batches 0 and 1 must be authed first.
	h.notifyIn <- CommitNotification{Batch: 2, Committed: written.Writes}
	// Batch 0 authed -> still not safe.
	h.warmIn <- warmBatchOf(0, okTx("x", 1, "v"))
	select {
	case <-h.out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	if !h.cache.IsWrite("k") {
		t.Fatal("after only batch 0, the demotion must still be held (stamp 2 needs batches 0 AND 1)")
	}
	// Batch 1 authed -> the watermark reaches 1 = stamp-1, so the demotion lands.
	h.warmIn <- warmBatchOf(1, okTx("x", 1, "v"))
	select {
	case <-h.out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.cache.IsWrite("k") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.cache.IsWrite("k") {
		t.Fatal("once the watermark reaches stamp-1, the key must be demoted to read")
	}
}

// A notification whose stamp is already covered is applied immediately.
func TestAuthWorkerAppliesAnAlreadySafeNotificationImmediately(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{})

	written := streamTxOf(map[string]uint64{"k": 1}, map[string]string{"k": "inflight"})
	h.cache.ApplyWrites(written)
	h.cache.NextAggregate()
	h.notifyIn <- CommitNotification{Batch: 0, Committed: written.Writes} // stamp 0 -> nothing below it

	deadline := time.Now().Add(5 * time.Second)
	for h.cache.IsWrite("k") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.cache.IsWrite("k") {
		t.Fatal("a notification stamped 0 has no batch below it and must demote immediately")
	}
}

// Eviction runs at the END of a batch, never between its txs: two txs of one batch
// reading the same committed key must read the query service once, even when the
// read budget is exhausted.
func TestAuthWorkerEvictsOnlyAtTheBatchBoundary(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 10
	cfg.AuthBatchTimeout = time.Hour
	cfg.AuthReadCacheBytes = 1 // every evictable entry goes on the first sweep
	exec := &fakeAuthExec{result: okTx("shadowed", 1, "v")}
	h := startAuthPhase(t, cfg, exec)
	h.under.mu.Lock()
	h.under.recs["shared"] = &blocks.WriteRecord{Key: "shared", Version: 4, Value: []byte("v")}
	h.under.mu.Unlock()
	// Both txs are forced to re-execute, and each re-execution reads "shared".
	exec.storeGets = func(store api.StateReader) { _, _ = store.Get("ns", "shared") }
	h.cache.ApplyWrites(streamTxOf(map[string]uint64{"shadowed": 1}, map[string]string{"shadowed": "inflight"}))

	stale := okTx("shadowed", 1, "stale")
	h.warmIn <- warmBatchOf(0, stale, stale)
	deadline := time.Now().Add(5 * time.Second)
	for h.under.getCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Let the batch finish.
	time.Sleep(100 * time.Millisecond)
	if got := h.under.getCount(); got != 1 {
		t.Errorf("query service reads = %d, want 1 -- eviction must not run between two txs of one batch", got)
	}
}

func TestAuthWorkerReportsTheWatermark(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = time.Hour
	h := startAuthPhase(t, cfg, &fakeAuthExec{})
	if h.phase.Prev() != -1 {
		t.Fatalf("Prev = %d before any batch, want -1", h.phase.Prev())
	}
	h.warmIn <- warmBatchOf(0, okTx("k", 1, "v"))
	select {
	case <-h.out:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.phase.Prev() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.phase.Prev() != 0 {
		t.Fatalf("Prev = %d after batch 0, want 0", h.phase.Prev())
	}
}
```

`Prev()` is read from the test goroutine while the worker runs, which is a data race on paper. Read it only after the worker has quiesced — the tests above do that by waiting on `h.out` first and then polling — and keep `Prev()` documented as test-only observability. If `-race` still flags it, guard `authPhase.prev` behind an `atomic.Int64` mirror updated in `settleBatch`; do not add a mutex to the worker's hot path.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestAuthWorkerHolds|TestAuthWorkerApplies|TestAuthWorkerEvicts|TestAuthWorkerReportsTheWatermark' -v`
Expected: FAIL — `h.phase.Prev undefined`, and the demotion never lands because nothing advances the watermark.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_auth_worker.go

// Add this field to the authPhase struct. It mirrors the watermark for
// observability; only the worker goroutine writes it, in settleBatch.
//
//	prevMirror atomic.Int64

// Prev reports the watermark: every warm batch up to and including this number has
// been authed. -1 means none has. Observability only.
func (a *authPhase) Prev() int64 { return a.prevMirror.Load() }
```

Change `newAuthPhase` to bind the value before returning it, so `prevMirror` can start at `-1` ("no batch authed yet") rather than at the zero value, which would mean "batch 0 is authed":

```go
func newAuthPhase(d authDeps) *authPhase {
	d.Cfg.Sanitize()
	a := &authPhase{
		d:     d,
		store: newAuthStore(d.Cache, d.Under),
		mark:  newWatermark(),
		pend:  make([]*StreamTx, 0, d.Cfg.AuthBatchSize),
	}
	a.prevMirror.Store(-1)
	return a
}
```

```go
// settleBatch closes out one authed batch, in this order:
//
//  1. advance the watermark with this batch's number and apply every commit
//     notification that just became safe -- demoting each committed key from write
//     back to read;
//  2. evict.
//
// The order is not incidental. Demotion returns entries to the read half's
// accounting, so evicting first would work from an understated read size for a
// whole batch. And eviction happens ONLY here, at the end of a batch: the design
// specifies it for performance, and it is also what keeps a batch's txs reading a
// stable cache -- a key vanishing between two txs would change what the second one
// reads.
func (a *authPhase) settleBatch(batch uint64) {
	if committed := a.mark.Advance(batch); len(committed) > 0 {
		a.d.Cache.Demote(committed)
	}
	a.prevMirror.Store(a.mark.Prev())
	a.d.Cache.Evict()
}

// handleNotification files a stamped commit notification with the watermark and
// applies it immediately if every batch below the stamp has already been authed.
//
// The notification arrives here having already been used to refresh the warmup
// cache and stamped by the warmup notification worker; the auth phase uses only
// its keys and versions (see runWarmNotifier and StreamKV).
func (a *authPhase) handleNotification(n CommitNotification) {
	if committed := a.mark.Hold(n.Batch, n.Committed); len(committed) > 0 {
		a.d.Cache.Demote(committed)
	}
}
```

Add the `settleBatch` call as the last statement of `handleBatch`, immediately before `return true`:

```go
	a.settleBatch(wb.Batch)
	return true
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -race -v -run 'TestAuthStore|TestAuthWorker|TestAuthReexecution'`
Expected: PASS (18 tests, no race reports).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_auth_worker.go gateway/core/stream_auth_worker_test.go
git commit -m "feat(stream): watermark-gated demotion and end-of-batch eviction"
```

---
## Phase 7 — Submit and commit notification

### Task 21: The submit worker — endorse, register, submit, wait for ordering

**Files:**
- Create: `gateway/core/stream_submit_worker.go`
- Test: `gateway/core/stream_submit_worker_test.go`

**Interfaces:**
- Consumes: `submitBatch` (Task 1), `EndorsementClient.EndorseMerged` (Task 5), `committerTxID` (existing, `gateway/core/executor.go:446`), `OrderGate` (existing, `gateway/core/order_gate.go`), `resetTimer` (Task 15).
- Produces:
  - `type submitEndorser interface { EndorseMerged(ctx context.Context, txs []*types.Transaction, merged endorsement.ExecutionResult) (sdk.Endorsement, error) }`
  - `type submitDeps struct { In <-chan submitBatch; Endorse submitEndorser; Submit func(context.Context, sdk.Endorsement) error; Gate *OrderGate; Register func(txID string, sb submitBatch, committed []StreamKV); Fatal func(error); OrderTimeout time.Duration }`
  - `func newSubmitPhase(d submitDeps) *submitPhase`, `Start(ctx)`, `Wait()`, `Submitted() uint64`
  - `func aggregate(txs []*StreamTx) (endorsement.ExecutionResult, []StreamKV, error)`

**The submitter owns aggregation.** It is the only stage that knows which txs share a committer tx, so it is the only one that can fold their read-write sets correctly:

- **first version per read key** — the aggregate is validated against the state it was built on, and the first tx to read a key read it before any of this aggregate's writes stepped it.
- **last write per key** — last writer wins for the value, and every write to a key inside one aggregate carries the same stamped version (`AuthCache.ApplyWrites` steps a key once per aggregate), so the last write's version *is* the aggregate's version. That is what the commit notification carries back.
- **per-tx outcomes** in aggregation order, marshalled into the merged result's `Event`, exactly as `endorseBatch` does for the serial path — the committed block's receipts are recovered from there.

**Two orderings are load-bearing here, and both are register-before-act:**

1. **Register in the submit map before broadcasting.** A commit notification can otherwise arrive before the map holds the tx, and its committed keys — the payload the warmup refresh and the auth demotion both need — would be lost with no way to recover them.
2. **Wait for the ORDERER's block delivery, not the committer's, before submitting the next batch.** Every version the auth cache predicts assumes commits happen in submission order. Ordered delivery is what establishes that, and it is cheaper to wait for than a commit.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_submit_worker_test.go
package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	commonpb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// endorsementWithTxID builds the minimum sdk.Endorsement from which
// committerTxID can recover txID.
func endorsementWithTxID(t *testing.T, txID string) sdk.Endorsement {
	t.Helper()
	hdr := &commonpb.Header{
		ChannelHeader: protoutil.MarshalOrPanic(&commonpb.ChannelHeader{TxId: txID}),
	}
	return sdk.Endorsement{Proposal: &peer.Proposal{Header: protoutil.MarshalOrPanic(hdr)}}
}

type fakeSubmitEndorser struct {
	mu         sync.Mutex
	calls      int
	lastMerged endorsement.ExecutionResult
	err        error
	build      func(i int) sdk.Endorsement
}

func (f *fakeSubmitEndorser) EndorseMerged(_ context.Context, _ []*types.Transaction, merged endorsement.ExecutionResult) (sdk.Endorsement, error) {
	f.mu.Lock()
	i := f.calls
	f.calls++
	f.lastMerged = merged
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return sdk.Endorsement{}, err
	}
	return f.build(i), nil
}

type submitHarness struct {
	in         chan submitBatch
	registered chan string
	committed  chan []StreamKV
	submitted  chan string
	fatals     chan error
	gate       *OrderGate
	endorser   *fakeSubmitEndorser
	phase      *submitPhase
}

func startSubmitPhase(t *testing.T, orderTimeout time.Duration, endorseErr, submitErr error) *submitHarness {
	t.Helper()
	h := &submitHarness{
		in:         make(chan submitBatch, 8),
		registered: make(chan string, 8),
		committed:  make(chan []StreamKV, 8),
		submitted:  make(chan string, 8),
		fatals:     make(chan error, 4),
		gate:       NewOrderGate(),
	}
	h.endorser = &fakeSubmitEndorser{
		err:   endorseErr,
		build: func(i int) sdk.Endorsement { return endorsementWithTxID(t, "tx-"+string(rune('a'+i))) },
	}
	var lastID string
	var mu sync.Mutex
	h.phase = newSubmitPhase(submitDeps{
		In:      h.in,
		Endorse: h.endorser,
		Submit: func(_ context.Context, e sdk.Endorsement) error {
			if submitErr != nil {
				return submitErr
			}
			mu.Lock()
			id := lastID
			mu.Unlock()
			h.submitted <- id
			return nil
		},
		Gate: h.gate,
		Register: func(txID string, _ submitBatch, committed []StreamKV) {
			mu.Lock()
			lastID = txID
			mu.Unlock()
			h.committed <- committed
			h.registered <- txID
		},
		Fatal:        func(err error) { select { case h.fatals <- err: default: } },
		OrderTimeout: orderTimeout,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.phase.Start(ctx)
	t.Cleanup(func() { cancel(); h.phase.Wait() })
	return h
}

// aSubmitBatch is one settled tx: read k@1, write k stamped at 2 (what
// AuthCache.ApplyWrites would have stamped).
func aSubmitBatch() submitBatch {
	return submitBatch{txs: []*StreamTx{{
		Tx:     streamTx(0),
		Status: 200,
		Reads:  []StreamKV{{Key: "k", Version: 1, Value: []byte("v1")}},
		Writes: []StreamKV{{Key: "k", Version: 2, Value: []byte("v2")}},
	}}}
}

// Aggregation is the submitter's job, and these are the two rules: FIRST version
// per read (the aggregate is validated against the state it was built on) and LAST
// write per key (last writer wins, and every write to a key inside one aggregate
// carries the same stamped version).
func TestAggregateKeepsFirstReadAndLastWrite(t *testing.T) {
	first := &StreamTx{
		Tx:     streamTx(0),
		Status: 200,
		Reads:  []StreamKV{{Key: "k", Version: 5, Value: []byte("v5")}, {Key: "fresh", Absent: true}},
		Writes: []StreamKV{{Key: "k", Version: 6, Value: []byte("a")}},
	}
	second := &StreamTx{
		Tx:     streamTx(1),
		Status: 200,
		// It read k through the auth cache, so it saw the stepped version 6.
		Reads:  []StreamKV{{Key: "k", Version: 6, Value: []byte("a")}},
		Writes: []StreamKV{{Key: "k", Version: 6, Value: []byte("b")}},
	}

	merged, committed, err := aggregate([]*StreamTx{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.RWS.Reads) != 2 {
		t.Fatalf("merged reads = %+v, want one per distinct key", merged.RWS.Reads)
	}
	for _, rd := range merged.RWS.Reads {
		switch rd.Key {
		case "k":
			if rd.Version == nil || rd.Version.BlockNum != 5 {
				t.Errorf("read version for k = %+v, want the FIRST one seen (5), not the second (6)", rd.Version)
			}
		case "fresh":
			if rd.Version != nil {
				t.Errorf("an absent read must record a nil version, got %+v", rd.Version)
			}
		}
	}
	if len(merged.RWS.Writes) != 1 {
		t.Fatalf("merged writes = %+v, want one per distinct key", merged.RWS.Writes)
	}
	if string(merged.RWS.Writes[0].Value) != "b" {
		t.Errorf("merged write value = %q, want the LAST write %q", merged.RWS.Writes[0].Value, "b")
	}
	if len(committed) != 1 || committed[0].Version != 6 || string(committed[0].Value) != "b" {
		t.Fatalf("committed payload = %+v, want k at version 6 with the last value", committed)
	}
	var outcomes []execution.PerTxOutcome
	if err := json.Unmarshal(merged.Event, &outcomes); err != nil {
		t.Fatalf("Event must carry the per-tx outcomes: %v", err)
	}
	if len(outcomes) != 2 {
		t.Errorf("outcomes = %d, want one per tx", len(outcomes))
	}
}

// An aggregate with nothing to write would become a committer tx the committer
// rejects as MALFORMED_NO_WRITES.
func TestAggregateRejectsAWriteLessBatch(t *testing.T) {
	if _, _, err := aggregate([]*StreamTx{{Tx: streamTx(0), Status: common.StatusTxRejected}}); err == nil {
		t.Fatal("an aggregate with no writes must be an error")
	}
}

// The committed payload the submitter registers is what the commit notification
// carries back, so it must be derived at aggregation and not somewhere else.
func TestSubmitWorkerRegistersTheAggregatedCommittedPayload(t *testing.T) {
	h := startSubmitPhase(t, 5*time.Second, nil, nil)
	h.in <- aSubmitBatch()
	select {
	case committed := <-h.committed:
		if len(committed) != 1 || committed[0].Key != "k" || committed[0].Version != 2 {
			t.Fatalf("registered payload = %+v, want k at the stamped version 2", committed)
		}
		if string(committed[0].Value) == "" {
			t.Error("the committed VALUE must be registered: the warmup refresh needs it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was registered")
	}
	h.gate.Observe([]string{<-h.registered})
}

// The map registration must precede the broadcast, or a fast commit notification
// arrives before the payload it needs is stored.
func TestSubmitWorkerRegistersBeforeSubmitting(t *testing.T) {
	h := startSubmitPhase(t, 5*time.Second, nil, nil)
	h.in <- aSubmitBatch()

	var regID string
	select {
	case regID = <-h.registered:
	case <-time.After(5 * time.Second):
		t.Fatal("the batch was never registered")
	}
	select {
	case subID := <-h.submitted:
		if subID != regID {
			t.Fatalf("submitted %q but registered %q -- registration must happen first, with the same txID", subID, regID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the batch was never submitted")
	}
	h.gate.Observe([]string{regID})
}

// One batch at a time: the next is not endorsed until the previous has been seen
// in an ordered block.
func TestSubmitWorkerWaitsForOrderedDeliveryBeforeTheNextBatch(t *testing.T) {
	h := startSubmitPhase(t, 5*time.Second, nil, nil)
	h.in <- aSubmitBatch()
	h.in <- aSubmitBatch()

	first := <-h.registered
	<-h.submitted
	select {
	case second := <-h.registered:
		t.Fatalf("registered %q while %q had not been ordered yet -- submission must be depth-1", second, first)
	case <-time.After(200 * time.Millisecond):
	}

	h.gate.Observe([]string{first})
	select {
	case <-h.registered:
	case <-time.After(5 * time.Second):
		t.Fatal("the second batch must proceed once the first is observed in an ordered block")
	}
}

// Ordered delivery never arriving breaks the ordering invariant every version the
// auth cache predicts depends on, so it is fatal rather than skipped.
func TestSubmitWorkerOrderTimeoutIsFatal(t *testing.T) {
	h := startSubmitPhase(t, 50*time.Millisecond, nil, nil)
	h.in <- aSubmitBatch()
	select {
	case err := <-h.fatals:
		if err == nil {
			t.Fatal("expected a non-nil fatal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an unobserved ordered delivery must be fatal: proceeding would silently break commit ordering")
	}
}

func TestSubmitWorkerEndorseAndSubmitErrorsAreFatal(t *testing.T) {
	for name, h := range map[string]*submitHarness{
		"endorse": startSubmitPhase(t, 5*time.Second, errors.New("endorse failed"), nil),
		"submit":  startSubmitPhase(t, 5*time.Second, nil, errors.New("broadcast failed")),
	} {
		h.in <- aSubmitBatch()
		select {
		case err := <-h.fatals:
			if err == nil {
				t.Fatalf("%s: expected a non-nil fatal error", name)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the failure must be reported as fatal", name)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestSubmitWorker -v`
Expected: FAIL — `undefined: newSubmitPhase`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_submit_worker.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// submitEndorser is the slice of the endorsement client the submit phase needs:
// sign one merged committer tx over results the auth phase already settled.
type submitEndorser interface {
	EndorseMerged(ctx context.Context, txs []*types.Transaction, merged endorsement.ExecutionResult) (sdk.Endorsement, error)
}

// submitDeps is submitPhase's wiring.
type submitDeps struct {
	In      <-chan submitBatch
	Endorse submitEndorser
	Submit  func(ctx context.Context, e sdk.Endorsement) error
	Gate    *OrderGate

	// Register stores the batch and its committed payload under the committer TxID
	// BEFORE it is broadcast, so a commit notification can never arrive before the
	// payload it needs.
	Register func(txID string, sb submitBatch, committed []StreamKV)

	Fatal        func(error)
	OrderTimeout time.Duration
}

// submitPhase is the submit worker: ONE serialized worker that turns each settled
// auth batch into one committer tx and pushes it, waiting for the orderer to
// deliver it before moving on.
//
// One at a time, and waiting for ORDERED delivery rather than commit, is not a
// throughput compromise -- it is the mechanism that makes commit order equal
// submission order. Every version the auth cache predicts (a key advancing by one
// per committer tx) assumes exactly that; without it the predictions are wrong and
// every dependent read aborts.
//
// In this milestone the submit batch is the auth batch, 1:1. Adaptive batch sizing
// (growing and shrinking with queue depth) is deferred: it would change what is
// submitted while the first overlap measurement is being taken.
type submitPhase struct {
	d         submitDeps
	submitted atomic.Uint64
	wg        sync.WaitGroup
}

func newSubmitPhase(d submitDeps) *submitPhase {
	if d.OrderTimeout <= 0 {
		d.OrderTimeout = DefaultStreamCommitTimeout
	}
	return &submitPhase{d: d}
}

// Submitted counts committer txs broadcast and observed in an ordered block.
func (s *submitPhase) Submitted() uint64 { return s.submitted.Load() }

// Start launches the worker.
func (s *submitPhase) Start(ctx context.Context) {
	s.wg.Go(func() { s.run(ctx) })
}

// Wait blocks until the worker has exited.
func (s *submitPhase) Wait() { s.wg.Wait() }

func (s *submitPhase) run(ctx context.Context) {
	// One reusable timer for the ordered-delivery wait: a fresh time.After per
	// batch would allocate a timer on the pipeline's hottest boundary.
	timer := time.NewTimer(s.d.OrderTimeout)
	defer timer.Stop()
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case sb := <-s.d.In:
			if !s.submitOne(ctx, sb, timer) {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// submitOne turns one settled batch into a committer tx: sign, register, arm,
// broadcast, and wait for the orderer to deliver it. Returns false when the
// pipeline must stop.
func (s *submitPhase) submitOne(ctx context.Context, sb submitBatch, timer *time.Timer) bool {
	merged, committed, err := aggregate(sb.txs)
	if err != nil {
		s.d.Fatal(fmt.Errorf("aggregate submit batch (%d txs): %w", len(sb.txs), err))
		return false
	}
	ethTxs := make([]*types.Transaction, len(sb.txs))
	for i, tx := range sb.txs {
		ethTxs[i] = tx.Tx
	}
	end, err := s.d.Endorse.EndorseMerged(ctx, ethTxs, merged)
	if err != nil {
		s.d.Fatal(fmt.Errorf("endorse submit batch (%d txs): %w", len(sb.txs), err))
		return false
	}
	txID, err := committerTxID(end.Proposal)
	if err != nil {
		s.d.Fatal(fmt.Errorf("extract committer tx id (%d txs): %w", len(sb.txs), err))
		return false
	}

	// Register-then-submit, both for the submit map and the order gate. A commit
	// notification or an ordered block can be delivered before Submit even returns,
	// so anything that must observe this tx has to be in place first.
	s.d.Register(txID, sb, committed)
	wait := s.d.Gate.Arm(txID)

	if err := s.d.Submit(ctx, end); err != nil {
		s.d.Gate.Disarm()
		s.d.Fatal(fmt.Errorf("submit %s (%d txs): %w", txID, len(sb.txs), err))
		return false
	}

	resetTimer(timer, s.d.OrderTimeout)
	select {
	case <-wait:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		s.submitted.Add(1)
		return true
	case <-timer.C:
		// Proceeding without having seen the tx ordered would silently break the
		// commit-order invariant the auth cache's version predictions rest on, so
		// this is fatal rather than a skipped wait.
		s.d.Gate.Disarm()
		s.d.Fatal(fmt.Errorf("submit %s: ordered delivery not observed within %s", txID, s.d.OrderTimeout))
		return false
	case <-ctx.Done():
		s.d.Gate.Disarm()
		return false
	}
}

// aggregate folds a committer tx's worth of internal transactions into the merged
// execution result to sign, plus the per-key committed payload its commit
// notification will carry back.
//
// It lives here because the submitter is the only stage that knows which txs share
// a committer tx, and the folding rules follow directly from that:
//
//   - READS take the FIRST version seen per key. The aggregate is validated against
//     the state it was built on, and the first tx to read a key read it before any
//     of this aggregate's writes stepped it. A key first seen as absent is recorded
//     with a nil MVCC version, which is what "absent" means on the wire.
//   - WRITES take the LAST write per key -- last writer wins for the value. Its
//     version is the aggregate's version for that key: AuthCache.ApplyWrites steps a
//     key once per aggregate, so every write to it inside this aggregate carries the
//     same stamped version.
//   - OUTCOMES are per tx, in aggregation order, marshalled into the merged result's
//     Event. The committed block's per-tx receipts are recovered from there, which is
//     why they ride in Event and not in the response payload.
//
// A tx with no writes contributes its outcome and its reads but nothing to commit;
// an aggregate with no writes at all is an error, because the committer would reject
// it as MALFORMED_NO_WRITES.
func aggregate(txs []*StreamTx) (endorsement.ExecutionResult, []StreamKV, error) {
	var rws blocks.ReadWriteSet
	readIdx := make(map[string]int)
	writeIdx := make(map[string]int)
	committed := make([]StreamKV, 0, len(txs))
	outcomes := make([]execution.PerTxOutcome, len(txs))

	for i, tx := range txs {
		outcomes[i] = execution.PerTxOutcome{Status: tx.Status, Event: tx.Event}

		for _, rd := range tx.Reads {
			if _, seen := readIdx[rd.Key]; seen {
				continue // first version per key wins
			}
			read := blocks.KVRead{Key: rd.Key}
			if !rd.Absent {
				read.Version = &blocks.Version{BlockNum: rd.Version}
			}
			readIdx[rd.Key] = len(rws.Reads)
			rws.Reads = append(rws.Reads, read)
		}

		for _, w := range tx.Writes {
			write := blocks.KVWrite{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete}
			if at, seen := writeIdx[w.Key]; seen {
				rws.Writes[at] = write     // last write per key wins
				committed[readIdxOf(committed, w.Key)] = w
				continue
			}
			writeIdx[w.Key] = len(rws.Writes)
			rws.Writes = append(rws.Writes, write)
			committed = append(committed, w)
		}
	}

	if len(rws.Writes) == 0 {
		return endorsement.ExecutionResult{}, nil, fmt.Errorf("aggregate of %d txs has no writes", len(txs))
	}

	event, err := json.Marshal(outcomes)
	if err != nil {
		return endorsement.ExecutionResult{}, nil, fmt.Errorf("marshal per-tx outcomes: %w", err)
	}
	return endorsement.ExecutionResult{RWS: rws, Status: 200, Message: "OK", Event: event}, committed, nil
}

// readIdxOf finds key's slot in committed. The slice holds one entry per written
// key and a committer tx writes a handful of them, so the scan is cheaper than a
// second index.
func readIdxOf(committed []StreamKV, key string) int {
	for i := range committed {
		if committed[i].Key == key {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestSubmitWorker -race -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_submit_worker.go gateway/core/stream_submit_worker_test.go
git commit -m "feat(stream): ordered single-submitter worker with register-then-submit"
```

---
### Task 22: The submit map and the commit notification worker

**Files:**
- Create: `gateway/core/stream_commit_notifier.go`
- Test: `gateway/core/stream_commit_notifier_test.go`

**Interfaces:**
- Consumes: `submitBatch` / `CommitNotification` / `StreamKV` / `StreamTx` (Task 1), `RecordCommitLatency` (existing, `metrics_hooks.go:53`).
- Produces:
  - `type TxOutcome int` with `TxPending`, `TxCommitted`, `TxAborted`
  - `type TxStatusQuerier interface { TxStatus(ctx context.Context, txIDs []string) (map[string]TxOutcome, error) }`
  - `type commitDeps struct { Out chan<- CommitNotification; Status TxStatusQuerier; Timeout time.Duration; Fatal func(error); Now func() time.Time }`
  - `func newCommitPhase(d commitDeps) *commitPhase`, and on it: `Register(txID string, sb submitBatch, committed []StreamKV)`, `Handle(txID string, committed bool)`, `Pending() int`, `Start(ctx)`, `Wait()`

The notification it emits carries **committed keys with their version and their value**. The value is not optional: the warmup refresh needs it, because attaching a fresh version to a stale value passes MVCC validation and commits a wrong result. The auth demotion needs only the version. The `Batch` field is left zero here — the warmup notification worker stamps it, after refreshing.

The design specifies a `sync.Map` for the submit map. This uses a mutex-guarded map instead, deliberately: the map is touched three times per *committer tx* (register, resolve, sweep), not per read, so contention is a non-issue, and the timeout sweeper needs to range over it and collect a batch of overdue IDs — which a plain map does directly.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_commit_notifier_test.go
package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeStatusQuerier struct {
	queried  [][]string
	statuses map[string]TxOutcome
	err      error
}

func (f *fakeStatusQuerier) TxStatus(_ context.Context, txIDs []string) (map[string]TxOutcome, error) {
	ids := make([]string, len(txIDs))
	copy(ids, txIDs)
	f.queried = append(f.queried, ids)
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]TxOutcome, len(txIDs))
	for _, id := range txIDs {
		out[id] = f.statuses[id] // zero value is TxPending
	}
	return out, nil
}

// aCommittedPayload is the payload aSubmitBatch's aggregate would produce.
func aCommittedPayload() []StreamKV {
	return []StreamKV{{Key: "k", Version: 2, Value: []byte("v2")}}
}

type commitHarness struct {
	out    chan CommitNotification
	fatals chan error
	status *fakeStatusQuerier
	now    time.Time
	phase  *commitPhase
}

func newCommitHarness(t *testing.T, timeout time.Duration) *commitHarness {
	t.Helper()
	h := &commitHarness{
		out:    make(chan CommitNotification, 8),
		fatals: make(chan error, 4),
		status: &fakeStatusQuerier{statuses: map[string]TxOutcome{}},
		now:    time.Unix(1_700_000_000, 0),
	}
	h.phase = newCommitPhase(commitDeps{
		Out:     h.out,
		Status:  h.status,
		Timeout: timeout,
		Fatal:   func(err error) { select { case h.fatals <- err: default: } },
		Now:     func() time.Time { return h.now },
	})
	return h
}

func TestCommitHandleEmitsTheCommittedPayloadAndClearsTheEntry(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	if h.phase.Pending() != 1 {
		t.Fatalf("Pending = %d after Register, want 1", h.phase.Pending())
	}

	h.phase.Handle("tx-1", true)
	select {
	case n := <-h.out:
		if len(n.Committed) != 1 || n.Committed[0].Key != "k" || n.Committed[0].Version != 2 {
			t.Fatalf("notification = %+v, want the batch's committed keys with versions", n.Committed)
		}
		if string(n.Committed[0].Value) == "" {
			t.Error("the committed VALUE must travel with the notification: the warmup refresh cannot attach a fresh version to a stale value")
		}
		if n.Batch != 0 {
			t.Errorf("Batch = %d, want 0 -- the warmup notification worker stamps it, after refreshing", n.Batch)
		}
	case <-time.After(time.Second):
		t.Fatal("a committed tx must produce a notification")
	}
	if h.phase.Pending() != 0 {
		t.Errorf("Pending = %d after resolution, want 0", h.phase.Pending())
	}
}

func TestCommitHandleIsExactlyOnceAndIgnoresUnknownTxIDs(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	h.phase.Handle("tx-1", true)
	h.phase.Handle("tx-1", true)
	h.phase.Handle("never-registered", true)

	<-h.out
	select {
	case n := <-h.out:
		t.Fatalf("a second resolution must be a no-op; got %+v", n)
	case <-time.After(100 * time.Millisecond):
	}
}

// Rollback is deferred to a phase-2 plan, so an abort stops the pipeline loudly
// rather than continuing with an auth cache full of writes that will never commit.
func TestCommitAbortIsFatalInThisMilestone(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	h.phase.Handle("tx-1", false)

	select {
	case err := <-h.fatals:
		if err == nil {
			t.Fatal("expected a non-nil fatal error")
		}
	case <-time.After(time.Second):
		t.Fatal("an abort must be fatal while rollback is unimplemented")
	}
	select {
	case n := <-h.out:
		t.Fatalf("an aborted tx must not refresh the warmup cache; got %+v", n)
	case <-time.After(100 * time.Millisecond):
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run TestCommit -v`
Expected: FAIL — `undefined: newCommitPhase`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_commit_notifier.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

// TxOutcome is a committer tx's adjudicated fate, as the pipeline cares about it.
// It collapses the query service's status enum to the three cases that lead to
// different actions.
type TxOutcome int

const (
	// TxPending: not validated yet, i.e. still in flight. Keep waiting.
	TxPending TxOutcome = iota
	// TxCommitted: proceed exactly as if the notification had arrived.
	TxCommitted
	// TxAborted: any aborted/malformed/rejected status. Roll back (fatal in this
	// milestone).
	TxAborted
)

// TxStatusQuerier adjudicates timed-out committer txs. One call takes every txID
// that timed out together, which is what makes it affordable to ask rather than
// assume. The adapter from the query service's status enum lives at the wiring
// layer, so this package stays free of the committer protos.
type TxStatusQuerier interface {
	TxStatus(ctx context.Context, txIDs []string) (map[string]TxOutcome, error)
}

// pendingCommit is one submitted committer tx awaiting its outcome.
type pendingCommit struct {
	committed []StreamKV
	txs       []*StreamTx
	// at is when the tx was submitted, for the commit-latency sample.
	at time.Time
	// due is when this tx becomes overdue. It is pushed forward when adjudication
	// says the tx is merely still in flight (re-arming the timeout).
	due time.Time
}

// commitDeps is commitPhase's wiring.
type commitDeps struct {
	// Out is the WARMUP phase's notification queue -- deliberately not the auth
	// phase's. The notification has to refresh the warmup cache and be stamped with
	// a batch number before the auth phase can safely act on it.
	Out chan<- CommitNotification

	Status  TxStatusQuerier
	Timeout time.Duration
	Fatal   func(error)
	// Now is injectable so the timeout logic is testable without sleeping.
	Now func() time.Time
}

// commitPhase owns the submit map and the commit notification worker. Every
// submitted committer tx is registered here before it is broadcast and leaves only
// when its outcome is known -- from the notification stream, or from a
// query-service adjudication after a timeout.
//
// The map is guarded by a mutex rather than being a sync.Map. It is touched three
// times per COMMITTER TX (register, resolve, sweep), never per read, so contention
// is irrelevant; and the timeout sweeper needs to range over it to collect every
// overdue txID for one batched status query, which a plain map does directly.
type commitPhase struct {
	d commitDeps

	mu      sync.Mutex
	pending map[string]*pendingCommit

	wg   sync.WaitGroup
	done chan struct{}
}

func newCommitPhase(d commitDeps) *commitPhase {
	if d.Timeout <= 0 {
		d.Timeout = DefaultStreamCommitTimeout
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	return &commitPhase{
		d:       d,
		pending: make(map[string]*pendingCommit),
		done:    make(chan struct{}),
	}
}

// Pending reports how many submitted committer txs are unresolved.
func (c *commitPhase) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Register stores a submitted batch and the committed payload the submitter derived
// for it under the committer TxID. It MUST be called before the tx is broadcast: a
// commit notification can be delivered before Submit returns, and the committed
// keys/versions/values exist nowhere else.
//
// It holds the internal transactions, not just their hashes, because a rollback
// (phase 2) has to put their EVM txs back on the input queue.
func (c *commitPhase) Register(txID string, sb submitBatch, committed []StreamKV) {
	now := c.d.Now()
	c.mu.Lock()
	c.pending[txID] = &pendingCommit{
		committed: committed,
		txs:       sb.txs,
		at:        now,
		due:       now.Add(c.d.Timeout),
	}
	c.mu.Unlock()
}

// Handle resolves a submitted committer tx, exactly once. A txID that is not (or
// no longer) pending is ignored: the notification stream and the timeout
// adjudication can both reach the same tx, and whichever arrives first wins.
//
// On commit it emits the notification -- committed keys with their version AND
// their value -- to the WARMUP phase and drops the entry. The value is required:
// the warmup refresh attaching a fresh version to a stale value would pass MVCC
// validation and commit a wrong result.
//
// On abort it is fatal. Rollback (optimization.md section "Rollback") is deferred
// to a phase-2 plan, and continuing without it would leave the auth cache pinned
// on writes that will never commit, so every tx reading those keys would
// re-execute forever.
func (c *commitPhase) Handle(txID string, committed bool) {
	c.mu.Lock()
	e, ok := c.pending[txID]
	if ok {
		delete(c.pending, txID)
	}
	c.mu.Unlock()
	if !ok {
		return
	}

	if !committed {
		c.d.Fatal(fmt.Errorf("committer tx %s aborted (%d evm txs): rollback is deferred to the phase-2 plan, so the pipeline cannot continue", txID, len(e.txs)))
		return
	}

	if RecordCommitLatency != nil {
		RecordCommitLatency(c.d.Now().Sub(e.at))
	}
	select {
	case c.d.Out <- CommitNotification{Committed: e.committed}:
	case <-c.done:
	}
}

// Stop releases anything blocked on Out. Call after the workers have been
// cancelled.
func (c *commitPhase) Stop() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run TestCommit -race -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_commit_notifier.go gateway/core/stream_commit_notifier_test.go
git commit -m "feat(stream): submit map and commit notification worker"
```

---

### Task 23: Notification-timeout adjudication

**Files:**
- Modify: `gateway/core/stream_commit_notifier.go` (append)
- Test: `gateway/core/stream_commit_notifier_test.go` (append)

**Interfaces:**
- Consumes: `commitPhase` (Task 22), `TxStatusQuerier`.
- Produces: `func (c *commitPhase) Adjudicate(ctx context.Context)`, `func (c *commitPhase) Start(ctx context.Context)`, `func (c *commitPhase) Wait()`.

A notification that never arrives is not self-correcting: the tx stays in the submit map forever, its writes are never demoted so they can never be evicted from the auth cache, and every tx reading those keys re-executes forever. So the timeout has to do something — and what it must **not** do is assume the worst. A slow commit and a lost notification are indistinguishable from here, and rolling back a tx that is merely slow costs a re-execution plus, if it then commits after all, a spurious rollback of everything behind it. Hence: ask.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_commit_notifier_test.go (append)

func TestAdjudicateCommittedProceedsAsIfNotified(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	h.status.statuses["tx-1"] = TxCommitted
	h.now = h.now.Add(2 * time.Minute)

	h.phase.Adjudicate(context.Background())
	select {
	case n := <-h.out:
		if len(n.Committed) != 1 {
			t.Fatalf("notification = %+v, want the batch's committed keys", n.Committed)
		}
	case <-time.After(time.Second):
		t.Fatal("a COMMITTED adjudication must proceed exactly as a notification would")
	}
	if h.phase.Pending() != 0 {
		t.Errorf("Pending = %d, want 0", h.phase.Pending())
	}
}

func TestAdjudicateAbortedIsFatal(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	h.status.statuses["tx-1"] = TxAborted
	h.now = h.now.Add(2 * time.Minute)

	h.phase.Adjudicate(context.Background())
	select {
	case <-h.fatals:
	case <-time.After(time.Second):
		t.Fatal("an aborted status must trigger the same path as an abort notification")
	}
}

// STATUS_UNSPECIFIED means "not validated yet", i.e. still in flight. Keep waiting
// and re-arm; do NOT roll back.
func TestAdjudicatePendingKeepsWaitingAndReArms(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	h.status.statuses["tx-1"] = TxPending
	h.now = h.now.Add(2 * time.Minute)

	h.phase.Adjudicate(context.Background())
	select {
	case n := <-h.out:
		t.Fatalf("a still-in-flight tx must not be resolved; got %+v", n)
	case err := <-h.fatals:
		t.Fatalf("a still-in-flight tx must not be rolled back; got %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if h.phase.Pending() != 1 {
		t.Fatalf("Pending = %d, want 1 -- the tx stays registered", h.phase.Pending())
	}
	// Re-armed: it must not be asked about again until another Timeout has passed.
	h.phase.Adjudicate(context.Background())
	if len(h.status.queried) != 1 {
		t.Errorf("status was queried %d times, want 1 -- a pending tx must be re-armed, not re-asked immediately", len(h.status.queried))
	}
}

// A failed status query must change nothing: we still cannot tell slow from lost.
func TestAdjudicateQueryErrorLeavesEverythingPending(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	h.phase.Register("tx-1", aSubmitBatch(), aCommittedPayload())
	h.status.err = errors.New("query service unreachable")
	h.now = h.now.Add(2 * time.Minute)

	h.phase.Adjudicate(context.Background())
	select {
	case err := <-h.fatals:
		t.Fatalf("a failed status query must not roll anything back; got %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if h.phase.Pending() != 1 {
		t.Errorf("Pending = %d, want 1", h.phase.Pending())
	}
}

// Every tx that timed out together is adjudicated in ONE call.
func TestAdjudicateAsksAboutEveryOverdueTxInOneCall(t *testing.T) {
	h := newCommitHarness(t, time.Minute)
	for _, id := range []string{"tx-1", "tx-2", "tx-3"} {
		h.phase.Register(id, aSubmitBatch(), aCommittedPayload())
		h.status.statuses[id] = TxCommitted
	}
	h.phase.Register("tx-fresh", aSubmitBatch(), aCommittedPayload()) // registered at the same instant...
	h.now = h.now.Add(2 * time.Minute)
	// ...so make it not yet due by re-registering it at the new "now".
	h.phase.Register("tx-fresh", aSubmitBatch(), aCommittedPayload())

	h.phase.Adjudicate(context.Background())
	if len(h.status.queried) != 1 {
		t.Fatalf("made %d status calls, want 1", len(h.status.queried))
	}
	if len(h.status.queried[0]) != 3 {
		t.Fatalf("queried %v, want exactly the three overdue txIDs", h.status.queried[0])
	}
	for _, id := range h.status.queried[0] {
		if id == "tx-fresh" {
			t.Fatal("a tx that is not yet overdue must not be adjudicated")
		}
	}
}

func TestCommitPhaseWorkerExitsOnContextCancel(t *testing.T) {
	h := newCommitHarness(t, 40*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	h.phase.Start(ctx)
	cancel()
	done := make(chan struct{})
	go func() { h.phase.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the timeout worker must exit when the context is cancelled")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestAdjudicate|TestCommitPhaseWorker' -v`
Expected: FAIL — `h.phase.Adjudicate undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_commit_notifier.go (append)

// Start launches the timeout worker. It sweeps at a quarter of the timeout, so an
// overdue tx is adjudicated within Timeout*1.25 of submission rather than needing
// one timer per in-flight committer tx.
func (c *commitPhase) Start(ctx context.Context) {
	c.wg.Go(func() {
		interval := c.d.Timeout / 4
		if interval <= 0 {
			interval = c.d.Timeout
		}
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				c.Adjudicate(ctx)
			case <-ctx.Done():
				return
			}
		}
	})
}

// Wait blocks until the timeout worker has exited.
func (c *commitPhase) Wait() { c.wg.Wait() }

// Adjudicate asks the query service about every overdue committer tx, in ONE call,
// and acts on each answer:
//
//   - TxCommitted: proceed exactly as if the notification had arrived. The
//     keys/versions/values are still in the submit map, so the normal
//     refresh/stamp/demote flow applies unchanged.
//   - TxAborted: roll back -- fatal in this milestone.
//   - TxPending: not validated yet, i.e. still in flight. Keep waiting and re-arm
//     the timeout. Do NOT roll back.
//
// The third case is the reason the status is asked for rather than inferred from
// the timeout: a slow commit and a lost notification look identical from here, and
// rolling back a tx that is merely slow costs a re-execution and, if it then
// commits after all, a spurious rollback of everything that followed it.
//
// A failed query is not an answer, so nothing is resolved and nothing is re-armed:
// the next sweep asks again.
func (c *commitPhase) Adjudicate(ctx context.Context) {
	now := c.d.Now()
	c.mu.Lock()
	overdue := make([]string, 0, len(c.pending))
	for id, e := range c.pending {
		if !e.due.After(now) {
			overdue = append(overdue, id)
		}
	}
	c.mu.Unlock()
	if len(overdue) == 0 {
		return
	}
	if c.d.Status == nil {
		// No adjudicator wired: there is nothing safe to do, and assuming the worst
		// is exactly what this mechanism exists to avoid. Re-arm and log.
		logger.Warnf("commit timeout for %d committer tx(s) with no tx-status querier wired; still waiting", len(overdue))
		c.rearm(overdue, now)
		return
	}

	statuses, err := c.d.Status.TxStatus(ctx, overdue)
	if err != nil {
		logger.Warnf("adjudicate %d overdue committer tx(s): %v; still waiting", len(overdue), err)
		return
	}

	var stillPending []string
	for _, id := range overdue {
		switch statuses[id] {
		case TxCommitted:
			c.Handle(id, true)
		case TxAborted:
			c.Handle(id, false)
		default:
			stillPending = append(stillPending, id)
		}
	}
	if len(stillPending) > 0 {
		logger.Debugf("%d overdue committer tx(s) not validated yet; re-arming", len(stillPending))
		c.rearm(stillPending, now)
	}
}

// rearm pushes each tx's deadline forward by one timeout, so a tx the query
// service reports as still in flight is not re-asked on every sweep.
func (c *commitPhase) rearm(txIDs []string, now time.Time) {
	c.mu.Lock()
	for _, id := range txIDs {
		if e, ok := c.pending[id]; ok {
			e.due = now.Add(c.d.Timeout)
		}
	}
	c.mu.Unlock()
}
```

`logger` is the package logger already used by `gateway/core/executor.go`; no new declaration is needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -run 'TestCommit|TestAdjudicate' -race -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_commit_notifier.go gateway/core/stream_commit_notifier_test.go
git commit -m "feat(stream): adjudicate notification timeouts via GetTransactionStatus"
```

---
## Phase 8 — Orchestration and wiring

### Task 24: `StreamPipeline` — assemble the phases

**Files:**
- Create: `gateway/core/stream_pipeline.go`
- Modify: `gateway/core/stream_commit_notifier.go` (add `streamNotifier`)
- Test: `gateway/core/stream_pipeline_test.go`

**Interfaces:**
- Consumes: every phase built in Tasks 8–23; `OrderGate`; `notification.TxStatusEvent`.
- Produces:
  - `type StreamEndorser interface { warmExecutor; authExecutor; submitEndorser; EndorserCount() int }`
  - `type StreamOptions struct { Cfg StreamConfig; WarmUnder execution.ReadStore; AuthUnder execution.ReadStore; Status TxStatusQuerier }`
  - `type StreamDeps struct { Opts StreamOptions; Namespace string; Endorse StreamEndorser; Submit func(context.Context, sdk.Endorsement) error; Gate *OrderGate; Watch func(txID string) }`
  - `func NewStreamPipeline(d StreamDeps) (*StreamPipeline, error)`, and on it: `Start(ctx)`, `Stop()`, `Add(tx *types.Transaction) error`, `HandleCommit(txID string, committed bool)`, `Err() error`, `Stats() StreamStats`
  - `type streamNotifier` implementing `notification.TxStatusHandler` plus `Watch(txID string)`

`NewStreamPipeline` **fails fast** when `Endorse.EndorserCount() != 1`: the accept-as-is fast path decides on the gateway's own copy of a read-write set, which presumes one deterministic executor.

- [ ] **Step 1: Write the failing test**

```go
// gateway/core/stream_pipeline_test.go
package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	commonpb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"github.com/hyperledger/fabric-x-sdk/notification"
)

// fakePipelineEndorser executes a trivial read-modify-write on one key, through
// whichever store it is handed, so the pipeline's caches and version arithmetic are
// exercised end to end.
type fakePipelineEndorser struct {
	mu        sync.Mutex
	nextTxID  int
	endorsers int
	warmed    int
	reexeced  int
}

func (f *fakePipelineEndorser) EndorserCount() int {
	if f.endorsers == 0 {
		return 1
	}
	return f.endorsers
}

func (f *fakePipelineEndorser) rmw(store api.StateReader) execution.ExecutedTx {
	rec, err := store.Get("ns", "acct")
	if err != nil {
		return execution.ExecutedTx{Result: endorsement.ExecutionResult{Status: 500, Message: err.Error()}}
	}
	read := blocks.KVRead{Key: "acct"}
	ex := execution.ExecutedTx{}
	if rec != nil {
		read.Version = &blocks.Version{BlockNum: rec.Version}
		ex.Reads = []blocks.WriteRecord{*rec}
	}
	ex.Result = endorsement.ExecutionResult{
		Status: 200,
		RWS: blocks.ReadWriteSet{
			Reads:  []blocks.KVRead{read},
			Writes: []blocks.KVWrite{{Key: "acct", Value: []byte("next")}},
		},
	}
	return ex
}

func (f *fakePipelineEndorser) WarmBatchOn(_ context.Context, txs []*types.Transaction, store api.StateReader) ([]execution.ExecutedTx, error) {
	out := make([]execution.ExecutedTx, len(txs))
	for i := range txs {
		out[i] = f.rmw(store)
	}
	f.mu.Lock()
	f.warmed += len(txs)
	f.mu.Unlock()
	return out, nil
}

func (f *fakePipelineEndorser) AuthTxOn(_ context.Context, _ *types.Transaction, store api.StateReader) (execution.ExecutedTx, error) {
	f.mu.Lock()
	f.reexeced++
	f.mu.Unlock()
	return f.rmw(store), nil
}

func (f *fakePipelineEndorser) EndorseMerged(_ context.Context, _ []*types.Transaction, _ endorsement.ExecutionResult) (sdk.Endorsement, error) {
	f.mu.Lock()
	f.nextTxID++
	id := f.nextTxID
	f.mu.Unlock()
	return sdk.Endorsement{Proposal: proposalWithTxID(id)}, nil
}

// proposalWithTxID builds a proposal committerTxID can decode.
func proposalWithTxID(n int) *peer.Proposal {
	hdr := &commonpb.Header{
		ChannelHeader: protoutil.MarshalOrPanic(&commonpb.ChannelHeader{TxId: fmt.Sprintf("tx-%d", n)}),
	}
	return &peer.Proposal{Header: protoutil.MarshalOrPanic(hdr)}
}

func startPipeline(t *testing.T, cfg StreamConfig, e *fakePipelineEndorser, commitAfterOrder bool) (*StreamPipeline, *fakePipelineEndorser) {
	t.Helper()
	gate := NewOrderGate()
	var p *StreamPipeline
	var err error
	p, err = NewStreamPipeline(StreamDeps{
		Opts: StreamOptions{
			Cfg:       cfg,
			WarmUnder: newStreamFakeReader(map[string]*blocks.WriteRecord{"acct": {Key: "acct", Version: 1, Value: []byte("v1")}}),
			AuthUnder: newStreamFakeReader(map[string]*blocks.WriteRecord{"acct": {Key: "acct", Version: 1, Value: []byte("v1")}}),
			Status:    &fakeStatusQuerier{statuses: map[string]TxOutcome{}},
		},
		Namespace: "ns",
		Endorse:   e,
		Gate:      gate,
		Watch:     func(string) {},
		Submit: func(_ context.Context, end sdk.Endorsement) error {
			id, err := committerTxID(end.Proposal)
			if err != nil {
				return err
			}
			// Stand in for the orderer's block delivery, then for the committer's
			// notification.
			go func() {
				gate.Observe([]string{id})
				if commitAfterOrder {
					p.HandleCommit(id, true)
				}
			}()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewStreamPipeline: %v", err)
	}
	p.Start(context.Background())
	t.Cleanup(p.Stop)
	return p, e
}

func TestNewStreamPipelineRequiresExactlyOneEndorser(t *testing.T) {
	_, err := NewStreamPipeline(StreamDeps{
		Opts:      StreamOptions{Cfg: DefaultStreamConfig()},
		Namespace: "ns",
		Endorse:   &fakePipelineEndorser{endorsers: 2},
		Gate:      NewOrderGate(),
	})
	if err == nil {
		t.Fatal("NewStreamPipeline must reject more than one endorser: accept-as-is decides on the gateway's own read-write set")
	}
}

// End to end on the happy path: every tx added is submitted, committed, and the
// commit flows back through the warmup refresh and the auth demotion.
func TestStreamPipelineEndToEndCommitsEveryTx(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 2
	cfg.WarmBatchTimeout = 10 * time.Millisecond
	cfg.AuthBatchSize = 2
	cfg.AuthBatchTimeout = 10 * time.Millisecond
	p, e := startPipeline(t, cfg, &fakePipelineEndorser{}, true)

	const n = 20
	for i := range n {
		if err := p.Add(streamTx(uint64(i))); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Committed >= n {
			break
		}
		if err := p.Err(); err != nil {
			t.Fatalf("pipeline failed: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := p.Err(); err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	st := p.Stats()
	if st.Committed < n {
		t.Fatalf("committed %d of %d txs; stats = %+v", st.Committed, n, st)
	}
	if st.Accepted+st.Reexecuted < n {
		t.Errorf("settled %d txs (accepted %d + reexecuted %d), want at least %d", st.Accepted+st.Reexecuted, st.Accepted, st.Reexecuted, n)
	}
	e.mu.Lock()
	warmed := e.warmed
	e.mu.Unlock()
	if warmed < n {
		t.Errorf("warmed %d txs, want at least %d", warmed, n)
	}
	// Every submitted committer tx was resolved, so nothing is left in the map.
	if got := p.Stats().PendingCommits; got != 0 {
		t.Errorf("PendingCommits = %d, want 0", got)
	}
}

// An abort stops the pipeline: rollback is deferred, so continuing would pin the
// auth cache on writes that will never commit.
func TestStreamPipelineAbortStopsThePipeline(t *testing.T) {
	cfg := DefaultStreamConfig()
	cfg.WarmBatchSize = 1
	cfg.AuthBatchSize = 1
	cfg.AuthBatchTimeout = 10 * time.Millisecond
	gate := NewOrderGate()
	var p *StreamPipeline
	var err error
	p, err = NewStreamPipeline(StreamDeps{
		Opts: StreamOptions{
			Cfg:       cfg,
			WarmUnder: newStreamFakeReader(nil),
			AuthUnder: newStreamFakeReader(nil),
		},
		Namespace: "ns",
		Endorse:   &fakePipelineEndorser{},
		Gate:      gate,
		Watch:     func(string) {},
		Submit: func(_ context.Context, end sdk.Endorsement) error {
			id, _ := committerTxID(end.Proposal)
			go func() { gate.Observe([]string{id}); p.HandleCommit(id, false) }()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	defer p.Stop()

	_ = p.Add(streamTx(0))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && p.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if p.Err() == nil {
		t.Fatal("an abort must stop the pipeline while rollback is unimplemented")
	}
}

// The stream handler skips STATUS_UNSPECIFIED: that is the sidecar's own
// commit-timeout signal, and only the pipeline's batched adjudication can answer
// "still in flight".
func TestStreamNotifierSkipsUnspecifiedAndResolvesTheRest(t *testing.T) {
	var resolved []string
	var committed []bool
	subscribe := make(chan []string, 4)
	n := newStreamNotifier(subscribe, func(txID string, ok bool) {
		resolved = append(resolved, txID)
		committed = append(committed, ok)
	})
	n.Watch("tx-1")
	select {
	case ids := <-subscribe:
		if len(ids) != 1 || ids[0] != "tx-1" {
			t.Fatalf("subscribed %v, want [tx-1]", ids)
		}
	default:
		t.Fatal("Watch must register the txID with the notification stream")
	}

	err := n.Handle(context.Background(), []notification.TxStatusEvent{
		{TxID: "tx-1", Status: committerpb.Status_COMMITTED},
		{TxID: "tx-2", Status: committerpb.Status_STATUS_UNSPECIFIED},
		{TxID: "tx-3", Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(resolved) != 2 || resolved[0] != "tx-1" || resolved[1] != "tx-3" {
		t.Fatalf("resolved %v, want [tx-1 tx-3] -- STATUS_UNSPECIFIED must be left to the pipeline's adjudication", resolved)
	}
	if !committed[0] || committed[1] {
		t.Errorf("committed = %v, want [true false]", committed)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/core/ -run 'TestNewStreamPipeline|TestStreamPipeline|TestStreamNotifier' -v`
Expected: FAIL — `undefined: NewStreamPipeline`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/core/stream_commit_notifier.go (append)

// streamNotifier is the streaming pipeline's notification-stream demux. It
// implements notification.TxStatusHandler and does two things only: register a
// txID with the stream on Watch, and forward each event's verdict.
//
// It deliberately does NOT arm a per-TxID timer the way txNotifier does. A timer
// there can only answer "committed?" as a bool, and the one answer the streaming
// design insists on is the third one -- "not validated yet, keep waiting" -- so
// timeouts belong to commitPhase.Adjudicate, which asks the query service and can
// express it. For the same reason a STATUS_UNSPECIFIED event, which is the
// sidecar's own commit-timeout signal rather than an outcome, is skipped here.
type streamNotifier struct {
	subscribe chan<- []string
	resolve   func(txID string, committed bool)
}

func newStreamNotifier(subscribe chan<- []string, resolve func(txID string, committed bool)) *streamNotifier {
	return &streamNotifier{subscribe: subscribe, resolve: resolve}
}

// Watch registers txID with the notification stream. Called before the committer
// tx is broadcast (register-then-submit); the send is blocking by design, so the
// subscribe channel must be buffered and drained.
func (n *streamNotifier) Watch(txID string) {
	n.subscribe <- []string{txID}
}

// Handle forwards every real verdict and skips the sidecar's commit-timeout
// signal. Never returns an error: a handler error is non-fatal to the stream, and
// exactly-once resolution is enforced downstream by commitPhase.Handle.
func (n *streamNotifier) Handle(_ context.Context, events []notification.TxStatusEvent) error {
	for _, ev := range events {
		if ev.Status == committerpb.Status_STATUS_UNSPECIFIED {
			continue
		}
		n.resolve(ev.TxID, ev.Valid())
	}
	return nil
}
```

Add `"github.com/hyperledger/fabric-x-common/api/committerpb"` and `"github.com/hyperledger/fabric-x-sdk/notification"` to that file's imports.

```go
// gateway/core/stream_pipeline.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	sdk "github.com/hyperledger/fabric-x-sdk"
)

// ErrStreamStopped is returned by Add once the pipeline has stopped, whether from
// shutdown or a fatal error (see StreamPipeline.Err for the cause).
var ErrStreamStopped = errors.New("streaming pipeline stopped")

// StreamEndorser is everything the streaming pipeline asks of the endorsement
// client: warm a batch, re-execute one tx, and sign a merged committer tx.
// *EndorsementClient satisfies it.
type StreamEndorser interface {
	warmExecutor
	authExecutor
	submitEndorser
	EndorserCount() int
}

// StreamOptions is the streaming pipeline's configuration plus the two
// query-service readers and the tx-status adjudicator, none of which the gateway
// can build for itself.
//
// WarmUnder must be backed by a CONNECTION POOL and be safe for concurrent use;
// AuthUnder should be a dedicated connection. Neither may memoize reads -- see
// query.DirectReader for why.
type StreamOptions struct {
	Cfg       StreamConfig
	WarmUnder execution.ReadStore
	AuthUnder execution.ReadStore
	Status    TxStatusQuerier
}

// StreamDeps is the pipeline's full wiring.
type StreamDeps struct {
	Opts      StreamOptions
	Namespace string
	Endorse   StreamEndorser
	Submit    func(ctx context.Context, e sdk.Endorsement) error
	Gate      *OrderGate
	// Watch registers a committer TxID with the notification stream before it is
	// broadcast. May be nil in tests.
	Watch func(txID string)
}

// StreamStats is a snapshot for tests and observability.
type StreamStats struct {
	Accepted       uint64 // txs whose warm read-write set was taken verbatim
	Reexecuted     uint64 // txs the auth phase had to run again
	Submitted      uint64 // committer txs broadcast and observed in an ordered block
	Committed      uint64 // evm txs in committed committer txs
	PendingCommits int    // submitted committer txs awaiting an outcome
	Watermark      int64  // highest contiguous authed warm batch number
	WarmCacheBytes int64
	AuthReadBytes  int64
	StaleRejects   uint64 // accept-as-is refusals from a newer cached version (watermark bug signal)
}

// StreamPipeline is the three-phase streaming executor of
// evm-design/optimization.md: a concurrent warmup phase, a serial auth phase and
// an ordered submit phase, connected by queues, with commit notifications flowing
// back commit-notifier -> warmup (refresh + stamp) -> auth (demote under the
// watermark).
//
// It replaces, rather than extends, the previous warm(N+1)||auth(N) pipelined
// executor. That design cached SPECULATIVE writes at predicted versions, and a
// mispredicted chain produced an over-read and an abort that the retry reproduced
// exactly (see docs/evm-design-impl/findings.md section 6). Here the warmup cache
// holds committed state only, so there is nothing to mispredict; the cost is that
// a warm read of a key the pipeline itself has written is stale, and the auth
// phase's write-state check plus the watermark catch it.
//
// MILESTONE SCOPE: no rollback and no adaptive submit batching. Any abort is
// fatal -- Err reports it and the pipeline stops. Both are phase-2 work.
type StreamPipeline struct {
	cfg StreamConfig

	warmCache *WarmCache
	authCache *AuthCache

	in          chan *types.Transaction
	warmQ       chan WarmedBatchResult
	commitQ     chan CommitNotification // commit notifier -> warmup notification worker
	authNotifyQ chan CommitNotification // warmup notification worker -> auth worker
	submitQ     chan submitBatch

	warm   *warmPhase
	auth   *authPhase
	submit *submitPhase
	commit *commitPhase

	committed atomic.Uint64
	firstErr  atomic.Pointer[error]

	cancel   context.CancelFunc
	stopped  chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewStreamPipeline builds the pipeline. It rejects a multi-endorser client: the
// accept-as-is fast path decides on the gateway's own copy of a read-write set,
// which presumes one deterministic executor. Reconciling divergent read-write sets
// across endorsers is a BFT question and out of this milestone's scope.
func NewStreamPipeline(d StreamDeps) (*StreamPipeline, error) {
	if d.Endorse == nil {
		return nil, fmt.Errorf("streaming pipeline: no endorser")
	}
	if n := d.Endorse.EndorserCount(); n != 1 {
		return nil, fmt.Errorf("streaming pipeline requires exactly 1 endorser, got %d: the accept-as-is fast path decides on the gateway's own read-write set", n)
	}
	cfg := d.Opts.Cfg
	cfg.Sanitize()

	p := &StreamPipeline{
		cfg:         cfg,
		warmCache:   NewWarmCache(d.Namespace, cfg.WarmCacheBytes),
		authCache:   NewAuthCache(d.Namespace, cfg.AuthReadCacheBytes),
		in:          make(chan *types.Transaction, cfg.WarmQueueSize),
		warmQ:       make(chan WarmedBatchResult, cfg.WarmBatchQueueSize),
		commitQ:     make(chan CommitNotification, cfg.NotifyQueueSize),
		authNotifyQ: make(chan CommitNotification, cfg.NotifyQueueSize),
		submitQ:     make(chan submitBatch, cfg.SubmitQueueSize),
		stopped:     make(chan struct{}),
	}

	p.commit = newCommitPhase(commitDeps{
		Out:         p.commitQ,
		Status:      d.Opts.Status,
		Timeout:     cfg.CommitTimeout,
		Fatal:       p.fail,
		OnCommitted: func(evmTxs int) { p.committed.Add(uint64(evmTxs)) },
	})
	p.warm = newWarmPhase(cfg, p.warmCache, d.Opts.WarmUnder, d.Endorse, p.in, p.warmQ, p.fail)
	p.auth = newAuthPhase(authDeps{
		Cfg: cfg, Namespace: d.Namespace, Cache: p.authCache, Under: d.Opts.AuthUnder, Exec: d.Endorse,
		WarmIn: p.warmQ, NotifyIn: p.authNotifyQ, Out: p.submitQ,
		Requeue: p.requeue, Fatal: p.fail,
	})
	p.submit = newSubmitPhase(submitDeps{
		In: p.submitQ, Endorse: d.Endorse, Submit: d.Submit, Gate: d.Gate,
		Register: func(txID string, sb submitBatch, committed []StreamKV) {
			// Both registrations must precede the broadcast: the submit map holds the
			// only copy of the committed keys/versions/values, and the notification
			// stream must already know the txID.
			p.commit.Register(txID, sb, committed)
			if d.Watch != nil {
				d.Watch(txID)
			}
		},
		Fatal:        p.fail,
		OrderTimeout: cfg.CommitTimeout,
	})
	return p, nil
}

// Start launches every worker. The consumers start before the producers so no
// queue send can block on a worker that has not run yet.
func (p *StreamPipeline) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)

	p.commit.Start(ctx) // timeout adjudication
	p.submit.Start(ctx)
	p.auth.Start(ctx)

	p.wg.Go(func() { runWarmNotifier(ctx, p.warmCache, p.warm, p.commitQ, p.authNotifyQ) })

	p.warm.Start(ctx)
}

// Stop cancels every worker and waits for them. Idempotent.
func (p *StreamPipeline) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopped)
		if p.cancel != nil {
			p.cancel()
		}
	})
	p.commit.Stop()
	p.warm.Wait()
	p.auth.Wait()
	p.submit.Wait()
	p.commit.Wait()
	p.wg.Wait()
}

// Add puts a tx on the input queue. It blocks while the queue is full, which is
// the pipeline's backpressure, and fails once the pipeline has stopped.
func (p *StreamPipeline) Add(tx *types.Transaction) error {
	if err := p.Err(); err != nil {
		return err
	}
	select {
	case p.in <- tx:
		return nil
	case <-p.stopped:
		return ErrStreamStopped
	}
}

// HandleCommit resolves a submitted committer tx. Wired to the notification
// stream via streamNotifier, and called by commitPhase.Adjudicate on a timeout.
func (p *StreamPipeline) HandleCommit(txID string, committed bool) {
	p.commit.Handle(txID, committed)
}

// Err reports the first fatal error, or nil.
func (p *StreamPipeline) Err() error {
	if e := p.firstErr.Load(); e != nil {
		return *e
	}
	return nil
}

// Stats snapshots the pipeline's counters.
func (p *StreamPipeline) Stats() StreamStats {
	return StreamStats{
		Accepted:       p.auth.Accepted(),
		Reexecuted:     p.auth.Reexecuted(),
		Submitted:      p.submit.Submitted(),
		Committed:      p.committed.Load(),
		PendingCommits: p.commit.Pending(),
		Watermark:      p.auth.Prev(),
		WarmCacheBytes: p.warmCache.Bytes(),
		AuthReadBytes:  p.authCache.ReadBytes(),
		StaleRejects:   p.authCache.StaleReadRejects(),
	}
}

// fail records the first fatal error and stops the pipeline. Later errors are
// logged and dropped: the first one is the diagnosis, the rest are its shrapnel.
func (p *StreamPipeline) fail(err error) {
	if p.firstErr.CompareAndSwap(nil, &err) {
		logger.Errorf("streaming pipeline stopping: %v", err)
		p.stopOnce.Do(func() {
			close(p.stopped)
			if p.cancel != nil {
				p.cancel()
			}
		})
		return
	}
	logger.Warnf("streaming pipeline (already stopping): %v", err)
}

// requeue returns a retryably-rejected tx to the tail of the input queue.
//
// The send runs in its own goroutine, and that is not laziness: the auth worker
// calls this, and a direct blocking send would deadlock when the input queue is
// full -- the warm TXs worker would be waiting on in-flight slots that batch
// workers can only release by pushing to the warm queue, which only the blocked
// auth worker drains. Ordering across a requeue is already not preserved (see the
// auth worker), so a concurrent send changes nothing about the guarantees.
func (p *StreamPipeline) requeue(tx *types.Transaction) {
	go func() {
		select {
		case p.in <- tx:
		case <-p.stopped:
		}
	}()
}
```

`commitDeps` gains one field, and `commitPhase.Handle` calls it on the committed path only — so `Stats().Committed` counts EVM txs that actually committed, not ones that were merely submitted:

```go
// gateway/core/stream_commit_notifier.go

// commitDeps gains:
	// OnCommitted, when non-nil, is called with a resolved committer tx's EVM tx
	// count on the COMMITTED path only.
	OnCommitted func(evmTxs int)

// and in Handle, immediately after the RecordCommitLatency call:
	if c.d.OnCommitted != nil {
		c.d.OnCommitted(len(e.txs))
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./gateway/core/ -race -v -run 'TestNewStreamPipeline|TestStreamPipeline|TestStreamNotifier'`
Expected: PASS (4 tests). Then the whole package: `go test ./gateway/... -race`.

- [ ] **Step 5: Commit**

```bash
git add gateway/core/stream_pipeline.go gateway/core/stream_commit_notifier.go gateway/core/stream_pipeline_test.go
git commit -m "feat(stream): assemble the three-phase streaming pipeline"
```

---
### Task 25: Wire it into the gateway and retire the old pipelined executor

**Files:**
- Modify: `gateway/config/config.go:45-68` (`Gateway` struct: repurpose `Pipelined`, add the stream knobs) and its validation at `:121-128`
- Modify: `gateway/core/api.go` (`SetStreaming`, `SetStreamNotifier`, `Start`, `AddPending`, `StreamStats` accessor)
- Modify: `gateway/core/executor.go` (delete `runExecutorPipelined`, `drainAndWarm`, `pipelineIteration`, `warmResult`)
- Modify: `gateway/app/wiring.go` (`BuildGateway` takes `*core.StreamOptions`; force one submitter + the order gate)
- Modify: `gateway/app/app.go:174` (build the readers and the status adapter; pass them)
- Create: `gateway/app/stream_readers.go` (the two nil-view readers and the tx-status adapter)
- Modify: `integration/test_helpers.go:260` (pass `nil` for `StreamOptions`)
- Test: `gateway/app/stream_readers_test.go`, and `gateway/core/api_test.go` (append)

**Interfaces:**
- Consumes: `StreamPipeline` / `StreamOptions` / `TxStatusQuerier` (Task 24), `query.NewStore` / `NewDirectReader` (Task 6), `query.GRPCClient.GetTxStatus` (Task 7).
- Produces:
  - `func (g *Gateway) SetStreaming(o *StreamOptions)` — enable the streaming executor; call before `Start`.
  - `func (g *Gateway) SetStreamNotifier(subscribe chan<- []string) notification.TxStatusHandler`
  - `func (g *Gateway) StreamStats() (StreamStats, bool)`
  - `func newStreamReaders(ecfg econfig.Endorser, namespace string) (warm, auth execution.ReadStore, status core.TxStatusQuerier, closeAll func(), err error)` in `gateway/app`
  - `func txStatusAdapter(c query.TxStatusClient) core.TxStatusQuerier` in `gateway/app`

`Gateway.Pipelined` / the `-pipeline` flag now select **this** pipeline. The previous meaning is gone along with its loop: it was the unsolved trilemma (see [findings §6](../findings.md)), it was never a default, and leaving two things called "pipelined" in one package would be worse than either.

**The serial path is untouched.** `Pipelined == false` still runs `runExecutor`'s `executeCycle` over the `PendingPool` and `VersionedCache`, byte for byte.

- [ ] **Step 1: Write the failing test**

```go
// gateway/app/stream_readers_test.go
package app

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/gateway/core"
)

type stubTxStatusClient struct {
	out map[string]committerpb.Status
	err error
}

func (s *stubTxStatusClient) GetTxStatus(context.Context, []string) (map[string]committerpb.Status, error) {
	return s.out, s.err
}

func TestTxStatusAdapterCollapsesTheStatusEnum(t *testing.T) {
	a := txStatusAdapter(&stubTxStatusClient{out: map[string]committerpb.Status{
		"c": committerpb.Status_COMMITTED,
		"m": committerpb.Status_ABORTED_MVCC_CONFLICT,
		"s": committerpb.Status_ABORTED_SIGNATURE_INVALID,
		"d": committerpb.Status_REJECTED_DUPLICATE_TX_ID,
		"b": committerpb.Status_MALFORMED_BAD_ENVELOPE,
		"u": committerpb.Status_STATUS_UNSPECIFIED,
	}})
	got, err := a.TxStatus(context.Background(), []string{"c", "m", "s", "d", "b", "u"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]core.TxOutcome{
		"c": core.TxCommitted,
		"m": core.TxAborted,
		"s": core.TxAborted,
		"d": core.TxAborted,
		"b": core.TxAborted,
		"u": core.TxPending, // not validated yet: keep waiting, never roll back
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s -> %v, want %v", id, got[id], w)
		}
	}
}
```

```go
// gateway/core/api_test.go (append)

func TestSetStreamingSelectsTheStreamingExecutor(t *testing.T) {
	g := newTestGateway(t) // use the package's existing gateway test constructor
	if _, ok := g.StreamStats(); ok {
		t.Fatal("StreamStats must report false before SetStreaming")
	}
	g.SetStreaming(&StreamOptions{
		Cfg:       DefaultStreamConfig(),
		WarmUnder: newStreamFakeReader(nil),
		AuthUnder: newStreamFakeReader(nil),
	})
	if _, ok := g.StreamStats(); !ok {
		t.Fatal("StreamStats must report true after SetStreaming")
	}
}
```

If `gateway/core`'s tests have no shared gateway constructor, build one inline from `core.New` the way the existing tests in the package do (grep for `core.New(` / `New(ec,` in `gateway/core/*_test.go`) and pass a single-endorser `EndorsementClient`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./gateway/app/ -run TestTxStatusAdapter -v && go test ./gateway/core/ -run TestSetStreaming -v`
Expected: FAIL — `undefined: txStatusAdapter`, `g.SetStreaming undefined`.

- [ ] **Step 3: Write minimal implementation**

```go
// gateway/app/stream_readers.go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package app

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	econfig "github.com/hyperledger/fabric-x-evm/endorser/config"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/endorser/query"
	"github.com/hyperledger/fabric-x-evm/gateway/core"
)

// defaultStreamWarmConnections is the warm phase's pool size when the endorser
// config leaves it unset. Concurrent reads on a single HTTP/2 connection
// serialize; a sweep measured 4212 tx/s at 1 connection rising to a ~5040 tx/s
// plateau at 4-16 and regressing at 32.
const defaultStreamWarmConnections = 8

// newStreamReaders builds the streaming pipeline's two query-service readers and
// its tx-status adjudicator.
//
// The warm reader is backed by a POOL, because a whole batch's reads fire at once.
// The auth reader is backed by ONE DEDICATED connection, because the auth worker
// reads serially and sharing the warm transports would let a warm burst perturb
// the pipeline's serial floor.
//
// Both are nil-view and non-memoizing (query.DirectReader): a pinned view
// reintroduces the commit-visibility skew this design removes, and per-view
// memoization would silently defeat the warmup cache's notification refresh.
//
// closeAll shuts both clients down; the caller defers it for the process's life.
func newStreamReaders(ecfg econfig.Endorser, namespace string) (warm, auth execution.ReadStore, status core.TxStatusQuerier, closeAll func(), err error) {
	if ecfg.Database.Database != "query-service" {
		return nil, nil, nil, nil, fmt.Errorf("streaming pipeline requires the query-service database, got %q", ecfg.Database.Database)
	}

	n := ecfg.QueryServiceConnections
	if n <= 0 {
		n = defaultStreamWarmConnections
	}
	var opened []*grpc.ClientConn
	closeOpened := func() {
		for _, c := range opened {
			_ = c.Close()
		}
	}
	for i := range n + 1 { // n for the warm pool, 1 dedicated for auth
		conn, dialErr := query.Dial(ecfg.QueryService)
		if dialErr != nil {
			closeOpened()
			return nil, nil, nil, nil, fmt.Errorf("dial query service (connection %d/%d): %w", i+1, n+1, dialErr)
		}
		opened = append(opened, conn)
	}

	warmClient := query.NewGRPCClientPool(opened[:n], ecfg.ViewTimeout)
	warmStore := query.NewStore(warmClient, namespace)
	warmStore.SetNilView(true)

	authClient := query.NewGRPCClient(opened[n], ecfg.ViewTimeout)
	authStore := query.NewStore(authClient, namespace)
	authStore.SetNilView(true)

	return warmStore.NewDirectReader(),
		authStore.NewDirectReader(),
		txStatusAdapter(warmClient),
		func() { _ = warmStore.Close(); _ = authStore.Close() },
		nil
}

// txStatusAdapter maps the query service's status enum onto the three outcomes the
// pipeline distinguishes. It lives here, at the wiring layer, so gateway/core
// carries no dependency on the committer protos.
//
// STATUS_UNSPECIFIED maps to TxPending -- "not validated yet, still in flight" --
// and is the case the whole mechanism exists for: it is what lets a timeout keep
// waiting instead of rolling back a tx that is merely slow. Every other non-
// COMMITTED status (aborted, rejected, malformed) is TxAborted.
func txStatusAdapter(c query.TxStatusClient) core.TxStatusQuerier {
	return &statusAdapter{c: c}
}

type statusAdapter struct{ c query.TxStatusClient }

func (a *statusAdapter) TxStatus(ctx context.Context, txIDs []string) (map[string]core.TxOutcome, error) {
	raw, err := a.c.GetTxStatus(ctx, txIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]core.TxOutcome, len(raw))
	for id, st := range raw {
		switch st {
		case committerpb.Status_COMMITTED:
			out[id] = core.TxCommitted
		case committerpb.Status_STATUS_UNSPECIFIED:
			out[id] = core.TxPending
		default:
			out[id] = core.TxAborted
		}
	}
	return out, nil
}
```

```go
// gateway/core/api.go -- replace the `pipelined bool` field's doc comment and add:

// stream, when non-nil, selects the three-phase streaming executor
// (StreamPipeline) instead of the serial drain loop. Set once via SetStreaming
// before Start, so it is never written concurrently with the executor.
//
// This is what the gateway YAML's `pipelined:` flag now selects. It previously
// selected a warm(N+1)||auth(N) loop over a cache of SPECULATIVE writes, which
// livelocked on conflict-heavy traffic (docs/evm-design-impl/findings.md section
// 6); that loop is gone.
stream *StreamPipeline

// SetStreaming enables the streaming executor. o supplies the config and the two
// query-service readers; nil leaves the gateway on the serial path. Call before
// Start. Returns an error if the pipeline cannot be built (e.g. more than one
// endorser).
func (g *Gateway) SetStreaming(o *StreamOptions) error {
	if o == nil {
		return nil
	}
	p, err := NewStreamPipeline(StreamDeps{
		Opts:      *o,
		Namespace: g.namespace,
		Endorse:   g.endorsers,
		Submit:    g.SubmitFabricTx,
		Gate:      g.orderGate,
		Watch:     g.Watch,
	})
	if err != nil {
		return err
	}
	g.stream = p
	return nil
}

// SetStreamNotifier installs the streaming pipeline's notification-stream demux
// and returns it as the notification.TxStatusHandler the caller feeds to
// notification.NewProcessor. Unlike SetNotifier it arms no per-TxID timer: the
// pipeline's own batched adjudication owns timeouts, because only it can express
// "not validated yet, keep waiting". Call after SetStreaming, before Start.
func (g *Gateway) SetStreamNotifier(subscribe chan<- []string) notification.TxStatusHandler {
	n := newStreamNotifier(subscribe, g.stream.HandleCommit)
	g.streamNotifier = n
	return n
}

// StreamStats reports the streaming pipeline's counters, and false when the
// gateway is on the serial path.
func (g *Gateway) StreamStats() (StreamStats, bool) {
	if g.stream == nil {
		return StreamStats{}, false
	}
	return g.stream.Stats(), true
}

// StreamErr reports the streaming pipeline's first fatal error, or nil.
func (g *Gateway) StreamErr() error {
	if g.stream == nil {
		return nil
	}
	return g.stream.Err()
}
```

`Gateway` needs a `namespace string` field (from `netCfg.Namespace`, already passed to `NewEndorsementClient`), an `orderGate *OrderGate` field set by a new `SetOrderGate(*OrderGate)`, and a `streamNotifier *streamNotifier` field. Add them alongside the existing setters.

In `Start`, select the executor (and delete `runExecutor`'s own `defer g.wg.Done()`, since `wg.Go` now owns it):

```go
	if g.stream != nil {
		g.stream.Start(ctx)
		return
	}
	g.wg.Go(func() { g.runExecutor(ctx) })
```

In `AddPending`, route to the pipeline:

```go
func (g *Gateway) AddPending(tx *types.Transaction) {
	if g.stream != nil {
		// The streaming pipeline owns its own input queue; the PendingPool and its
		// arrivals signal belong to the serial executor. A full queue blocks here,
		// which is the pipeline's backpressure.
		if err := g.stream.Add(tx); err != nil {
			logger.Errorf("streaming pipeline rejected tx %s: %v", tx.Hash(), err)
		}
		return
	}
	g.pending.Add(tx)
	select {
	case g.arrivals <- struct{}{}:
	default:
	}
}
```

Note in `SendTransaction`'s doc that its `g.pending.Has` duplicate check is inert on the streaming path (the pool is unused there), so a duplicate submission reaches the pipeline and is settled by nonce classification instead.

Delete from `gateway/core/executor.go`: `warmResult` (line 255), `runExecutorPipelined` (line 277), `drainAndWarm`, `pipelineIteration`, and the `if g.pipelined { ... }` branch in `runExecutor`. Also delete `Gateway.pipelined` and `SetPipelined`, replacing their single call site in `wiring.go` with `SetStreaming`.

In `gateway/app/wiring.go`, change `BuildGateway`'s trailing `pipelined bool` parameter to `streaming *core.StreamOptions`, and:

```go
	// The streaming pipeline REQUIRES ordered single submission: every version its
	// auth cache predicts assumes commits land in submission order.
	if streaming != nil {
		if submitterCount != 1 {
			logger.Warnf("streaming pipeline requires ordered single submission; clamping submitter-count %d to 1", submitterCount)
			submitterCount = 1
		}
		if orderGate == nil {
			return nil, fmt.Errorf("streaming pipeline requires an ordered-delivery gate (set gateway.ordered-submit)")
		}
	}
```

and after the existing setters:

```go
	gw.SetOrderGate(orderGate)
	if err := gw.SetStreaming(streaming); err != nil {
		return nil, fmt.Errorf("enable streaming pipeline: %w", err)
	}
```

In `gateway/app/app.go` (around line 174), build the options when `cfg.Gateway.Pipelined` is set:

```go
	var streamOpts *core.StreamOptions
	if cfg.Gateway.Pipelined {
		if len(cfg.Endorsers) != 1 {
			return nil, fmt.Errorf("streaming pipeline requires exactly 1 endorser, got %d", len(cfg.Endorsers))
		}
		warmUnder, authUnder, status, closeReaders, err := newStreamReaders(cfg.Endorsers[0], cfg.Network.Namespace)
		if err != nil {
			return nil, fmt.Errorf("build streaming readers: %w", err)
		}
		app.onShutdown = append(app.onShutdown, closeReaders)
		streamOpts = &core.StreamOptions{
			Cfg:       cfg.Gateway.Stream,
			WarmUnder: warmUnder,
			AuthUnder: authUnder,
			Status:    status,
		}
	}
```

If `App` has no shutdown-hook slice, add one (`onShutdown []func()`, run from its existing `Close`/`Stop`); the two query clients must be closed or the process leaks their connections.

Finally, in `gateway/config/config.go`, extend the `Gateway` struct and its validation:

```go
	// Pipelined selects the three-phase STREAMING executor (see
	// core.StreamPipeline). It requires ordered single submission and exactly one
	// endorser, both of which the wiring enforces. Default false (serial).
	Pipelined bool `mapstructure:"pipelined" yaml:"pipelined"`

	// Stream tunes the streaming executor. Every zero field takes its documented
	// default (core.StreamConfig.Sanitize), so an empty block is valid.
	Stream core.StreamConfig `mapstructure:"stream" yaml:"stream"`
```

and in validation:

```go
	if c.Gateway.Pipelined && !c.Gateway.OrderedSubmit {
		return fmt.Errorf("gateway.pipelined requires gateway.ordered-submit: the streaming pipeline's version predictions assume commits land in submission order")
	}
```

Update `integration/test_helpers.go:260` and `gateway/app/app.go:174` to the new `BuildGateway` signature; the harness passes `nil` unless a test opts in.

- [ ] **Step 4: Run test to verify it passes**

Run: `go build ./... && go test ./gateway/... ./endorser/... ./integration/ -race`
Expected: PASS, including every existing serial test. If any test referenced `SetPipelined`, point it at `SetStreaming` or drop it — the old loop no longer exists.

- [ ] **Step 5: Commit**

```bash
git add gateway/config/config.go gateway/core/api.go gateway/core/executor.go gateway/app/wiring.go gateway/app/app.go gateway/app/stream_readers.go gateway/app/stream_readers_test.go gateway/core/api_test.go integration/test_helpers.go
git commit -m "feat(gateway): select the streaming executor via pipelined; retire the old pipelined loop"
```

---
## Phase 9 — Validate against serial, then remove the dead machinery

### Task 26: Replay both datasets and record the numbers

**Files:**
- Modify: `gateway/app/stream_readers.go` (export `newStreamReaders` as `NewStreamReaders`)
- Modify: `integration/test_helpers.go` (build `StreamOptions` when `cfg.Gateway.Pipelined`)
- Modify: `integration/perf/replay_json_dataset_test.go` (report the streaming counters; fail on any abort)
- Create: `docs/evm-design-impl/reports/2026-08-streaming-pipeline-milestone.md`

**Interfaces:**
- Consumes: `Gateway.StreamStats` / `StreamErr` (Task 25), `app.NewStreamReaders`.
- Produces: the milestone measurement. No new production symbols.

**This is the task that decides whether the milestone succeeded.** The exit criterion is: on a conflict-free replay of **both** datasets, streaming EVM tx/s exceeds serial on the same host and dataset, with **zero** MVCC aborts. If it does not, the outcome is a measurement and a diagnosis — not a feature — and phase 2 does not start until the gap is explained.

- [ ] **Step 1: Write the failing test**

The harness currently passes `nil` for `StreamOptions`, so `-pipeline` would silently run serial. Catch that first:

```go
// integration/perf/replay_json_dataset_test.go (append)

// TestPipelineFlagActuallySelectsTheStreamingExecutor guards against the failure
// mode where -pipeline is set, the harness passes no StreamOptions, and the run
// silently measures the serial executor instead.
func TestPipelineFlagActuallySelectsTheStreamingExecutor(t *testing.T) {
	if !*pipeline {
		t.Skip("run with -pipeline")
	}
	h := newReplayHarness(t) // the harness this file already builds for a replay run
	if _, ok := h.Gateway.StreamStats(); !ok {
		t.Fatal("-pipeline was set but the gateway is on the serial path: the harness is not passing StreamOptions")
	}
}
```

Adapt `newReplayHarness` to whatever constructor the file already uses for its replay runs (grep for `NewFabricXTestHarnessWithNotifications` / `NewLocalTestHarness` in this file) and expose the gateway from it.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./integration/perf/ -run TestPipelineFlagActuallySelects -pipeline -v`
Expected: FAIL — "the harness is not passing StreamOptions".

- [ ] **Step 3: Write minimal implementation**

Rename `newStreamReaders` to `NewStreamReaders` in `gateway/app/stream_readers.go` (and its call site in `app.go`), then in `integration/test_helpers.go`, before the `app.BuildGateway` call at line 260:

```go
	var streamOpts *core.StreamOptions
	if cfg.Gateway.Pipelined {
		warmUnder, authUnder, status, closeReaders, err := app.NewStreamReaders(cfg.Endorsers[0], cfg.Network.Namespace)
		if err != nil {
			t.Fatalf("build streaming readers: %v", err)
		}
		t.Cleanup(closeReaders)
		streamOpts = &core.StreamOptions{
			Cfg:       cfg.Gateway.Stream,
			WarmUnder: warmUnder,
			AuthUnder: authUnder,
			Status:    status,
		}
	}
```

and pass `streamOpts` in place of the former `pipelined` argument (fourth from the end, before `cache`, `orderGate`, `orderTimeout`). The streaming path also needs the notification demux instead of the per-TxID notifier: where the harness calls `gw.SetNotifier(subscribe, timeout)`, call `gw.SetStreamNotifier(subscribe)` when `cfg.Gateway.Pipelined`.

Then extend the replay run's reporting. Where the test prints `Replay complete: ... X EVM tx/s`, add:

```go
	if st, ok := h.Gateway.StreamStats(); ok {
		settled := st.Accepted + st.Reexecuted
		ratio := 0.0
		if settled > 0 {
			ratio = float64(st.Accepted) / float64(settled)
		}
		t.Logf("STREAM accepted=%d reexecuted=%d accept-ratio=%.3f submitted=%d committed=%d watermark=%d warm-cache=%dB auth-read=%dB stale-rejects=%d",
			st.Accepted, st.Reexecuted, ratio, st.Submitted, st.Committed, st.Watermark,
			st.WarmCacheBytes, st.AuthReadBytes, st.StaleRejects)
		// stale-rejects is a HEALTH metric, not a failure. It counts warm reads the
		// auth cache caught as stale -- which is what protects a key the warmup
		// cache had evicted when the query service still trails our own commit. A
		// large count means the warm cache budget is too small for the working set,
		// not that anything is wrong.
		if st.StaleRejects > 0 {
			t.Logf("STREAM stale-rejects=%d (warm cache %dB of %dB): the auth cache caught this many stale warm reads; raise stream.warm-cache-bytes if the count is large",
				st.StaleRejects, st.WarmCacheBytes, cfg.Gateway.Stream.WarmCacheBytes)
		}
		if err := h.Gateway.StreamErr(); err != nil {
			t.Fatalf("streaming pipeline failed: %v", err)
		}
	}
```

The accept ratio is the number to watch: the overlap only pays to the extent warm work survives the auth phase.

- [ ] **Step 4: Run the measurement**

Serial baseline and streaming, both datasets, same host. Data lives outside the repo at `$EVM_PERF_DATA`; deploy with `GOGC=500` and a high `GOMEMLIMIT`, never `GOGC=off`.

```bash
# Serial baseline
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v \
  -dataset synthetic -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/serial-synthetic.log
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v \
  -dataset historic  -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/serial-historic.log

# Streaming
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v -pipeline \
  -dataset synthetic -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/stream-synthetic.log
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v -pipeline \
  -dataset historic  -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/stream-historic.log
```

Expected in each streaming log: a `Replay complete: ... EVM tx/s` line whose rate exceeds the matching serial run's, `stale-rejects=0`, and no `streaming pipeline failed`. Keep the raw logs outside the repo.

- [ ] **Step 5: Write the report and commit**

Write `docs/evm-design-impl/reports/2026-08-streaming-pipeline-milestone.md` with: the four tx/s numbers and the host they came from, the accept ratio per dataset, the watermark's high-water mark, the `stale-rejects` count, and — if streaming did not beat serial — the phase timings (`RecordWarmPhaseDuration` vs `RecordAuthPhaseDuration`) that say which phase is the floor. State plainly whether the exit criterion was met.

```bash
git add docs/evm-design-impl/reports/2026-08-streaming-pipeline-milestone.md \
        gateway/app/stream_readers.go gateway/app/app.go \
        integration/test_helpers.go integration/perf/replay_json_dataset_test.go
git commit -m "test(perf): wire the streaming pipeline into the replay harness and record the milestone measurement"
```

---

### Task 27: Remove the reopen/inherit machinery the old pipeline needed

**Files:**
- Modify: `endorser/execution/batch_executor.go` (delete `AuthMergedBatch`; drop `WarmedBatch.authReader`/`authSrc`/`authCounted` and `authBatch`'s `wb.authSrc` branch)
- Modify: `endorser/execution/statedb.go:73-113` (delete `ReopenableReadStore`, `SelectiveReopenable`)
- Modify: `endorser/query/view.go` (delete `Reopen`, `ReopenInvalidating`, the `EVM_PIPE_NO_COLD_INHERIT` experiment, the `namespace` field if it becomes unused)
- Modify: `endorser/storage/lightkvs.go:256-280` (delete `Reader.Reopen` and its interface assertion)
- Modify: `gateway/core/cached_view.go` (delete `capturing`, `capturedMu`, `captured`, `inherited`, `capture`, `Reopen`)
- Modify: `gateway/core/versioned_cache.go` (delete `capturePipeline`, `invalidateDepth`, `evictHoldDepth`, `SetInvalidateDepth`, `SetEvictHoldDepth`, `RecentlyCommittedKeys`, `recentCommittedKeys`, and `DrainEvictionsDeferred` if it has no serial caller)
- Modify: `endorser/api/service.go`, `endorser/core/endorser.go`, `gateway/core/endorse.go` (delete `WarmBatch`/`AuthBatch`/`WarmedBatch` at all three layers)
- Test: delete the tests that exercised only the removed paths; keep every serial one

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing. This is deletion only.

All of it existed to make the *previous* pipelined auth pass read a fresh view while inheriting warm's cold reads — the clone-versus-reopen half of the trilemma. The streaming pipeline never reopens anything: the warmup cache is committed-only and refreshed from notifications, and the auth cache is a separate index with its own dedicated reader. Leaving the machinery in place would leave two competing explanations of how auth reads resolve.

Do this **after** Task 26, not before: the deletion touches the endorser read path that the serial baseline also runs through, and it should not be able to perturb the measurement it is being compared against.

- [ ] **Step 1: Establish the green baseline to preserve**

Run: `go build ./... && go test ./... -race 2>&1 | tee /tmp/pre-deletion.log`
Expected: PASS. Record the test count; it is the thing that must still hold afterwards.

- [ ] **Step 2: Delete, one file at a time, building after each**

After each file: `go build ./...`. The compiler is the guide — every deletion that breaks a reference means either another caller exists (investigate before deleting) or a test needs removing. Work in the dependency order above (leaves first: `lightkvs.go`, `view.go`, `statedb.go`, then `batch_executor.go`, then the three service layers, then the gateway caches).

For each symbol, confirm it is genuinely unreferenced before deleting:

```bash
grep -rn "AuthMergedBatch\|ReopenInvalidating\|SelectiveReopenable\|ReopenableReadStore\|capturePipeline\|SetInvalidateDepth\|SetEvictHoldDepth\|RecentlyCommittedKeys\|DrainEvictionsDeferred\|EVM_PIPE_NO_COLD_INHERIT\|EVM_PIPE_INVALIDATE_DEPTH\|EVM_PIPE_EVICT_HOLD_DEPTH" --include="*.go" .
```

A hit in a non-test file that is not itself being deleted means stop and re-read that caller.

- [ ] **Step 3: Delete the tests that only covered removed paths**

`endorser/query/view_test.go`'s reopen tests, `gateway/core`'s `TestCachedView_InflightWriteShadowsInheritedUnderRead`, and `endorser/core/endorser_test.go`'s `TestWarmThenAuthBatchMatchesExecuteBatch` / `TestWarmBatchErrorAndAuthBatchEngineFailure` cover only the removed API. Delete them. Keep every test that exercises `ExecuteBatch`, `ExecuteMergedBatch`, or the serial executor: those are the serial path's guarantees.

- [ ] **Step 4: Verify nothing else moved**

Run: `go build ./... && go test ./... -race`
Expected: PASS, with only the deliberately-deleted tests missing versus `/tmp/pre-deletion.log`.

Then re-run the serial baseline on one dataset and confirm it still matches Task 26's number:

```bash
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v \
  -dataset synthetic -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/serial-synthetic-postdelete.log
```

Expected: the same EVM tx/s as `/tmp/serial-synthetic.log`, within run-to-run noise. A regression means the deletion touched the serial read path — revert and find it.

- [ ] **Step 5: Commit**

```bash
git add endorser/execution/batch_executor.go endorser/execution/statedb.go endorser/query/view.go \
        endorser/query/view_test.go endorser/storage/lightkvs.go endorser/api/service.go \
        endorser/core/endorser.go endorser/core/endorser_test.go \
        gateway/core/cached_view.go gateway/core/versioned_cache.go gateway/core/endorse.go
git commit -m "refactor: remove the reopen/inherit machinery the retired pipelined executor needed"
```

---

### Task 28: Delete the serial executor

**Files:**
- Modify: `gateway/core/executor.go` (delete `runExecutor`, `executeCycle`, `submitBatch`, `resolveInflight`, `cascadeFrom`, `rebuildCacheFromInflight`, `trackInflight`, `inflightBatch`, `batchResult`, `backoff`)
- Delete: `gateway/core/pending.go`, `gateway/core/versioned_cache.go`, `gateway/core/readonly_cache.go`, `gateway/core/cached_view.go`, and their tests
- Modify: `gateway/core/api.go` (drop `pending`, `arrivals`, `cache`, `inflightSlots`, the in-flight registry, `maxBatchSize`, `SetMaxBatchSize`, `SetMaxInflight`, `queryCommitStatus`, `SetCommittedVersionReader`, `SetNotifier`, `MaxInflightObserved`, `Watched`; `Start` unconditionally starts the pipeline)
- Modify: `gateway/core/endorse.go` (delete `ExecuteBatch`, `finishBatch`, `classifyBatchOutcomes`, `decodeMergedRWS`, `decodeNsRWS` if unreferenced)
- Modify: `endorser/execution/batch_executor.go`, `executor.go` (delete `ExecuteBatch`'s len>1 two-pass path, `WarmedBatch`, `WarmBatch`, `authBatch`, `ExecuteMergedBatch`, `mergeOutcomes`, `MergeResults`, `overlayReader` — whatever the streaming path does not reach)
- Modify: `gateway/app/wiring.go`, `gateway/app/app.go`, `gateway/config/config.go` (drop `Pipelined` — there is only one executor now — and the serial-only knobs)
- Modify: `integration/*` (drop serial-only tests; retarget the rest at the pipeline)

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing. This is deletion only.

Two executors is one too many. Task 26 establishes that the streaming pipeline is faster; from that point the serial path is dead weight that every future change has to keep compiling and green. Deleting it also retires the last consumer of the speculative `VersionedCache`, which is the thing that made the previous pipelined attempt unsolvable.

**Gate: do not start this task until Task 26's measurement is recorded and shows streaming ahead on both datasets.** If it does not, the serial executor is still the production path and this task is wrong.

- [ ] **Step 1: Capture the baseline that must survive**

Run: `go build ./... && go test ./... -race 2>&1 | tee /tmp/pre-serial-removal.log`
Expected: PASS. Note the test count, and list which tests are serial-only (they exercise `PendingPool`, `VersionedCache`, `cascadeFrom`, or `executeCycle`) — those are the ones legitimately disappearing.

- [ ] **Step 2: Delete the gateway serial path, compiler-first**

Delete `gateway/core/pending.go`, `versioned_cache.go`, `readonly_cache.go`, `cached_view.go` and their `_test.go` files, then `go build ./...` and follow the errors into `api.go` and `executor.go`. Every reference is either a serial-only field/method (delete it) or something the pipeline also uses (keep it — `committerTxID`, `OrderGate`, `notifier.go`, `batch_submitter.go`, `metrics_hooks.go`).

Confirm nothing outside the deleted set still wants them:

```bash
grep -rn "PendingPool\|VersionedCache\|ReadOnlyCache\|cachedView\|NewCachedSnapshotter\|executeCycle\|cascadeFrom\|resolveInflight\|SetMaxBatchSize\|SetMaxInflight" --include="*.go" .
```

- [ ] **Step 3: Delete the endorser's two-pass batch path**

`ExecuteBatch`'s `len(txs) > 1` branch, `WarmedBatch`, `WarmBatch`, `authBatch`, `ExecuteMergedBatch`, `mergeOutcomes`, `MergeResults` and `overlayReader` exist for the serial merged batch. The streaming pipeline reaches none of them: it calls `WarmBatchOn`, `AuthTxOn` and `EndorseMerged`. Keep `Execute`/`ExecuteBatch`'s `len(txs) == 1` path if the RPC layer still needs single-tx execution (`eth_sendRawTransaction` on the non-batch path, `Call`, the read-only accessors) — check with:

```bash
grep -rn "ExecuteBatch\|ExecuteMergedBatch\|ExecuteTransaction" --include="*.go" . | grep -v _test
```

- [ ] **Step 4: Verify**

Run: `go build ./... && go test ./... -race`
Expected: PASS, with only the serial-only tests from Step 1 missing.

Then re-run the streaming replay on both datasets and confirm the throughput is unchanged from Task 26:

```bash
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v \
  -dataset synthetic -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/stream-synthetic-postremoval.log
go test ./integration/perf/ -run TestReplayJSONDatasetPerformance -timeout 4h -v \
  -dataset historic  -orderers 1 -submitters 10 -max-batch-size 128 2>&1 | tee /tmp/stream-historic-postremoval.log
```

Expected: the same EVM tx/s as Task 26's streaming runs, within run-to-run noise. (No `-pipeline` flag: there is only one executor now.)

- [ ] **Step 5: Commit**

```bash
git add gateway/core/ gateway/app/ gateway/config/config.go endorser/execution/ integration/
git commit -m "refactor: delete the serial executor now that the streaming pipeline is validated"
```

Update [docs/evm-design-impl/README.md](../README.md) in the same commit: the serial default is gone, and the streaming pipeline's measured numbers replace it as the headline.

---

## Deferred to the phase-2 plan

Recorded here so the next plan starts from a list rather than a re-read:

1. **Rollback, stage 1 (stop-the-world)** — optimization.md §158–177, all six steps, plus the open question of a tx that keeps aborting. Note the design's own caveat: it does not preserve input order, which is fine under CFT and is one of the places BFT needs revisiting.
2. **Adaptive submit batch sizing** — optimization.md §124–126: grow the batch when the submit queue passes the high threshold, shrink it below the low one. This decouples the submit batch from the auth batch, which in turn means the merged endorsement is signed at submit time over a set spanning several auth batches — so `AuthCache.NextAggregate` moves from the auth boundary to the submit boundary, and the cache's single-goroutine ownership has to be reconsidered with it.
3. **Multi-endorser warm/auth (N > 1)** — reconciling divergent read-write sets before the accept-as-is decision.
4. **Parameter sweep** — warm and auth batch size and timeout, `MaxInflightTxs`, and the two cache budgets. The design says twice that these want a sweep; Phase 9 measures one point in that space.
5. **The in-flight limit as an adaptive control** — optimization.md §17–18: start very large, reduce when resources are exhausted. This milestone uses a fixed limit.
6. **Dropping read values at submit time.** `StreamTx` keeps its read set — values included — until the tx commits, because that is what makes prefill and rollback possible without a cache lookup. Only the auth phase needs the read *values*, so the submitter could release them and keep just the versions. Worth measuring against the memory it costs before doing it.

## Self-review

Run through this before handing the plan off.

**1. Spec coverage** — every section of `evm-design/optimization.md` maps to a task:

| Spec | Task(s) |
|---|---|
| Warmup Config (batch size/timeout, queue size, max in-flight, cache bytes) | 1 |
| Warmup Cache (sync.Map, MFU, committed-only, immutable records) | 8 |
| Warmup Cache: query-service fill never lowers a version | 8 |
| Warmup TXs Worker (batching, atomic batch-index, in-flight counter) | 15 |
| Warmup Notification Worker (resident-only refresh, then sample and stamp) | 9, 16 |
| Warmup Batch Worker (parallel, warm cache, nil-view pool, full RWS, decrement) | 2, 14, 15 |
| Auth Config (batch size/timeout, queue size, read cache bytes) | 1 |
| Auth Cache (no sync, MFU, shared immutable data, read\|write state, read half limited) | 10, 11, 12 |
| Auth Worker: nested select, notification queue first | 18 |
| Auth Worker: accept-as-is when no read key is in write state | 19 |
| Auth Worker: re-execute via the auth cache and a dedicated connection | 3, 17, 19 |
| Auth Worker: add missing read keys before re-executing | 19 |
| Auth Worker: batch into the submit queue | 18 |
| Auth Cache: one version step per aggregate, stamped onto the tx | 11 |
| Auth Worker: notification-map, previous-batch-number, done-batch set | 13, 20 |
| Auth Worker: demote on version match, keep on mismatch | 12 |
| Auth Worker: evict only at the end of a batch | 20 |
| Submitter Config (queue size) | 1 |
| Submit Worker: aggregate into one committer tx; store txs; register before submit; wait for ORDERER delivery | 21, 22 |
| Submitter Notification Worker: validate, collect keys/versions/values, feed the WARMUP queue, remove from the map | 22 |
| Notification Timeout: `GetTransactionStatus` over a list; COMMITTED / ABORTED / STATUS_UNSPECIFIED | 7, 23 |
| Submitter: aggregate txs' read-write sets into one committer tx | 21 |
| Submitter: adaptive batch size (high/low thresholds) | **deferred** (stated in Scope) |
| Rollback §158–177 | **deferred** (stated in Scope) |

**2. Placeholder scan** — no task contains "TBD", "implement later", "similar to Task N", or a test step without test code. Every code step has a compilable body. Four steps deliberately point at an existing local convention instead of inventing one, each with a grep target; verify these are still the only four:

- Task 25: the `gateway/core` test gateway constructor (`grep 'New(ec' gateway/core/*_test.go`).
- Task 26: the replay harness constructor in `integration/perf/replay_json_dataset_test.go`.
- Task 25: the `App` shutdown-hook slice in `gateway/app/app.go` (add one if absent).
- Task 26: the `Replay complete: ... EVM tx/s` reporting site to extend.

**3. Type consistency** — check these names resolve identically everywhere they appear:

- `StreamKV{Key, Version, Value, Absent, IsDelete}` — Tasks 1, 8, 9, 11, 12, 13, 15, 19, 21, 22.
- `StreamTx{Tx, Status, Event, Reads, Writes}` — created in Task 15 (`newStreamTx`), mutated in 11 (`ApplyWrites` stamps write versions) and 19 (`setFrom` on re-execution), consumed in 21 (`aggregate`) and 22.
- `execution.ExecutedTx{Result, Reads}` — Tasks 2, 3, 4, 5, 15, 18, 19, 24.
- `CommitNotification{Committed, Batch}` — Tasks 1, 16, 20, 22.
- `WarmedBatchResult{Batch, Txs}` — Tasks 1, 15, 18.
- `submitBatch{txs}` — Tasks 1, 18, 21, 22.
- `WarmCache`: `Get`/`Fill`/`Refresh`/`Sweep`/`Len`/`Bytes`/`MaxBytes` — Tasks 8, 9, 14, 15, 19.
- `AuthCache`: `Get`/`Has`/`PutRead`/`IsWrite`/`Stale`/`ApplyWrites`/`NextAggregate`/`Demote`/`Evict`/`Len`/`WriteLen`/`ReadBytes`/`MaxReadBytes`/`StaleReadRejects`/`UnexpectedDemotes` — Tasks 10, 11, 12, 17, 18, 19, 20.
- `watermark`: `Hold`/`Advance`/`Prev`/`HeldLen`/`DoneLen` — Tasks 13, 18, 20.
- `WarmBatchOn` / `AuthTxOn` / `EndorseMerged` — consistent at all three layers (engine Tasks 2–3, service Task 4, client Task 5) and in the three narrow interfaces (`warmExecutor` Task 15, `authExecutor` Task 18, `submitEndorser` Task 21). The engine and service take/return `execution.ExecutedTx`; the client's `EndorseMerged` returns `(sdk.Endorsement, error)` only.
- `commitDeps` gains `OnCommitted` in Task 24 — confirm Task 22's struct listing is read together with it.
- `newStreamReaders` becomes exported `NewStreamReaders` in Task 26 — confirm Task 25's `app.go` call site is updated in the same commit.

## Execution handoff

Two ways to run this:

1. **Subagent-driven (recommended)** — a fresh subagent per task with review between tasks. The task boundaries here are drawn for it: each ends at a green test suite and a commit.
2. **Inline** — execute in one session with checkpoints, using `superpowers:executing-plans`.

Phases 0–8 are ordinary development. **Phase 9 needs the ec2 host** and a run budget: at roughly 5.4 KB per tx across nine ledger copies, a host has room for about 13M txs before disk becomes the limit, and four replay runs share that budget — so size the runs before starting, and remember duration and throughput trade off against each other.

