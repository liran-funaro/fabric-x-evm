# Design ↔ implementation gap analysis

Two-way comparison between the design in this folder and the code in this repository.

- **Part A** — design ideas and features that are **missing or incomplete in the implementation**.
- **Part B** — implementation mechanisms and decisions that are **missing from the design**.
- **Part C** — points where the two **actively contradict** each other.

Reviewed at commit `87080f7` (branch `bft-redesign`, 2026-08-02), including uncommitted working-tree
changes. Design sources: `oev-executor-design.md` (cited as `§`), `exec-summary.md`, `my-idea.md`,
`my-pitch.md`, `my-notes.md`, `author-notes.md`. Code citations are `path:line`.

---

## Part A — In the design, missing from the implementation

**Scope note.** The current deployment target is **CFT with a single gateway and a single endorser**.
That is a deliberate phase, not an oversight (`exec-summary.md`: "Phase 1 is crash-tolerant (CFT);
BFT comes later"). Several items below are therefore *consequences of the BFT phase not having
landed* rather than independent defects, and are tagged **Deferred (BFT)**: A1 (the OEV path), A2
(the fault-model axis), A5 (contention routing — there is no second mode to route to), A12
(endorsement threshold), and the practical impact of C2. They are listed for completeness of the
design↔code map, not as work to do now. The gaps that bite a single-gateway CFT deployment **today**
are A3, A6, A7, A10, A11, A14, and the contradictions C1 and C3.

### A1. The entire BFT / OEV path does not exist

§1.2.2, §2.5 (BFT), §2.9 (Executors) specify: raw transactions submitted under a
committer-ignored envelope type, a set of executor services following the ordered block stream,
deterministic re-execution in block order, M-of-N co-signing, all-orderer submission, one merged
write-set per batch, and the global ordering counter chaining batches.

**Present in code:** nothing. No committer-ignored envelope type (`common/proposal.go:11-20` defines
only `EVMTx`, `Call`, `State`, `EVMBatch`, all ordinary `ENDORSER_TRANSACTION` payloads), no
block-stream executor, no signature exchange, no counter key, no all-orderer fan-out for a batch
(one submitter connects to the orderer set; `gateway/app/wiring.go:48-68`).

Impact: the design's answer to the Byzantine hot-spot case is unbuilt. This is the single largest
gap and matches the phasing in `exec-summary.md` ("Phase 1 is CFT; BFT comes later"), so it is a
scheduling gap, not a contradiction.

### A2. R9 (configurable fault tolerance) is not an axis anywhere — Deferred (BFT)

§R9 and §2.9 (Configuration) require the deployment to declare crash-only or Byzantine, and the
ordering mechanism to follow from it.

**Present in code:** no fault-model configuration field (`gateway/config/config.go:44-68`,
`endorser/config/config.go:17-34`). The implementation is unconditionally the CFT shape.

This is the correct state for Phase 1: R9's second clause — "must not pay for Byzantine tolerance
when only crashes are assumed" — is satisfied by construction when there is only one mode. The axis
becomes necessary only when A1 lands.

### A3. CFT crash-failover standby is not implemented

§1.2.1 and §2.9 require a standby that resumes from the last committed point, relying on
determinism (R5) plus idempotency to drop a duplicate in-flight batch.

**Present in code:** all executor state is process-local and volatile — the pending pool
(`gateway/core/pending.go:22-27`), the in-flight registry (`gateway/core/api.go:74-77`), and the
speculative write cache (`gateway/core/versioned_cache.go:42-101`). A restart loses every accepted
but uncommitted transaction. Nothing persists submitted-batch identity, and see **C2**: the
committer TxID is random, so an idempotent duplicate-drop is impossible even in principle.

### A4. CFT Option 2 ("submit the independent set, then wait") is not implemented

§1.2.1 Option 2 specifies: compute an antichain of mutually non-conflicting transactions from the
chosen order, submit that set, wait until it appears committed in a received block, then release the
next layer — and **not** re-execute the dependents, only fill in observed committed versions.

**Present in code:** `gateway.ordered-submit` (`gateway/config/config.go:66`,
`gateway/core/batch_submitter.go:196-232`, `gateway/core/order_gate.go`,
`gateway/app/ordered_delivery.go`) is a *depth-1 serial* gate: submit one merged batch, wait until
its committer TxID appears in a delivered **ordered** block, then submit the next. Differences from
the design:

- No dependency graph and no antichain: batching is FIFO by arrival, not by independence.
- One batch per round instead of a whole independent layer, so it cannot exploit parallelism within
  a layer.
- It waits for the **orderer**, not the **committer** — this is `my-pitch.md`'s fallback variant, not
  §1.2.1 Option 2 (ordered ≠ valid; the gate explicitly proceeds after `wait-timeout`).
- Dependents **are** re-executed on any rollback (`gateway/core/executor.go:597-658` re-adds them to
  pending); the design's "reuse the precomputed read/write-sets, fill in observed versions" is absent.

### A5. Contention-based routing between modes does not exist — Deferred (BFT)

§1.3, §2.5, §2.8 and §2.10 require: measure the abort rate per domain, promote that traffic to
ordered execution when it crosses a threshold, demote when contention subsides.

**Present in code:** no abort-rate metric per key/account/contract, no threshold, no routing
decision. Every transaction takes exactly one path. `Gateway.CascadeCount`
(`gateway/core/api.go:362-364`) counts rollbacks globally for tests only.

Consequence: because there is no second mode to route to (A1), and because the single mode already
merges everything (**C1**), routing has nothing to decide.

### A6. Gas is not measured at all (R6, §2.7)

§R6/§2.7 require gas to be *measured* post-mortem and the per-block total reported for
observability — monitored, not enforced.

**Present in code:** gas is discarded. `Executor.ApplyMessage` receives geth's `ExecutionResult` and
uses only `ReturnData`/`Err`; `result.UsedGas` is never read
(`endorser/execution/executor.go:640-690`). `PerTxOutcome` carries only status and event
(`endorser/execution/executor.go:252-255`). Receipts report `GasUsed: 0` and
`CumulativeGasUsed: 0` (`gateway/api/models.go:69-86`); blocks report `GasUsed: 0`
(`gateway/api/models.go:254`). `docs/COMPATIBILITY.md` documents this as intentional
("gas not metered").

Only the *cap* half is implemented: `EVMConfig.MaxTxGas` truncates the per-tx gas limit
(`endorser/execution/executor.go:631-633`), and gas prices are zeroed for free gas
(`endorser/execution/executor.go:625-628`).

Gap: the design's "monitored" is unimplemented; the design's asymmetry argument (a per-tx cap is
possible, a shared-budget cap is not) is implemented in exactly the half the design says is
available.

### A7. Post-mortem state root is implemented but disabled

§2.7 requires the indexer to build the Merkle Patricia Trie and compute the root after each block
commits.

**Present in code:** the trie store exists and works (`gateway/storage/trie/store.go`), and
`Chain.Handle` wires it (`gateway/core/chain.go:78-98`). But **both production and the integration
harness construct the chain with `withTrie == false`** (`gateway/app/app.go:157`,
`integration/test_helpers.go:184`), so every block records `StateRoot = types.EmptyRootHash`
(`gateway/core/chain.go:89`). There is no config flag to turn it on — the parameter is a literal.
Only `gateway/core/chain_resilience_test.go:64` passes `true`.

Also missing: the proof/light-client API §2.7 motivates (§2.10 defers the details, but nothing
exposes a root or a proof today).

### A8. `(block, index, sub-index)` is only two-thirds implemented

§2.6 requires each EVM transaction inside a bundle to stay individually addressable by a three-level
coordinate.

**Present in code:** the sub-index is computed (`gateway/core/chain.go:236`) and carried on the
domain model (`gateway/domain/models.go:27-31`), but it is **not persisted and not served**: there is
no `sub_index` column (`gateway/storage/schema.sql`, transactions table), and neither
`toStorageTransaction` nor `toDomainTransaction` maps it (`gateway/storage/store.go:58-95`).

What *is* implemented is a flat, block-global `tx_index` that increments once per EVM transaction
across all merged batches in the block (`gateway/core/chain.go:147-152`), which is what
`eth_getTransactionByBlockNumberAndIndex` and receipts need. So Ethereum addressability is intact;
the design's explicit third coordinate is dropped on write.

### A9. Sequential-nonce bundling as a *targeted* mechanism is absent

§2.6 specifies a time-window batcher that detects several transactions **from one account with
consecutive nonces arriving close together** and bundles those.

**Present in code:** no per-account grouping and no time window. The executor drains the whole
pending pool (bounded by `max-batch-size`) in global FIFO order and merges it
(`gateway/core/executor.go:133`, `gateway/core/pending.go:76-91`). Consecutive nonces from one sender
work only as a side effect of FIFO order plus the serial authoritative pass. See **C1**: the
implementation generalized the mechanism to all traffic instead of scoping it.

### A10. Out-of-order (high-nonce) buffering is not implemented

Flagged in `author-notes.md` as lightly specified; `previous-context.md` describes buffer-and-release.

**Present in code:** the opposite, deliberately. `ValidateTx` accepts nonce gaps at ingress
(`gateway/core/validate.go:79-82` rejects only nonce-too-low), and `docs/COMPATIBILITY.md` records
"Nonce gaps are accepted". A gap-filling transaction is then repeatedly drained, excluded by the
endorser as retryable (`endorser/execution/batch_executor.go:354-368`,
`common/proposal.go:25-35`), and left pending — a re-execution cost per cycle rather than a mempool
hold. Terminal exclusions (nonce-too-low) are evicted (`gateway/core/executor.go:186-190`).

### A11. Determinism prerequisites (R5) are partly unmet

§1.2 "Determinism" requires three things. Status:

| Requirement | Status |
|---|---|
| Canonical write-set encoding (keys sorted, deduplicated, last-write-wins) | **Met.** Dedup + last-write-wins in `endorser/execution/merge.go:22-47`; the SDK builder sorts reads/writes/blind-writes by key (`fabric-x-sdk/endorsement/fabricx/builder.go:150-158`). |
| Execution rules configured on-chain and versioned | **Not met.** The chain config is built locally from a YAML chain ID (`common.BuildChainConfig`, `endorser/app/factory.go`), with all forks enabled from genesis. Nothing is read from chain state, so two endorsers on different binaries can silently diverge. |
| Transaction ID a deterministic function of batch contents | **Not met.** See **C2**. |

### A12. No endorsement threshold logic — Deferred (BFT), correct as-is under CFT

§R3 and §2.9 require the namespace endorsement policy to be satisfied by collecting enough matching
signatures — **one signer under CFT**, M-of-N under BFT.

**Present in code:** exactly the CFT case. One embedded endorser signs the merged batch and its single
signature meets the policy, which is what §1.2.1 specifies ("it signs the result alone — its one
signature meets the endorsement policy (R3)"), and the endorser's own comment says so
(`endorser/api/service.go:54-59`: "so CFT quorum needs only one signature for the whole batch").
**No gap at the current phase.**

Two forward-looking notes, for when BFT (A1) lands:

- The fan-out is **N-of-N, not M-of-N**: if more than one endorser is configured (the config allows
  it), the gateway requires *all* of them to return `StatusOK` and the first error aborts the batch
  (`gateway/core/endorse.go:300-336`, `:344-349`). There is no threshold and no partial-quorum
  assembly. Read paths use `endorsers[0]` only (`gateway/core/endorse.go:506-546`). Under CFT this is
  indistinguishable from one-signer; under BFT it would be a liveness bug (any single faulty endorser
  blocks every batch).
- Endorsers are all embedded in the gateway process from one config list
  (`gateway/app/app.go:100-123`). A cross-org remote endorser is designed
  (`docs/design/endorsement-api/`) and the proto is generated (`api/endorsementpb/`), but no client or
  server implementation exists yet — nothing in the tree imports `endorsementpb`. That transport is a
  precondition for any real quorum across trust domains.

### A13. Multi-namespace support is absent

§1.2.2 notes a batch may span several namespaces and must satisfy each policy.

**Present in code:** one namespace per deployment, from config; the engine, invocation, and RWS
decode are all single-namespace (`gateway/core/endorse.go:46-54`,
`gateway/core/endorse.go:461-476`).

### A14. Block timestamp does not come from consensus, and EVM `TIMESTAMP` is a constant

§2.7 requires the EVM block timestamp to be the Orderer's consensus-agreed timestamp.

**Present in code:** the *stored block row* takes `blocks.Block.Timestamp` from the delivered block
(`gateway/core/chain.go:142`), which is the closest available approximation. But the **EVM execution
context** uses a hardcoded `defaultBlockTime = 1_000_000` for every transaction, forever
(`endorser/execution/executor.go:435`, `:444`), so the `TIMESTAMP` opcode a contract observes is a
constant unrelated to any block. `docs/COMPATIBILITY.md:248` records this. A contract with
time-dependent logic behaves incorrectly; the design does not discuss the execution-context clock at
all (see **B18**).

### A15. The indexer is not a separate component

§2.9 lists the Indexer as its own component over the committed ledger.

**Present in code:** it is a block handler inside the gateway process (`Chain`, registered on the
gateway synchronizer, `gateway/app/app.go:181-185`), sharing the process lifecycle. Functionally
equivalent for a single-gateway deployment; the design's independent-agreement story for the state
root (§2.7) is not available while it is co-located.

---

## Part B — In the implementation, missing from the design

### B1. Cross-batch speculative execution (the biggest addition)

The design's CFT path is "execute the batch, submit it, and on conflict re-order and re-execute",
with cross-batch ordering enforced either by atomicity (Option 1) or by waiting for an observed
commit (Option 2).

The implementation does neither by default. It **submits a batch and immediately executes the next
one against the previous batch's uncommitted writes**:

- `executeCycle` returns right after submit and never awaits the commit
  (`gateway/core/executor.go:101-157`).
- `VersionedCache` holds submitted-but-uncommitted writes at *predicted* versions ("spec versions":
  base from the read-set, `+1` per subsequent writer) so the next batch reads them as if committed
  (`gateway/core/versioned_cache.go:136-214`).
- Commit outcomes resolve asynchronously; an abort/timeout **cascades**, invalidating the whole
  registry suffix from the failed batch onward and re-queuing every transaction in it
  (`gateway/core/executor.go:579-658`).
- The speculative cache is rebuilt from surviving batches when an invalidation erases a key an
  earlier survivor also wrote (`gateway/core/executor.go:660-673`,
  `gateway/core/versioned_cache.go:438-445`).
- Backpressure is an in-flight window semaphore, default 16 (`gateway/core/api.go:74-77`,
  `gateway/core/executor.go:220-227`).

This is a genuinely new ordering mechanism, and its correctness rests on a premise the design never
states: **cross-batch order is enforced by submission order alone**, which requires the orderer
submission path to be serialized to exactly one worker
(`gateway/app/wiring.go:37-42`, `gateway/core/batch_submitter.go:44-52`). The design explicitly warns
this is unsafe — §1.2.1 says "the Orderer does not guarantee that transactions commit in the order
they were submitted" — and offers atomicity or commit-gating instead. The implementation's actual
safety net is the cascade (self-correcting, at a re-execution cost) plus the opt-in ordered-submit
gate (A4).

The design needs a section for this whole mechanism: the spec-version model, suffix cascade,
in-flight window, and the exact ordering assumption it depends on.

### B2. Warm/auth pipelining across batches

§1.2 describes the two-phase (warm → authoritative) pass **within** one batch. The implementation
adds an outer pipeline that overlaps the concurrent warm pass of batch N+1 with the serial
authoritative pass of batch N, at prefetch depth 1 (`gateway/core/executor.go:253-416`,
`gateway/core/endorse.go:181-250`, `endorser/execution/batch_executor.go:152-297`).

Rationale (from the repo's own measurements): the serial authoritative pass is the CPU floor and the
warm pass is I/O; overlapping them collapses the per-batch wall from `warm+auth` to
`max(warm,auth)`.

Status: `pipelined` defaults to **false** (`gateway/config/config.go:64`). `report/pipeline_report.html`
and commit `f746fac` record that it livelocked on real hot-key traffic; the read-path fix at
`87080f7` claims depth-independence but is still being re-measured. The design has no notion of this
optimization or of its failure mode.

### B3. A layered read path with three caches

The design says reads go to the query service under one pinned view per batch. The implementation
resolves each read through four layers, in order (`gateway/core/cached_view.go:63-93`):

1. the live in-flight write cache (`VersionedCache`, B1);
2. inherited warm-pass write-cache resolutions (pipelined auth only);
3. a cross-batch **read-only MFU cache** of hot, rarely-written committed records — capacity 256,
   admission after 4 reads in one batch, evicted on any self-write
   (`gateway/core/readonly_cache.go`, `gateway/core/versioned_cache.go:178-189`);
4. the per-view query-service read cache (`endorser/query/view.go:88-131`).

The read-only cache has a stated correctness limitation the design never considers: a key written by
*another* gateway is not evicted, so a stale read loses its MVCC check and the batch aborts —
self-correcting, never a bad commit (`gateway/core/readonly_cache.go:63-71`).

### B4. Snapshot reopen with cold-read inheritance and selective invalidation

To keep pipelined auth reads as cheap as serial's, the authoritative pass **reopens** the warm
snapshot: a fresh query-service view for keys warm never fetched, while inheriting warm's already
fetched reads as cache hits (`endorser/query/view.go:163-209`,
`endorser/execution/statedb.go:72-115`, `endorser/execution/batch_executor.go:439-474`). A selective
variant drops recently-committed keys from the inherited clone
(`gateway/core/versioned_cache.go:248-277`). None of this exists in the design, and it carries a
non-obvious safety argument (the write-cache layer above shadows any key whose committed version
could have advanced) that is load-bearing.

### B5. Deferred committed-write eviction

A committed batch's writes are held in the write cache for `evictHoldDepth` extra boundaries instead
of being dropped on the commit notification, because the query service reflects a commit only after a
lag (`gateway/core/versioned_cache.go:327-380`). Documented as a read-locality optimization, not a
correctness requirement. The design has no concept of query-service commit-visibility lag.

### B6. Nil-view mode

An off-by-default mode that skips `BeginView`/`EndView` entirely and reads current committed state per
call, relying on the read and write caches for intra-pass consistency
(`endorser/query/view.go:23-65`, `endorser/app/factory.go:82-90`). This directly contradicts
`my-pitch.md`'s "make sure to use the same query service view ID for all TXs" and §1.2's single
per-batch snapshot, so if it is ever promoted from an experiment the design must say why pinning is
optional.

### B7. Per-TxID commit notification with a query-service adjudication fallback

The design assumes a batch's outcome is observed in a block. The implementation adds a
register-then-submit notification path (`gateway/core/notifier.go`) plus a fallback that decides
whether a batch committed by comparing **each written key's committed version against the spec
version the cache predicted** (`gateway/core/api.go:393-463`). It is deliberately conservative
(any error, missing key, or empty spec set ⇒ roll back) and documents its own weak spot (a
first-write-of-absent-key has spec 0, so it cannot discriminate alone; safe only under the
read-modify-write invariant). A blind 60 s timeout backstop covers the unwired path
(`gateway/core/executor.go:27-38`, `:469-494`).

### B8. Per-transaction exclusion instead of batch abort

The design's merged batch is all-or-nothing. The implementation lets the authoritative pass **exclude**
individual sub-transactions that the pre-execution gate rejects (nonce gap, bad signature,
insufficient funds) while still endorsing the rest, and classifies the exclusion:

- **terminal** (nonce-too-low) ⇒ evict from pending (`common/proposal.go:30-35`,
  `endorser/execution/batch_executor.go:488-497`, `gateway/core/executor.go:186-190`);
- **retryable** (nonce-too-high, …) ⇒ leave pending (`gateway/core/endorse.go:396-421`).

Excluded sub-txs get a sentinel outcome with an empty read/write-set, contribute nothing to the merged
set, and are skipped when receipts are built — including not advancing the flat tx index
(`gateway/core/chain.go:212-220`). This is essential to make an unconditionally merged batch (C1)
survivable, and the design says nothing about it.

### B9. Per-sub-tx outcomes carried in the committed transaction's event payload

Per-sub-tx status and logs ride in the merged `ExecutionResult.Event` as JSON
(`endorser/core/endorser.go:129-152`), because the proposal-response payload does not survive to the
committed block. Both the gateway (pre-submit, to classify included/terminal) and the indexer
(post-commit, to build receipts) decode the same envelope
(`gateway/core/endorse.go:423-447`, `gateway/core/chain.go:254-310`). The design asserts receipts are
recoverable post-mortem but never specifies the carrier.

### B10. The merged read/write-set is decoded back out of the signed response

To seed the speculative cache, the gateway re-parses the endorsers' signed `applicationpb.Tx` payload
to recover the merged RWS (`gateway/core/endorse.go:449-501`). Noted limitation: the wire format has
no delete flag, so `IsDelete` cannot be recovered — a delete is indistinguishable from a blind write
of an empty value (`gateway/core/endorse.go:478-484`).

### B11. Committer-invalid Fabric transactions map to empty EVM blocks

When a merged batch aborts, the block still gets a row (preserving chain linkage) but contributes no
EVM transactions, no receipts, and no logs, and the flat tx index does not advance
(`gateway/core/chain.go:161-177`, `gateway/storage/trie/store.go:81-85`). The retried transactions
appear in a later block with their one true receipt. The design's "EVM block = committed Fabric
block" (§2.7) does not cover the aborted case.

### B12. Batch-size bound driven by the orderer message limit

`max-batch-size` caps how many EVM txs are folded into one committer tx, because an unbounded drain
can exceed the orderer's max message size (`gateway/config/config.go:59`,
`gateway/core/pending.go:69-91`). Drain is non-destructive, so the remainder is picked up next cycle.
A real operational constraint on §1.2's "merge the batch" that the design does not mention.

### B13. Warm-pass concurrency model

The warm pass is a work-stealing pool with one worker per transaction by default, explicitly *not*
tied to `GOMAXPROCS` because warm reads block on gRPC; the point is to fill the query service's
read-coalescing window rather than expose its `max-batch-wait` on every small wave
(`endorser/execution/batch_executor.go:205-276`, `endorser/execution/executor.go:38-45`). Warm
failures are swallowed (`recover`) since the authoritative pass re-runs everything.

### B14. Query-service connection pool

Eight gRPC connections by default, round-robined per read, because one HTTP/2 transport serializes a
batch's concurrent reads. The constant carries its own measurement: 4212 tx/s at 1 connection rising
to a ~5040 tx/s plateau at 4–16, regressing at 32 (`endorser/app/factory.go:27-36`).

### B15. Execution fast path (allocation and CPU work)

A set of optimizations with no design counterpart, each documented against a profile:

- one reused `StateDB` + `Executor` per warm worker and one for the whole authoritative pass, reset
  in place between transactions (`endorser/execution/batch_executor.go:499-517`);
- a reused `vm.EVM` primed once per pass (`endorser/execution/executor.go:468-474`);
- a `sync.Pool` of executors for the single-tx paths (`endorser/execution/batch_executor.go:519-548`);
- a per-engine keccak cache for contract code hashes (`endorser/execution/executor.go:82-88`);
- single-allocation state keys with plain lowercase hex instead of EIP-55 `addr.Hex()`
  (`endorser/execution/statedb.go:469-507`);
- a journal split into un-boxed read/write/effect slices instead of one `[]any`
  (`endorser/execution/statedb.go:213-250`);
- a per-transaction read memo so a key is fetched from the view at most once
  (`endorser/execution/statedb.go:525-545`).

### B16. Overlay read-version semantics inside a batch

The design says the authoritative pass runs "against the warm cache" so a later transaction sees
earlier writes. The implementation pins down the part that matters for MVCC: the overlay returns the
in-batch **value** but always the **underlying snapshot's version**, never a synthesized intra-batch
one, because the whole merged batch is validated against the pre-batch committed versions. A write to
a key absent from the snapshot is reported with `IsDelete` forced true so the read records a nil
version (`endorser/execution/batch_executor.go:566-627`). Getting this wrong breaks view consistency;
the design should state it.

### B17. Read-error propagation as a batch abort

If a backing-store read fails mid-execution (e.g. an expired view), the EVM has run on zero-valued
reads, so both the result and the error are meaningless: the implementation reverts to the snapshot
and surfaces the read error rather than a `TxRejected`/`ExecFailure`, so the caller aborts the batch
and retries on a fresh view instead of spuriously excluding a valid transaction
(`endorser/execution/executor.go:597-615`, `:659-667`, `endorser/execution/executor.go:210-218`).
The design's R5 says nothing about read-failure semantics, yet this distinction is what keeps
exclusion decisions honest.

### B18. Stubbed EVM block context

Beyond the constant timestamp (A14): `NUMBER` is 0 during transaction execution, `GetHash` returns
zero, `PREVRANDAO` is a zero stub, `COINBASE` is the zero address, `BASEFEE` is 0, and the EVM block
gas limit is 300 000 000 (`endorser/execution/executor.go:435-456`). Defaults: 5 000 000 gas per call
if unspecified. The design treats block metadata as post-mortem (R8) but never says what the EVM
observes *during* execution — which is a compatibility surface in its own right
(`docs/COMPATIBILITY.md:247-252`).

### B19. Monotonic-version mode

Under `fabric-x` the read-set version is a single monotonic counter; under `fabric` it is
`(blockNum, txNum)` (`endorser/app/factory.go:100-104`, `endorser/execution/statedb.go:563-572`). The
design describes only the "each write bumps version by one" model.

### B20. Genesis-block synthesis

An empty block 0 is inserted when the store has none, so `eth_getBlockByNumber("latest")` is never
null before the first commit (`gateway/core/chain.go:100-124`).

### B21. Ingress validation delegated to geth's txpool

`SendTransaction` runs geth's `txpool.ValidateTransaction` plus one stateful nonce-too-low check,
deliberately skipping the balance check (gas is not metered) and rejecting unprotected (pre-EIP-155)
transactions (`gateway/core/validate.go:36-83`). Deviations are catalogued in
`docs/COMPATIBILITY.md` — notably: nonce gaps accepted (A10), replacement transactions unsupported,
blob transactions rejected.

### B22. Diagnostics and experiment scaffolding

A dedicated abort-classification package (`common/pipediag/`, default off via `EVM_PIPE_DIAG`) records
per-key cold fetches and committed versions to attribute a stale read to the query service, a stale
view clone, or the write cache (`gateway/core/executor.go:560-569`, `:622-639`,
`endorser/query/view.go:122-130`). Alongside it, five environment-variable experiment knobs are
currently in production source paths and explicitly marked "remove before final fix":
`EVM_PIPE_NO_DEFER` (`gateway/core/executor.go:339-349`), `EVM_PIPE_NO_WRITE_INHERIT`,
`EVM_PIPE_INVALIDATE_DEPTH`, `EVM_PIPE_EVICT_HOLD_DEPTH` (`gateway/core/api.go:276-315`),
`EVM_PIPE_NO_COLD_INHERIT` (`endorser/query/view.go:191-201`), plus `EVM_QS_NIL_VIEW`
(`endorser/app/factory.go:88`). Also always-on observability the design does not ask for:
commit-latency stats, peak in-flight watermark, cascade count (`gateway/core/api.go:88-97`,
`:356-391`).

### B23. Test/replay surface

Not design scope, but substantial and worth knowing exists: a state decorator hook for replaying
historical traces on a fresh chain (`endorser/execution/executor.go:66-73`), a dual (Fabric + eth
trie) StateDB for conformance work (`endorser/execution/dual_statedb.go`), hardhat/snapshot-revert
test RPC (`gateway/testimpl/`), the `ethereum/tests` and `execution-specs` corpora, and a
baseline-diffing tool (`cmd/baseline/`).

---

## Part C — Direct contradictions

### C1. The merged batch is the universal default, not a contended fallback

The design is explicit and repeated: EOV is the default (§2.4 — one transaction, endorse, submit,
retry); the merged batch belongs to the ordered/contended path (§1.2.1 Option 1, §2.5); §2.6's bundle
is scoped to one account's consecutive nonces; and R2 requires independent work to commit **in
parallel**.

The implementation merges **every** drained transaction — all senders, all contracts — into a single
Fabric transaction, unconditionally, on both the serial and pipelined loops
(`gateway/core/executor.go:133-152`, `gateway/core/endorse.go:152-171`). The per-transaction EOV path
still exists (`ProposalTypeEVMTx`, `EndorsementClient.ExecuteTransaction`) but **no production caller
uses it** — only integration tests do.

Consequences the design does not account for:

- **The committer's per-key parallelism is given up for this traffic.** A merged batch is one Fabric
  transaction, so the whole batch is one dependency unit: two transactions touching disjoint keys can
  no longer commit in parallel, and either both commit or neither does. That is the opposite of R2's
  intent for the optimistic bulk. What is bought is round-trip amortization (one commit per N txs)
  and free intra-batch ordering.
- **Blast radius.** One MVCC conflict on one key re-runs the whole batch (up to `max-batch-size`
  transactions), and the cascade (B1) can re-run every later in-flight batch too.
- **A per-gateway serial pipeline.** One executor goroutine, one submitter, one batch at a time in
  the authoritative pass. Horizontal scale requires multiple gateways, which the design does not
  discuss.

This is arguably the most consequential divergence: the design describes a two-mode system whose
default is parallel and optimistic; the code implements a single-mode system whose default is a
serialized merged pipeline.

### C2. The committer TxID is random, not a deterministic function of the batch

§1.2's determinism paragraph requires "the transaction ID is a deterministic function of the batch
contents (not of any submitter identity or wall-clock)", and §1.2.1/§1.2.2 both rely on idempotency to
drop duplicates — a CFT standby reproducing an in-flight batch, and BFT executors submitting the same
batch to all orderers.

`endorsement.NewInvocation` derives the TxID from a **24-byte cryptographically random nonce plus the
creator identity** (`fabric-x-sdk/endorsement/proposal.go:99-106`). It depends on both the submitter
identity and randomness, and is different on every call for identical batch contents.

Consequences: the CFT standby cannot dedup (A3); BFT all-orderer fan-out would produce N distinct
transactions rather than one idempotent submission (A1); and the gateway's own recovery comment
already concedes the workaround — a blindly rolled-back batch that had in fact committed is
"self-correcting" only because its transactions come back nonce-too-low
(`gateway/core/executor.go:27-38`).

**Practical impact today: none.** With a single gateway, no standby, and no BFT executor set, nothing
currently submits the same batch twice, so there is nothing to dedup. This is a *latent* blocker — it
must be fixed before A3 or A1 can be built, not because anything is broken now.

### C3. State layout: split account fields, code keyed by address

§2.3 specifies one account record `0x01‖address → {nonce, balance, codeHash}`, storage at
`0x02‖address‖slot`, and **content-addressed code** at `0x03‖codeHash`, and derives same-sender
ordering from "every transaction from an account reads and writes that account's record".

The implementation uses ASCII keys with hex components and splits the account
(`endorser/execution/statedb.go:485-507`, mirrored by the trie parser
`gateway/storage/trie/store.go:123-187`):

| Design | Implementation |
|---|---|
| `0x01‖address → {nonce, balance, codeHash}` | `acc:<hexaddr>:bal`, `acc:<hexaddr>:nonce`, `acc:<hexaddr>:code` |
| `0x02‖address‖slot` | `str:<hexaddr>:<hexslot>` |
| `0x03‖codeHash → bytecode` | (none — code is stored **by address**, not content-addressed) |

Consequences:

- Same-sender ordering still holds, but for a different reason than the design states: it comes from
  the shared `:nonce` key, not from a single account record. Two transactions that touch only an
  account's *balance* no longer conflict — finer-grained than the design intends, and R7's guarantee
  now rests on the nonce key alone.
- No code deduplication: N instances of the same contract store N copies of the bytecode, and there
  is no `codeHash` indirection.
- Keys are 2× larger than the design's binary layout (hex, plus ASCII prefixes and separators).

### C4. R2's "no single shared key" is honored; "keep the common path lock-free" is not

No global counter exists (correct for CFT — §1.2.1 and `author-notes.md` both say CFT needs none).
But every EVM transaction in a deployment funnels through one executor goroutine, one merged Fabric
transaction per cycle, and one orderer submitter (C1, B1). The bottleneck moved from a shared *key* to
a shared *component*. R2's letter is satisfied; its intent — "independent work commits in parallel" —
is not, on the default path.

---

## Summary

Severity is relative to the **current phase: CFT, single gateway, single endorser**.

| # | Design → implementation gap | Severity |
|---|---|---|
| A3 | No CFT standby / no crash recovery of accepted txs | **High** |
| A6 | Gas not measured (R6 unimplemented) | Medium |
| A7 | Post-mortem state root implemented but hard-disabled | Medium |
| A10 | No high-nonce buffering; gaps re-executed each cycle | Medium |
| A11 | Execution rules not on-chain/versioned | Medium |
| A14 | Block time not from consensus; EVM `TIMESTAMP` constant | Medium |
| A4 | CFT Option 2 replaced by a depth-1 orderer-wait gate | Medium |
| A8 | Sub-index computed but not persisted/served | Low |
| A9 | Nonce bundling not per-account/time-windowed | Low (superseded by C1) |
| A13 | Single namespace only | Low |
| A15 | Indexer co-located, not independent | Low |
| A1 | BFT/OEV path entirely absent | Deferred (BFT) |
| A2 | R9 fault model not configurable | Deferred (BFT) |
| A5 | No contention measurement or mode routing | Deferred (BFT) |
| A12 | No endorsement threshold (one signer = correct under CFT) | Deferred (BFT) |

| # | Implementation → design gap | Why it matters |
|---|---|---|
| B1 | Cross-batch speculative execution + suffix cascade | New ordering mechanism; unstated safety premise |
| B2 | Warm/auth pipelining across batches | Off by default; had a livelock failure mode |
| B3 | Three-layer read path incl. read-only MFU cache | New staleness surface |
| B4 | Snapshot reopen with read inheritance | Load-bearing safety argument |
| B5 | Deferred committed-write eviction | Depends on query-service visibility lag |
| B6 | Nil-view mode | Contradicts the pinned-view premise |
| B7 | Per-TxID notification + version-based adjudication | New commit-decision mechanism |
| B8 | Per-sub-tx exclusion, terminal vs retryable | Makes universal merging viable |
| B9 | Per-sub-tx outcomes in the tx event payload | Receipt carrier the design leaves open |
| B10 | Merged RWS decoded from the signed response | `IsDelete` unrecoverable on the wire |
| B11 | Aborted Fabric tx ⇒ empty EVM block | Uncovered case in §2.7 |
| B12 | `max-batch-size` from the orderer message limit | Operational bound on merging |
| B13 | Warm concurrency = batch size, I/O-bound rationale | Tuning contract |
| B14 | Query-service connection pool (8) | Measured throughput plateau |
| B15 | Execution fast path (reuse/pooling/key encoding) | Profile-driven |
| B16 | Overlay returns snapshot versions, not synthetic ones | MVCC correctness detail |
| B17 | Read failure aborts the batch, never excludes a tx | Keeps exclusion honest |
| B18 | Stubbed EVM block context | Compatibility surface |
| B19 | Monotonic-version mode | Two version models |
| B20 | Genesis-block synthesis | API smoothing |
| B21 | geth-txpool ingress validation + documented deviations | Failure model |
| B22 | pipediag + six env-var experiment knobs in prod paths | Should be removed/promoted |
| B23 | Replay/conformance/test-RPC surface | Non-design scope |

| # | Contradiction | Direction |
|---|---|---|
| C1 | Merged batch is universal, not a contended fallback | Implementation broadened the design |
| C2 | Random committer TxID vs deterministic-by-content | Violates R5; latent (no impact until A3/A1) |
| C3 | Split account keys, code by address, ASCII keys | Implementation differs from §2.3 |
| C4 | Serialization moved from a shared key to a shared component | R2 intent unmet on the default path |

## Suggested follow-ups

Design-side (no code change):

1. Add a section for cross-batch speculative execution (B1) — spec versions, cascade, in-flight
   window — and state its ordering premise explicitly, contrasted with §1.2.1's two options.
2. Rewrite §2.3 to the implemented key layout, or record the current layout as a deliberate
   deviation with the consequences in C3.
3. Reframe §2.4/§2.6 to match C1: merging is the default; §2.6's per-account bundle is the special
   case it subsumes.
4. Add the read-path layering, reopen semantics, and staleness arguments (B3, B4, B5, B16) — these
   are correctness arguments, not tuning notes.
5. Specify what the EVM observes during execution (B18) as a first-class compatibility surface.

Code-side, scoped to what a CFT single-gateway deployment actually needs:

1. Implement gas measurement (A6): read `result.UsedGas`, carry it in `PerTxOutcome`, persist per-tx
   and per-block totals. This is the only outright unimplemented requirement (R6) at this phase.
2. Add a config flag for the state-root trie and enable it (A7); persist `sub_index` (A8).
3. Remove or promote the six `EVM_*` experiment knobs and the bisect branches in production paths (B22).
4. Decide the crash story (A3): either persist the pending pool and in-flight registry, or document
   that accepted-but-uncommitted transactions are lost on restart and that clients must resubmit.
   Today it is neither built nor documented.
5. Add abort-rate instrumentation per key/account — cheap, useful for tuning `max-batch-size` and the
   in-flight window now, and the prerequisite for ever deciding whether A1/A5 are needed at all.

Deferred until BFT is on the table: a deterministic committer TxID (C2 — a hard prerequisite for both
A3's standby dedup and A1's all-orderer fan-out), the fault-model axis (A2), threshold endorsement and
the remote-endorser transport (A12).
