# EVM execution hot-path optimization — report

Repo: `fabric-x-evm`, branch `bft-redesign`. Started at HEAD `8a7439e`.

## The gate
Both benchmarks in `endorser/execution/flow_bench_test.go` must exceed **100,000 tx/s**
at `-benchtime=2s` (Apple M1 Max):

    go test -run '^$' -bench 'Benchmark(ExecuteBatch_128|Execute_1)$' -benchmem -benchtime=2s ./endorser/execution/

## Result — BOTH PASS, with large margin

| Benchmark | Baseline tx/s | Final tx/s | Gate | ns/op | B/op | allocs/op |
|---|---|---|---|---|---|---|
| `BenchmarkExecute_1`      | ~89,477  | **~461,614** (4.6x gate) | 100K | 2,166 (was 11,176) | 1,657 (was 47,893) | 34 (was 100) |
| `BenchmarkExecuteBatch_128` | ~39,093 | **~216,495** (2.2x gate) | 100K | 591,238 (was 3,274,271) | 962,315 (was 12,306,263) | 9,196 (was 25,760) |

`Execute_1` improved ~5.2x, `ExecuteBatch_128` ~5.5x. Both are comfortably and
repeatably above 100K tx/s at `-benchtime=2s` (measured across multiple runs;
single-tx runs at ~456–462K, batch at ~213–218K).

## Root cause (confirmed by profiling)
Throughput was allocation-driven GC: baseline CPU profile showed ~61% in
scheduler churn (`pthread_cond_wait`/`usleep`/`pthread_cond_signal`) driven by GC
plus `mallocgc` — all downstream of per-access and per-tx allocations. Top
`alloc_objects`: state-key hex strings (~30% incl. `hex.EncodeToString`), the
read journal (`[]any` boxing + per-read `*blocks.Version`), the EIP-2929 access
list, per-tx setup (`NewStateDB` maps, `NewExecutor` big.Ints, `vm.NewEVM`), and
big-int handling.

## Optimizations landed (one commit each, with before/after)

### 1. `12e17ce` — build state keys in a single allocation
`accKey`/`storeKey` allocated twice per state access (`common.Bytes2Hex` + string
concat) — the single largest allocator. Now the exact byte length is computed up
front, hex is encoded directly into one buffer, and `unsafe.String` hands that
buffer to the string with no copy (buffer is never mutated/aliased after). Output
is byte-for-byte identical, so the `gateway/storage/trie` parser is unaffected.
- Execute_1: 100 → 80 allocs/op, 89.5K → 93.9K tx/s
- ExecuteBatch_128: 201 → 161 allocs/tx

### 2. `6460118` — replace `[]any` journal with un-boxed typed slices
The single `journal []any` boxed every entry into an interface (one heap alloc
per read/write/effect — the #2 allocator). Split into three purpose-built value
slices:
- `reads` / `writes`: the hot path. Disjoint payloads, consumed independently
  (`getStateFromJournal` scans only writes; `Result` folds each into its own map;
  revert truncates both). The MVCC read version is stored inline, so a read no
  longer allocates a `*blocks.Version` — that pointer is now built once per unique
  key in `Result()`.
- `effects`: the rare EVM-mechanics entries (refund, SELFDESTRUCT, EIP-6780
  new-contract, EIP-1153 transient, EIP-2929 access list) that exist only to be
  undone by `RevertToSnapshot`; per-kind order (all revert needs) is preserved.

A first attempt used one fat union struct (156 B/entry) and *regressed*
throughput because slice-growth memory copying rose; the reads/writes/effects
split fixed that. Read-set/write-set semantics, first-seen read version,
last-write-wins, and snapshot/revert are byte-for-byte preserved; each revision
records `readIndex`/`writeIndex`/`effectIndex` for truncation.
- Execute_1: 80 → 71 allocs/op
- ExecuteBatch_128: 20,640 → 18,336 allocs/op (161 → 143 allocs/tx)

### 3. `ffdbbc0` — reuse StateDB+Executor+EVM across a batch's txs (the big lever)
`ExecuteBatch` built all per-tx machinery anew for every one of the 256
executions per 128-tx batch (128 warm + 128 authoritative). On the fast path
(production: no per-tx decorator, no debug logging) it now reuses ONE
StateDB+Executor per warm-pass worker and ONE for the whole authoritative pass,
resetting the StateDB in place between txs:
- `StateDB.reset` clears the journals/maps but keeps their capacity.
- `Executor` caches the signer (fixed per batch) and, via `primeEVM`, a reusable
  `vm.EVM`. Verified safe against geth v1.17.3: `stateTransition` calls
  `evm.SetTxContext` per tx, call depth returns to 0 after each top-level call,
  the jump-dest cache is code-keyed, and `Release` (arena recycle) is only
  deferred per-block in `state_processor.go`, never between txs.
- `Prepare` resets the access list instead of allocating a new one.

Each warm worker owns its own pair, so concurrency is race-free (`-race` clean).
The slow (decorator/debug) path is unchanged. `runOn`'s classification was
factored into `classify()` and is shared verbatim, so revert/ExecFailure/success
and the batch exclusion semantics are identical.
- ExecuteBatch_128: 40.3K → **216K tx/s**, 18,336 → 9,196 allocs/op,
  12.24MB → 0.96MB/op

### 4. `a5f9ed4` — pool reusable StateDB+Executor for the single-tx path
`Execute` (and `ExecuteBatch`'s N==1 case) still rebuilt everything per call.
Recycle a `reusableExec` (StateDB + Executor + primed EVM) through a
`sync.Pool` on the `EVMEngine`: each call resets the pooled StateDB in place
against its fresh snapshot, runs, returns the unit to the pool. `sync.Pool` keeps
this safe under concurrent `Execute` calls (each caller gets its own instance).
The returned read/write-set is unaffected by later reuse (`Result()` copies
keys/versions into its own maps; written value bytes are independent allocs).
Slow path unchanged; pooled run uses the same `classify()`, so Execute-vs-batch
N==1 exclusion semantics are identical.
- Execute_1: 88K → **~457K tx/s**, 69 → 34 allocs/op, 47,590 → 1,657 B/op

## Correctness verification
- `go test ./endorser/... ./gateway/core/ -count=1` — all green.
- `go test -race -count=1 ./endorser/execution/ ./gateway/core/` — green.
- `go test -race -count=1 ./integration/ -run 'TestBatchMergedCommit|TestLocalX'` — green.
- `go test ./integration/ -count=1` has pre-existing failures ONLY
  (`TestFablo`/`TestFabricX`: missing MSP keystore private keys; `TestSingleAdd11`:
  un-initialized `ethereum-tests` git submodule). Verified these fail identically
  on the base commit `8a7439e` via a throwaway worktree — they are environmental,
  not caused by these changes.

Preserved exactly: MVCC read-set semantics (`KVRead` + version recording in
`getStateFromStore`), the deferred read-error path (`setError`/`Error()` +
`Send`/`ApplyMessage` checks — a stale-view read still aborts, not panics), revert
(201) and `*ExecFailure` (460) classification in `classify`/`runOn`,
`MergeResults`, and the `overlayReader` / cross-batch behavior. The warm pass is
untouched in shape (still bounded, concurrent) — its per-worker state is now
reused rather than reallocated, never shared.

## Remaining cost / where the floor is
Both gates are met at 2–4.6x, so I stopped rather than chase diminishing returns.
The remaining per-tx cost (Execute_1: 34 allocs, 1.66 KB) is dominated by
irreducible geth work — `core.TransactionToMessage`, `uint256.FromBig` /
`big.NewInt` / `nat.make` in gas/value handling, `buyGas`, `newModernSigner`
per `types.MakeSigner` inside geth `Sender` caching — plus the state-key strings
and `Result` maps that MUST persist into the returned read/write-set. Further
gains would come from big-int reuse on the free-gas paths and interning keys, but
they are marginal now and carry more risk than value given the current margin.

## Commit SHAs (on `bft-redesign`, atop `8a7439e`)
- `12e17ce` perf(execution): build state keys in a single allocation
- `6460118` perf(execution): replace []any journal with un-boxed typed slices
- `ffdbbc0` perf(execution): reuse StateDB+Executor+EVM across a batch's txs
- `a5f9ed4` perf(execution): pool reusable StateDB+Executor for the single-tx path
