# Comparison: this design vs. Fabric-EVM `ARCHITECTURE.md`

Compares the design in [`oev-executor-design.md`](oev-executor-design.md) (referred to below as **ours**,
cited by `§`) with the Fabric-EVM `docs/ARCHITECTURE.md` (referred to as **Fabric-EVM**, cited by its
section titles). Both put an EVM on top of Fabric / Fabric-X.

## TL;DR

- **Same goal, same substrate.** Both embed an EVM behind standard Ethereum JSON-RPC, map EVM state to
  flat Fabric keys, reuse Fabric MVCC for consistency, treat gas as metering (no fees), and rely on
  Fabric ordering (CFT/BFT) instead of PoW/PoS.
- **Strong convergence on the crash-tolerant / Phase-1 path.** Fabric-EVM's *dependency manager*
  (endorse and submit a transaction only once the ones it depends on have committed) is, in effect,
  ours' **CFT** path (§1.2.1 / §2.5): a trusted node schedules by dependency and gates submission on
  observed commits.
- **Divergence on the hard cases.** Ours adds a genuine **order-first (OEV)** fallback with a **global
  ordering counter** for the *trustless* (BFT) hot-spot case; Fabric-EVM stays execute-first (EOV)
  throughout and leaves BFT (its Phase 4) *to be determined*. Ours also **bundles sequential nonces
  into one atomic transaction** and treats **blocks / receipts / state-root as post-mortem**, both of
  which Fabric-EVM handles differently or not yet.
- **Different document purpose.** Fabric-EVM is an *implementation architecture* (concrete components,
  go-ethereum, SnapshotDB, SQLite, proposal types, a 4-phase roadmap). Ours is a *design & correctness*
  argument (requirements, serializability proofs, an explicit fault-model axis).

## Similarities

- **Ethereum compatibility surface.** Standard JSON-RPC, unmodified client tooling (MetaMask, Web3.js,
  Hardhat), deterministic finality. (Fabric-EVM commits to unmodified `go-ethereum`; ours keeps the
  EVM engine abstract but requires deterministic, on-chain-versioned execution rules — §1.2 R5.)
- **MVCC as the consistency mechanism.** Both capture a read/write-set with per-key versions during
  execution and let Fabric's commit-time MVCC check catch stale reads. Independent state commits in
  parallel; overlapping state conflicts (§"Fabric-X mechanics", ours; "MVCC Validation", Fabric-EVM).
- **Flat state mapping with the nonce in state.** Both map accounts, storage slots, and code to flat
  keys and keep the account nonce *in the value*, not in the MVCC version. Same-sender serialization
  therefore falls out of the shared account key(s) (§2.3; "SnapshotDB").
- **Gas is metering, not payment.** Permissioned users are known and trusted, so gas bounds work but
  charges nothing (§2.7 R6; "Gas as Metering").
- **Dependency-aware scheduling to protect goodput.** Both avoid blindly retrying conflicts. Fabric-EVM
  gates endorsement/submission on committed dependencies; ours' CFT submits an independent set and waits
  for its block before releasing dependents (§2.5 CFT Option 2). Same core idea.
- **Special handling for bursts of sequential nonces.** Both single out a wallet sending `n, n+1, n+2`
  as a case that must not thrash (§2.6; Phase 1 "handle client bursts / same-client sequencing").
- **Parallel/speculative execution for disjoint work.** Fabric-EVM simulates in parallel pre-order;
  ours warms a per-batch cache in parallel, then runs authoritatively (§1.2 two-phase).
- **A separate query/index path.** Fabric-EVM's gateway synchronizer indexes committed blocks into
  SQLite; ours' indexer reconstructs Ethereum blocks/receipts/logs from the committed ledger (§2.7).
- **Determinism is required for endorsement.** Both require all endorsers to produce identical
  read/write-sets ("deterministic execution"; ours R5, made byte-identical and canonical).

## Divergences

### 1. The contention mechanism (the central one)

- **Fabric-EVM stays EOV end-to-end.** Its answer to contention is to *mask* MVCC conflicts: an
  in-memory mempool, a retry loop, and a dependency manager that only submits a transaction once its
  dependencies have committed. It never orders raw transactions before executing them. Its stated tenet
  is "must not break Ethereum clients… mask MVCC conflicts through retry and scheduling."
- **Ours adds a true OEV (order-first) fallback.** For a genuine hot spot, ours can route raw
  transactions through the Orderer *first* (as a committer-ignored envelope), then re-execute them in
  the fixed block order and emit one merged write-set per batch (§1.2.2 / §2.5 BFT). This is a mechanism
  Fabric-EVM does not have.
- **Consequence:** for the hardest hot spot, Fabric-EVM serializes submissions (submit, await commit,
  submit next); ours can instead *batch* the hot transactions per block into a single merged
  transaction — fewer commit round-trips — and, crucially, do so *without a trusted sequencer*.

### 2. The trust model: an execution axis vs. a deployment phase

- **Ours makes CFT vs. BFT a first-class fault-model axis (R9)** that changes the ordering mechanism:
  under **CFT a single trusted executor owns the order** and may **re-order on retry**; under **BFT no
  one is trusted, the Orderer fixes the order, and executors are bound to reproduce it** (so they cannot
  re-order, which is exactly why the global ordering counter is needed).
- **Fabric-EVM treats CFT/BFT as decentralization phases** (Phase 3 CFT multi-org, Phase 4 BFT), not as
  different execution mechanisms. Its execution model (EOV + dependency scheduling) is intended to stay
  the same across phases; more nodes/orgs change *who coordinates*, not *how ordering is enforced*.

### 3. The cross-batch ordering hazard and the global counter

- **Ours identifies and solves a specific hazard:** successive ordered batches whose *writes* are
  disjoint can still be *coupled by reads* and commit out of order (because the coordinator derives
  order from arrival at the orderer and submissions fan out to all orderers). Ours threads every
  BFT-ordered batch through **one global ordering counter** to force the fixed order (§1.2.2).
- **Fabric-EVM does not articulate this hazard or a counter.** In the trusted phases the dependency
  manager sidesteps it by gating on commits; for the trustless case it gestures at "dependency manager
  uses 'blocks' to avoid non-determinism" and a "`TxBatch` abstraction," but leaves the mechanism TBD.

### 4. Sequential-nonce handling: atomic bundle vs. sequenced separate txs

- **Ours bundles** a wallet's consecutive-nonce transactions into **one atomic Fabric-X transaction**
  that carries them verbatim, executed strictly in sequence, addressed by a `(block, index, sub-index)`
  coordinate (§2.6). All-or-nothing, correct order, no interleave.
- **Fabric-EVM sequences them as separate transactions** through the mempool/dependency manager, plus
  replacement-transaction support (same nonce, different payload). Partial commit is possible; ordering
  is enforced by scheduling rather than atomicity.

### 5. State layout granularity

- **Ours uses one account-record key** `0x01‖address → {nonce, balance, codeHash}` (plus storage and
  content-addressed code by `codeHash`). Coarse by design — any account touch conflicts — which is what
  gives free same-sender ordering.
- **Fabric-EVM splits the account** into `acc:<addr>:bal`, `:nonce`, `:code` (code keyed by address),
  plus `str:<addr>:<slot>`, and plans `input/<tx-id>` / `event/<tx-id>` keys to fold Fabric metadata and
  events into the read/write-set. Finer-grained conflict domains; more machinery.

### 6. State root / proofs

- **Ours explicitly addresses the Ethereum state root** as a **post-mortem** Merkle Patricia Trie the
  indexer builds after each committed block, off the hot path, for light-client/bridge proofs (§2.7).
- **Fabric-EVM's doc is silent on the state root / MPT.** Its SQLite index serves blocks, transactions,
  and logs, but state-root proofs are not discussed — a likely gap for light clients or bridges.

### 7. Scaling primitive

- **Fabric-EVM's scaling primitive is the dependency manager**: in-memory (Phase 1) → shared service
  (Phase 2) → distributed over etcd/ARMA (Phase 3+). It is *also* the load-distribution mechanism across
  replicas and organizations.
- **Ours has no separate dependency-manager service.** Independent traffic scales on Fabric-X's native
  parallelism; the contended minority scales via the trusted gateway (CFT) or the ordered pipeline +
  counter (BFT). R1 forbids changing the coordinator, so ordering is enforced with existing MVCC plus
  the counter, not a new coordination service.

## Where each could borrow from the other

- **Ours could adopt from Fabric-EVM:** the explicit dual-synchronizer split; the SnapshotDB `StateDB`
  wrapper pattern; a SQLite query backend; concrete go-ethereum + proposal types; and the mempool
  concerns (backpressure, replacement transactions, out-of-order/high-nonce buffering) that ours' §2.10
  currently lists only as open items.
- **Fabric-EVM could adopt from ours:** the order-first (OEV) path + global ordering counter as a
  concrete answer to its Phase-4 BFT "TBD"; the atomic sequential-nonce bundle with a sub-index; the
  post-mortem state-root treatment; and the "trusted executor owns the order, so it can re-order on
  retry" framing that distinguishes the crash-tolerant fast path from the Byzantine one.

## At a glance

| Dimension | Ours (`oev-executor-design.md`) | Fabric-EVM (`ARCHITECTURE.md`) |
|---|---|---|
| Base model | EOV default + **OEV order-first fallback** | **EOV throughout**, conflicts masked |
| Contention fix | CFT: trusted sequencer; BFT: order-first + counter | Dependency manager + mempool + retry |
| CFT vs BFT | **Execution-mechanism axis** (R9) | **Deployment phases** (3 = CFT, 4 = BFT) |
| BFT hot spot | Concrete (order-first + global counter) | **TBD** (likely ARMA) |
| Sequential nonces | **Atomic bundle** + `(blk, idx, sub-idx)` | Sequenced separate txs + replacement |
| Account state key | One record `{nonce, balance, codeHash}` | Split `bal` / `nonce` / `code` |
| State root / proofs | **Post-mortem MPT** by indexer | Not discussed |
| Query layer | Indexer over committed ledger | Gateway synchronizer → SQLite |
| Scaling primitive | Native parallelism + OEV pipeline | **Dependency manager** (in-mem → shared → distributed) |
| Document nature | Design & correctness argument | Implementation architecture + roadmap |
