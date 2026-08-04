# EVM-on-Fabric-X Gateway — Execution & Trust Design

This document specifies how to run an Ethereum-compatible (EVM) gateway on top of Fabric-X. It
is self-contained: all needed background — including the EVM concepts, since the intended reader
is a Fabric-X expert who may be new to Ethereum — is explained where it is first used.

It has two parts:

- **Part 1** states the *general* problem: some workloads have such high contention that the
  optimistic commit model cannot make progress, and must instead be **ordered before execution**.
  This part is independent of the EVM.
- **Part 2** builds the **EVM gateway** on Part 1, offering both execution modes and showing how
  they coexist exactly as in the general case.

## Requirements

These requirements are drawn from the project's design intent and are referenced throughout.

- **R1 — No changes to the Orderer or Committer.** All new behavior lives in the gateway, the
  executors, and namespace/policy configuration.
- **R2 — Contention is local; keep the common path lock-free.** Independent work (different
  accounts, contracts, or keys) must commit in parallel on the optimistic path, and no component
  may funnel *all* traffic through a single shared key. Traffic ordered before execution (the
  contended fallback) may be serialized when correctness demands it, but that serialization must
  stay confined to the contended minority and never touch the optimistic bulk.
- **R3 — Trust is Fabric-X's existing model.** Authorization is governed by per-namespace
  endorsement policies expressed with Fabric's *MSP rule* (an M-of-N signature policy). Nothing
  new is invented for trust.
- **R4 — Two coexisting execution modes.** An optimistic mode (execute, then order) for the
  common low-contention case, and an ordered mode (order, then execute) for high-contention hot
  spots — running side by side over the same state.
- **R5 — Deterministic re-execution.** Re-executing the same ordered input must yield a
  byte-identical result.
- **R6 — Gas is monitored, not enforced.** The deployment is permissioned (all users are known),
  so per-transaction resource use is measured for observability but is not used to reject or
  charge transactions.
- **R7 — Sequential per-account correctness.** Transactions from one account must apply in their
  declared order with no other transaction interleaving between them, and a failed transaction
  must never allow a later dependent one to commit out of order.
- **R8 — Compatibility surfaces are post-mortem.** Block metadata, receipts, gas totals, and any
  cryptographic state commitment are derived *after* commit from the committed ledger, off the
  execution/commit hot path.
- **R9 — Configurable fault tolerance.** A deployment assumes one of two fault models: crash-only
  (a node may fail-stop but never returns a wrong result) or Byzantine (a node may act arbitrarily).
  The system must stay correct under the assumed model, and must not pay for Byzantine tolerance
  when only crashes are assumed.

## Fabric-X mechanics this design relies on

- **State is a flat key/value store**: `(namespace, key) → (value, version)`. Each write bumps
  that key's `version` by one. There is no built-in notion of an account or a counter.
  `service/vc/create_namespace_tmpl.sql:17-22`.
- **Optimistic concurrency (MVCC).** A transaction carries a *read-set* (keys with the versions
  it read) and a *write-set*. At commit, if any read key's committed version differs from the
  version the transaction read, the transaction is aborted (`ABORTED_MVCC_CONFLICT`).
  `service/vc/create_namespace_tmpl.sql:65-89`.
- **Dependencies are derived, not declared.** The coordinator infers ordering constraints from
  overlapping read/write keys and preserves the original block order for conflicting transactions;
  independent transactions run in parallel.
  `service/coordinator/dependencygraph/{dependency_detector.go,transaction_node.go:46-48}`.
- **Idempotency.** Transaction IDs are unique (a primary key); a duplicate ID is rejected
  (`REJECTED_DUPLICATE_TX_ID`). `service/vc/{init_database_tmpl.sql:19-49,committer.go:220-257}`.
- **Only supported envelope types are applied to state.** Unsupported types are ordered but not
  committed to state (R1's hook). `service/sidecar/mapping.go:105-138`.
- **Endorsement (trust).** Each namespace has a policy; the verifier checks all of a transaction's
  signatures against it, and the MSP rule supports M-of-N. `utils/signature/verify.go:63-187`.
- **Any component can read, follow, and submit.** A new service can query committed state
  (`service/query/*`), open its own stream of ordered blocks (`utils/deliverorderer/orderer.go`),
  and submit transactions to all orderers (`loadgen/adapters/broadcast.go`).

---

# Part 1 — Executing under high contention

## 1.1 The two execution orders

Fabric-X commits **optimistically**: run a transaction, record what it read and wrote, and let
the MVCC check at commit time catch anyone who read stale data. Call this **EOV — Execute,
Order, Validate**. When work is spread across unrelated keys, nothing conflicts and everything
commits in parallel — the ideal case, and the default (R4).

Optimism fails on a **hot spot**: many transactions competing to read and write the *same* keys.
Each one reads the key, but only the first to commit wins; every other read is now stale, so
those transactions abort. Re-running them does not help — by the time a retry executes, the key
has moved again. Throughput on that key collapses, and the retries waste work.

The fix is to **fix the order before executing** the contended transactions, not after:
**OEV — Order, Execute, Validate**. With the order already decided, there is nothing to race over;
the result is one conflict-free update. *Who* fixes the order — a single trusted component, or the
Orderer itself — depends on the fault model (R9), and everything else follows from that choice
(§1.2).

**Crucially, OEV is a targeted fallback, not the default (R2).** Contention is a property of
*specific keys*, not the whole system, so only the contended traffic is routed to OEV; all other
work keeps running EOV in parallel. OEV serializes that contended minority only as far as
correctness demands (§1.2), but the optimistic bulk never funnels through it, so Fabric-X's
parallelism is preserved.

## 1.2 Ordered execution: CFT and BFT

Both realizations produce an ordinary endorsed Fabric-X transaction (a read-set + write-set with
signatures) that the Committer applies with no special handling, and both execute the ordered
transactions against a small **per-batch write cache** so that a transaction sees the writes of
earlier ones. (As an optimization a batch runs in two phases: a parallel, read-only pass that warms
the cache with the keys it will touch, then an authoritative pass in the fixed order against the
warm cache, fetching anything still missing.) What differs is **who fixes the order** — decided by
the fault model (R9) — and everything downstream follows from that.

One hazard is common to both: an ordered batch must never let a later transaction commit while an
earlier one is still pending. Take `TX1: A += X + B` and `TX2: X++`, ordered before an unrelated
`TX3: B++`, starting from `A = B = X = 1`; the only serializable result is `A = 3, X = 2, B = 2`. If
the ordered transactions are emitted as *separate* transactions that fail and retry on their own and
`TX3` commits first, `TX1`'s read of `B` is stale so it aborts while `TX2` still commits (`X = 2`);
retrying `TX1` reads the new `X` and `B` and computes `A = 1 + 2 + 2 = 5` — a result no serial order
can produce, since `TX1` was fixed *before* `TX2` yet read `TX2`'s write. Each realization prevents
this its own way.

### 1.2.1 CFT

CFT assumes a component may crash but never returns a wrong result, so **one trusted executor owns
the order.** It takes the contended transactions in an order it chooses (for example FIFO by
arrival), executes them in that order, and signs the result alone — its one signature meets the
endorsement policy (R3). Because it owns the order, nothing is yet bound to a fixed sequence, so on
any conflict a retry is free to **re-order** and re-execute. A crash-failover standby resumes from
the last committed point; the deterministic result (R5) plus idempotency drop any duplicate.

The one hazard is that the **Orderer does not guarantee that transactions commit in the order they
were submitted**: two conflicting transactions submitted in order may still land in a block reversed.
The trusted executor prevents any such inversion in one of two ways:

- **Option 1 — one merged batch.** Emit the whole ordered batch as a single atomic Fabric-X
  transaction. Its constituents cannot be reordered or partially fail — which also closes the shared
  hazard above. Coarse: one conflict re-runs the whole batch.
- **Option 2 — submit the independent set, then wait.** Submit only transactions that do not conflict
  with each other; because they do not conflict, the Orderer may place them in any order harmlessly.
  Wait until they appear committed in a received block, then submit the next set — the transactions
  that were waiting on those, now themselves mutually independent — and repeat. Each round is
  conflict-free by construction, and the wait between rounds pins the cross-round order. Dependent
  transactions are not re-executed; their read/write-sets were computed in the chosen order, so only
  the observed committed versions are filled in. Finer than Option 1, at the cost of one ordering
  round-trip per dependency layer.

CFT needs **no global counter**: a single trusted executor controls submission timing and gates each
release on an observed commit, so ordering comes from that gating, not from a shared key.

### 1.2.2 BFT

BFT assumes a component may behave arbitrarily, so no single result is trusted and **no single
component may fix the order.** The order is fixed by the Orderer instead: a transaction destined for
BFT-ordered execution is submitted with an envelope type the Committer does not apply to state (R1)
— the Orderer still orders it, the Committer ignores it — and a set of **executor** services follow
the ordered block stream, pick out these raw transactions by type, and re-execute them in block
order against the per-batch cache. They produce byte-identical results (R5) and co-sign; the MSP
rule's M-of-N threshold is met by collecting enough matching signatures (R3). Any executor holding
enough signatures assembles the final transaction and submits it to all orderers (so a single faulty
orderer cannot suppress it); duplicate submissions collapse via idempotency. A batch spanning several
namespaces must satisfy each one's policy — fine, since every executor runs it and can endorse all
touched namespaces.

Unlike CFT, BFT **cannot re-order on retry**: the order was fixed by the Orderer and every executor
is bound to reproduce it, so a failed batch must re-execute in the *same* order (R7). Two mechanisms
enforce that fixed order, and the running example shows why neither alone suffices.

- *Within a batch — atomic merge.* As the shared hazard above shows, ordered transactions emitted
  separately can retry into a non-serializable result. The fix is to make a batch **one atomic
  Fabric-X transaction**: its transactions commit together, in order, or not at all. This is why a
  BFT executor emits a single merged write-set per batch.

- *Across batches — the global ordering counter.* Atomicity within a batch is not enough; successive
  batches must also commit in order, and shared write-keys cannot be relied on to enforce it. Take
  the same example with `TX1`, `TX2`, and `TX3` now each in a *different* block. By write-key they
  look independent — `TX1` writes `A`, `TX2` writes `X`, `TX3` writes `B` — so nothing about their
  writes forces an order, yet the fixed order still requires `TX1` to commit before `TX2` and `TX3`
  (it *reads* `X` and `B`). The coordinator derives ordering purely from *arrival order at the
  orderer* (`service/coordinator/dependencygraph/transaction_node.go:46-48`), forming a read→write
  edge only in whatever order transactions arrive and *no* edge between transactions that merely read
  the same key (`dependency_detector.go:54-60`). Because ordered batches are submitted to every
  orderer independently (fan-out for liveness), their arrival order is not controllable — so if `TX2`
  or `TX3` arrives first, `TX1` aborts with no valid retry. This is not specific to one namespace:
  reads couple batches that share no writes, and an ordered transaction's read-set cannot be bounded
  in advance, so there is no safe way to *scope* the ordering to a subset of keys. The design
  therefore threads **every** BFT batch through **one global ordering counter**, read and written by
  every batch in block order: batch *N* is given the counter version its predecessor produces, so it
  reads version *v* and writes *v + 1* and batch *N + 1* reads *v + 1*. This makes the batches a
  linear chain — batch *N + 1*'s recorded read is valid only once batch *N* has committed and bumped
  the counter to *v + 1* — so they commit in exactly the fixed order however the orderers interleave
  the submissions; a batch whose submission gets ahead of its predecessor finds the wrong counter
  version, aborts, and retries once the predecessor lands. (Assigning the expected version from the
  block order, rather than reading whatever is currently committed, is what pins the chain to the
  *ordered* sequence instead of to a commit race.)

**Cost, and why it satisfies R2.** The counter is a single key that all BFT-ordered traffic passes
through, so that traffic runs as one globally-ordered pipeline — the deliberate price of correctness
without trust. It is bounded: only the contended, BFT-ordered minority is serialized; all optimistic
(EOV) traffic keeps committing fully in parallel and never touches the counter, nor does CFT-ordered
traffic. Contended state is serial by nature anyway, so the only real cost is that two *independent*
hot spots ordered at the same time wait on each other.

**Determinism (R5).** Co-signing requires byte-identical results, so all execution inputs are pinned
and canonical: execution rules are configured on-chain and versioned; the write-set is encoded
canonically (keys sorted, deduplicated, last write wins); and the transaction ID is a deterministic
function of the batch contents (not of any submitter identity or wall-clock). The same pinning lets
a CFT standby reproduce an in-flight batch for idempotent dedup.

## 1.3 Coexistence (EOV and OEV together)

- **Shared state, natural interleave.** Both modes write the same namespaces and emit the same
  kind of endorsed transaction, so they mix with no special handling.
- **MVCC arbitrates conflicts.** If an EOV transaction and an OEV batch touch the same key, one
  aborts: the EOV transaction retries; the OEV side re-runs — a BFT executor re-executes in the
  fixed order as the next link in its chain, a CFT executor re-orders and re-executes.
- **Routing by contention.** A domain runs EOV until its observed abort rate crosses a threshold,
  at which point its traffic is routed into ordered execution (§1.2); it can be routed back when
  contention subsides.

---

# Part 2 — The EVM gateway

## 2.1 EVM primer for Fabric-X readers

The **Ethereum Virtual Machine (EVM)** is a deterministic state machine. Its world state is a set
of **accounts**, each identified by a 20-byte address. An account holds:

- a **nonce** — a counter of how many transactions the account has sent. It must increase by
  exactly one per transaction. This enforces per-sender ordering and prevents replay (a
  transaction with an already-used nonce is invalid).
- a **balance** — the account's native-token amount.
- **code** — for contract accounts, the program (bytecode) that runs when the account is called.
- **storage** — for contract accounts, a private key/value map of 32-byte **slots** → 32-byte
  values, the contract's persistent memory.

A **transaction** is signed by an account's key and, when executed, reads and writes some accounts
and storage slots. Standard EVM transactions do **not** declare in advance which keys they touch —
that is only known after running them, which is why Ethereum clients historically execute a
block's transactions one at a time on a single thread. Independent transactions (disjoint accounts
and contracts) have no real conflict; Ethereum only *appears* fully sequential because of two
historical choices: single-threaded execution, and a single **state root** (see §2.7) committed
per block. Modern "parallel EVMs" exploit the fact that unrelated transactions can run
concurrently — which is exactly the position Fabric-X starts from.

Real contention in the EVM is therefore **local**: it arises when many users hit the **same
contract's storage** at once (for example, everyone trading against one popular exchange contract,
or transferring one popular token), or when one account sends many transactions (its nonce).

**Gas** is the EVM's unit of computational cost; each transaction consumes gas as it runs, and
each block reports the total gas its transactions used. In public Ethereum gas is used to charge
fees and cap work; its role here is limited (§2.6).

## 2.2 Mapping requirements to the EVM

- Independent accounts/contracts must run in parallel (R2). Fabric-X already does this via derived
  dependencies, so the gateway must not add a global bottleneck to that optimistic traffic (the
  global ordering counter of §1.2 touches only BFT-ordered contended traffic, never the optimistic
  bulk).
- Only two situations are truly contended and need ordering (R7): **same sending account** (nonce
  order), and **same contract storage** (a hot contract). Everything else streams optimistically.
- Both the optimistic and ordered modes of Part 1 apply unchanged (R4); the EVM is just the
  "execution" step inside them.
- The fault model (R9) decides how contended traffic gets its order fixed: a single trusted gateway
  under CFT (no Orderer pre-ordering), or the Orderer via OEV under BFT — the former lighter (§2.5).

## 2.3 EVM state layout — a single `evm` namespace

All EVM state lives in one Fabric-X namespace, `evm`, with keys distinguished by a one-byte prefix:

- `0x01 ‖ address` → the account record `{nonce, balance, codeHash}`.
- `0x02 ‖ address ‖ slot` → a 32-byte storage value.
- `0x03 ‖ codeHash` → contract bytecode.

Because Fabric-X detects conflicts *per key* (§"Fabric-X mechanics"), this flat layout already
gives maximal parallelism — two transactions touching different slots of the same contract do not
conflict — while one namespace keeps a single endorsement policy. The **nonce lives inside the
account value**, not in Fabric-X's per-key `version` (which is the MVCC token). Same-sender
ordering then falls out for free: every transaction from an account reads and writes that
account's record, so two of them necessarily conflict and are serialized (R7).

## 2.4 EVM under EOV (optimistic — the default)

1. A client sends a signed EVM transaction to the gateway.
2. Gateway endorsers each run it in an embedded EVM against current committed state (read via the
   query service), producing its read-set (accounts/slots/code with versions) and write-set.
   Because EVM execution is deterministic, honest endorsers produce identical results and co-sign
   them (R3, R5).
3. The signed transaction is submitted; on an MVCC abort it is re-executed against fresh state and
   retried.

## 2.5 EVM under contention (busy accounts, hot contracts)

When a contract's storage (or one account) is contended enough that EOV retries stop making
progress, its transactions are routed to ordered execution (§1.2). The two realizations specialize
to the EVM as below; contracts and accounts not routed keep running EOV in parallel (R2).

- **CFT (§1.2.1).** The **gateway** is the trusted executor. It orders the contended EVM
  transactions itself — FIFO by arrival, and by nonce within an account (§2.6) — executes them with
  the two-phase cache, and submits them either as one merged batch or by submitting the independent
  set and waiting for each round's block. Because it owns the order, a conflict lets it re-order and
  re-execute; no global counter.
- **BFT (§1.2.2).** No component is trusted to fix the order, so raw EVM transactions are submitted
  as the committer-ignored type and the Orderer fixes their order; a quorum of executors re-execute
  them in block order, co-sign to meet the M-of-N MSP rule, emit one merged write-set per batch, and
  chain the batches through the global ordering counter.

## 2.6 Merging an account's sequential transactions

A wallet often sends several transactions back-to-back with consecutive nonces (`n, n+1, n+2`).
Handled naïvely under EOV they collide: all three read the same account record, so only the first
commits and the rest abort.

**The gateway bundles them.** If it observes several transactions from one account with
consecutive nonces arriving close together, it batches them — using a time-window batcher of the
same kind the query service already uses for read requests — into **one Fabric-X transaction that
carries the individual EVM transactions unchanged** and marks them to be executed **strictly in
sequence with nothing interleaved**. This is safe precisely because a single Fabric-X transaction
is atomic and indivisible: no other transaction can slip between the bundled ones, and they commit
all-or-nothing in the correct order (R7).

The bundle works in either mode:

- **EOV:** a gateway endorser executes the sequence and produces one combined read/write-set for
  the whole bundle.
- **OEV:** the post-ordering executor (itself an endorser) does the same after ordering.

**Per-transaction bookkeeping and the sub-index.** Bundling raises one question: does collapsing
several EVM transactions into one Fabric-X transaction lose anything Ethereum tools expect
per transaction? The affected items are the per-transaction **receipt**, **transaction hash**, and
**position within the block** — clients look transactions up by hash and by `(block, index)`, and
read a receipt for each. Bundling does not change the transactions themselves (they are carried
verbatim), only their Fabric-X container, so nothing about their execution is lost. To keep each
one individually addressable, the indexer (§2.7) assigns a three-level coordinate —
**`(block number, transaction index, sub-index)`** — where the sub-index enumerates the EVM
transactions inside one bundle. Each EVM transaction keeps its own hash and receipt; the sub-index
is simply how the indexer points back into the bundling Fabric-X transaction. No other bookkeeping
is affected.

## 2.7 Blocks, receipts, gas, and state root — all post-mortem (R8)

The gateway does not produce Ethereum blocks during execution; it reconstructs them **after**
Fabric-X commits, with an **indexer** that follows the committed ledger. This keeps Ethereum's
block-shaped bookkeeping off the hot path.

- **EVM block = committed Fabric-X block.** The EVM block number is a counter over committed
  Fabric-X blocks; every EVM transaction (EOV or OEV) committed in Fabric-X block *B* belongs to
  EVM block *B*, ordered by `(index, sub-index)`. The indexer serves the read APIs clients expect
  (get block by number, get transaction by hash, get receipt).
- **Block timestamp** is the Orderer's consensus-agreed timestamp for that block. *(This is a
  planned Orderer capability; until it exists, the block time must come from an agreed source, not
  from any single component's wall-clock.)*
- **Gas is monitored, not enforced (R6).** Because the environment is trusted (all users are
  known), the gateway does not use gas to reject or charge transactions. It **measures** the gas
  each transaction actually consumed and reports the per-block total — a figure that is inherently
  a **post-mortem** sum, computable only after execution settles (a transaction's declared gas
  limit is only an upper bound, and only transactions that actually commit contribute). This is
  used for observability. Note the asymmetry that makes pre-enforcement unattractive here: the EVM
  can reject a transaction that *by itself* asks for more than a limit, but it cannot know *which*
  transaction would tip a shared budget over until it has run — and charging submitters for retries
  they did not cause would be unfair. Monitoring sidesteps all of this.
- **State root (Merkle tree) is post-mortem.** Ethereum commits, in each block header, a single
  cryptographic hash — the root of a **Merkle Patricia Trie** over the entire world state — used
  by light clients and bridges to prove "this account/slot had this value" without holding all the
  state. Computing it is I/O-heavy and, in standard Ethereum, is what forces every transaction to
  converge on one structure per block. This design does **not** put it on the hot path: Fabric-X
  commits flat key/value state with its own integrity. If Ethereum-style proofs are needed, the
  indexer builds the trie and computes the root **after** each Fabric-X block is fully committed —
  a pure function of the committed state, so it is deterministic and can be produced (and agreed)
  independently. This is sound: it delivers the compatibility surface without imposing Ethereum's
  state-root bottleneck on execution. The only cost is a small lag — the root for block *B* is
  available shortly after *B* commits — which is acceptable for proof/bridge use.

## 2.8 Coexistence for EVM

Identical to Part 1, specialized to EVM: disjoint accounts and contracts run EOV in parallel; an
account's own transactions are ordered (and typically bundled, §2.6); a hot contract is promoted to
the contended handling of §2.5. MVCC arbitrates any cross-mode conflict, and the gateway moves
traffic between modes based on observed contention. The optimistic bulk never serializes on a shared
key (R2); only the contended traffic serializes, and only as far as its fixed order demands — via
the global counter of §1.2 under BFT, or, under CFT, by the gateway gating on observed commits
(§2.5).

## 2.9 Components to build

- **Gateway:** Ethereum JSON-RPC endpoint; embedded EVM; query-service reader; read/write-set
  builder; sequential-nonce bundler; endorsement collection; submit + retry; contention-based
  routing (§2.5). Under CFT it also fixes the order for contended traffic itself — FIFO execution
  with the two-phase cache, then merged-batch or submit-independent-then-wait submission, plus a
  crash-failover standby (§1.2.1).
- **Executors (BFT only):** own block-stream follower; deterministic EVM re-execution with the
  two-phase per-batch cache; write-set builder; signature exchange and all-orderer submission;
  global-counter batch ordering with rollback (§1.2.2).
- **Indexer:** reconstructs EVM blocks, `(block, index, sub-index)` coordinates, receipts,
  post-mortem gas totals, and (if required) the Merkle state root, over the committed ledger; serves
  the Ethereum read APIs.
- **Configuration:** the single `evm` namespace and its endorsement policy — one signer under CFT,
  M-of-N under BFT (R9); the committer-ignored envelope type for raw OEV transactions (BFT only); the
  reserved key that backs the global ordering counter (§1.2, BFT only — CFT uses no counter); the
  on-chain EVM execution rules.

## 2.10 Open items

- **EOV↔contended routing and promotion.** The contention threshold, promotion/demotion, and the
  rollback-and-re-execute recovery are specified at a phase-1 level; adaptive routing is future work.
- **State-root and block-hash details.** The exact trie encoding, block-hash definition, and proof
  API are deferred to the compatibility work.
- **Orderer timestamp.** The design assumes an agreed per-block timestamp from consensus (planned).
