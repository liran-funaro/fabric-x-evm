# Assistant notes for the author

Notes from me (the assistant) to you, flagging where [`oev-executor-design.md`](oev-executor-design.md)
departs from, resolves, or depends on something in your source material (`my-idea.md`, `my-notes.md`,
`previous-context.md`). The design document is self-contained and references none of these; this file
is the only place that cross-references them, so read it alongside the design.

Notes are tagged: **[decide]** needs a call from you · **[verify]** a claim in your domain I want
you to check · **[fyi]** a route I took that differs from a source note but is already resolved.

---

## [decide] The global OEV counter contradicts your `previous-context.md` guidance

This is the one thing I most want you to confirm.

`previous-context.md` (the imported "EVM contention" notes) says, explicitly: *"Do not introduce an
artificial global lock"* and *"Only enforce strict ordering rules when incoming transactions share a
matching Origin Account (Nonce tracking) or point to the exact same Smart Contract Storage
Namespace."* The design does the
**opposite** for OEV: §1.2 threads *every* OEV batch through **one global sequence counter**, and I
softened requirement **R2** (line 22) so its "no global lock" now applies only to the optimistic
path, naming OEV as the deliberate exception.

**Why I overrode your guidance.** Scoping the order to matching account / contract-storage keys is
unsafe for *ordered* execution, because an ordered transaction can *read* keys outside that scope
and those reads must still be ordered relative to writes elsewhere. Fabric-X's coordinator orders
conflicting transactions by *arrival order at the orderer* (not by the intended order) and forms no
edge at all between transactions that only read the same key, and an ordered transaction's read-set
cannot be bounded in advance (an EVM call may touch any account or contract). §1.2's cross-block
example (`TX1: A += X + B`, `TX2: X++`, `TX3: B++`, each in a different block) shows disjoint-write
batches committing out of order and producing a non-serializable result (`A = 5`). So the "local
ordering only" rule you imported holds for *routing* (only contended traffic goes to OEV) but not
for the *ordering mechanism itself*.

**What this costs, and why I think it's acceptable.** The counter serializes only OEV — the
contended fallback, a minority of traffic — while all optimistic (EOV) traffic stays parallel and
never touches it. So your "no artificial global lock" instinct is preserved for the common path; it
is only the already-contended, ordered traffic that accepts a global sequence.

**Your call:** accept overriding the imported "scope the ordering" guidance (current design), or ask
me to pursue a bounded/sharded counter. I looked at sharding per provably-independent domain and
dropped it — unpredictable read-sets mean independence can't be guaranteed — but if you can
constrain what OEV transactions may read, a per-domain counter becomes viable.

**CFT fully reclaims your "no global lock" instinct.** CFT uses *no* global counter at all — a single
trusted executor fixes order and gates submission on observed commits — so on the contended path
under CFT your `previous-context.md` guidance is honored outright. The global counter is a BFT-only
device (I renamed it "global ordering counter" and scoped it to BFT throughout).

## [fyi] CFT is now formalized in Part 1 (general), CFT before BFT, with both your corrections applied

I added the fault model as a new axis (requirement **R9**) and, per "Part 1 should address BFT and
CFT," lifted the whole CFT/BFT split into **Part 1** (§1.2, retitled "Ordered execution: CFT and
BFT") in general terms — no EVM, no "gateway"; the trusted actor is "one trusted executor." §1.1 no
longer says contention is fixed "via the Orderer"; it now says *who* fixes the order depends on the
fault model. Part 2 §2.5 is now a short EVM specialization (CFT = the gateway is that trusted
executor; BFT = the executor quorum) that points back to §1.2.1 / §1.2.2.

Both corrections you gave are in:
- **CFT is not OEV/Orderer-first.** The trusted executor fixes the order itself (e.g. FIFO), executes
  (two-phase), and submits — its only problem is that the Orderer does not preserve submission order,
  so it must stop conflicting transactions being inverted in a block. Two options: (1) one merged
  RW-set; (2) submit the mutually-independent set, wait for its block, submit the next set.
- **CFT owns the order, so it can re-order on retry** — §1.2.1 / §1.2.2 now contrast this explicitly:
  CFT re-orders and re-executes on any conflict, whereas BFT is bound to the Orderer's fixed order and
  must re-execute the *same* order (which is exactly why BFT needs the counter and CFT does not).

## [verify] CFT trusted-executor assumptions I want you to sanity-check

Two load-bearing claims in §1.2.1: (a) **Determinism is still needed under CFT** — not for real-time
co-signing, but so a crash-failover standby reproduces an in-flight batch *byte-identically* and
idempotency (deterministic TX ID) drops the duplicate. I therefore broadened **R5** to plain
"same ordered input → byte-identical result" (it was previously conditioned on several endorsers
co-signing). (b) In Option 2, an "independent set" is an antichain of the dependency DAG from the
executor's chosen order — mutually non-conflicting, so the Orderer may place them in any order
harmlessly, and the wait for their block before the next set pins cross-set order. Confirm both, and
whether the trust unit you want is a single executor with a standby, or a small CFT quorum.

## [decide] The "Fabric-X mechanics this design relies on" section vs. your new "don't explain Fabric-X" rule

Your updated `agent-instructions.md` now says *"Do not explain how Fabric-X works... mention it
without explaining."* The design's "Fabric-X mechanics this design relies on" section (MVCC read/write
sets, dependency derivation, idempotency, etc.) does explain some of these — redundant for a Fabric-X
expert. I did **not** delete it, because its specific, code-cited behaviors (arrival-order dependency,
no read-read edge) are load-bearing for §1.2's non-serializability argument. Two options: trim it to
bare "mechanism → code-ref" pointers, or keep it as design justification. Your call — I left it intact
for now.

## [verify] Gas claims (`my-notes.md` item 5)

You flagged you're not an EVM expert and wanted "only sure items, not maybes." §2.7 now claims only
two things, both of which I'd like you to sanity-check against your own EVM knowledge: (a) gas is
*measured* post-mortem for monitoring and *not enforced* (trusted deployment, so no reason to charge
submitters for retries they didn't cause); (b) the EVM can reject a transaction that *by itself*
exceeds a limit, but cannot know *which* transaction would tip a shared budget over until it runs. I
removed every "maybe," but these two are the load-bearing factual claims — if either is wrong, §2.7
needs a fix.

## [verify] The design depends on an orderer per-block timestamp that doesn't exist yet

§2.7 (block timestamp) and §2.10 assume a consensus-agreed per-block timestamp from the orderer. You
noted this is planned but unbuilt. Flagging it as a real external dependency: until it lands, the
EVM block timestamp has no agreed source, and any component substituting its own wall-clock would
break determinism.

## [fyi] `my-idea.md`'s global counter is now followed faithfully

An earlier draft of the design dropped your global counter for a scoped-ordering scheme; that was
**reversed** (see the [decide] note above), so the design is now aligned with `my-idea.md`'s
"single RW-set TX with a dependency on a global block value." Two small generalizations: the counter
is applied per *batch* over the ordered stream rather than literally "the entire block," and it is
framed as a general OEV mechanism, not EVM-specific.

## [fyi] `previous-context.md`'s "explicitly flag dependencies in the dependency graph" → not done

`previous-context.md` proposed that the gateway *"explicitly flags dependencies via the internal
Fabric-X dependency graph"* and forms same-address linear chains inside it. The design does **not**
do this, because R1 forbids changing the coordinator. It achieves the same effect without any
coordinator change: same-account ordering falls out of the shared account-record key (§2.3), and
consecutive nonces are merged into one atomic transaction by the gateway (§2.6). Same outcome,
different (R1-compatible) mechanism.

## [fyi] Out-of-order high-nonce handling is only lightly specified

`previous-context.md` describes the gateway buffering a high (out-of-order) nonce until its
predecessors clear. §2.6 covers *merging consecutive nonces that arrive close together* but does not
spell out the buffer-and-release behavior for a gap in the nonce sequence. If you want that mempool
behavior treated as a first-class requirement, it should be added to §2.9's gateway component list.

## [fyi] The OEV ordering counter is not the EVM block number

`my-idea.md` tied ordering to a "global block value," which could be read as *the* block counter.
The design keeps two separate things: the global OEV counter (§1.2) that orders OEV batches, and the
EVM block number (§2.7), defined as the committed Fabric-X block. They advance independently — the
OEV counter barely moves in the common all-optimistic case.

## [fyi/resolved] Merkle/state-root post-mortem (`my-notes.md` item 7) — yes, it holds

You asked whether building the Merkle state root post-mortem "makes sense." It does, and §2.7 says
why: the root is a pure function of the committed flat state, so the indexer can compute (and agree
on) it after each block commits, off the hot path — at the cost of a small lag, which is acceptable
for proof/bridge use. No inconsistency; recording the answer here so the question isn't lost.
