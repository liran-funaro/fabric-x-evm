---
marp: true
theme: default
paginate: true
size: 16:9
header: 'EVM on Fabric-X — Execution & Trust'
---

<!-- _class: lead -->
<!-- _paginate: false -->
<!-- _header: '' -->

# EVM on Fabric-X
## Execution & trust, CFT-first

A design summary for the Fabric-EVM author.
**Phase 1 is crash-tolerant (CFT); BFT comes later.**

---

## The problem, in one line

- Independent EVM traffic already parallelizes on Fabric-X — leave it alone.
- **Contention is local:** one busy account (nonce) or one hot contract (shared storage).
- Under contention, execute-order-validate (EOV) **thrashes**: one winner per key, the rest abort and retry.
- Goal: **throughput ≈ goodput**, without an artificial global lock.

> Same target as your goodput problem — a different lever for the hot minority.

---

## One axis: who fixes the order

| | **CFT — Phase 1** | **BFT — later** |
|---|---|---|
| Trust | one node trusted (crash-only) | no node trusted |
| Who orders | **the gateway itself** | **the Orderer** (order-first) |
| On retry | **re-order freely** | bound to the fixed order |
| Global counter | **none** | **required** |

The fault model, not the EVM, decides the mechanism.

---

## CFT (Phase 1) — HOW

The **gateway is trusted**, so it owns the order:

1. Take contended txs in **FIFO / nonce order**.
2. **Execute** them in that order (two-phase: parallel warm → authoritative).
3. **Submit** — with a guard, because the Orderer may reorder submissions:
   - **Option A — merged batch:** one atomic Fabric-X tx (parts can't be split or reordered).
   - **Option B — independent set, then wait:** submit the non-conflicting set, wait for its block, submit the next.

Owning the order ⇒ on any conflict it just **re-orders and re-executes**. **No global counter.**

---

## CFT (Phase 1) — WHY

- **Matches the Phase-1 trust model** (single node / single org) — no cost paid for Byzantine tolerance you don't need.
- **Throughput ≈ goodput:** dependent chains serialize; everything independent commits in parallel.
- **This is your dependency manager, framed as trust:** submit-when-dependencies-commit *is* Option B.
- **Graceful under a true hot spot:** collapse the batch into one atomic tx (Option A) — fewer commit round-trips than one-at-a-time retry.
- **Cheapest thing that is correct.** Nothing global; nothing new in the Orderer or Committer.

---

## BFT — HOW

No node is trusted, so the **Orderer fixes the order** (order-first / OEV):

1. Submit **raw** txs as a committer-ignored envelope → the Orderer's block order is the agreed order.
2. A **quorum of executors** re-execute in that order, produce byte-identical results, **co-sign** (M-of-N).
3. Emit **one merged write-set per batch**, chained through **one global ordering counter**.

Executors are **bound to the fixed order** — they cannot re-order on retry.

---

## BFT — WHY

- **No trusted sequencer** ⇒ the order must come from consensus (the Orderer), not from any one node.
- **Cross-batch hazard:** batches with *disjoint writes* can still be *coupled by reads* and commit out of order.
- The **global ordering counter** chains batches into a linear sequence — they commit in the fixed order however the orderers interleave submissions.
- This is a **concrete answer to your Phase-4 "BFT: TBD."**

---

<!-- _class: lead -->

## Net

- **CFT / Phase 1 converges** with your dependency-manager design — we frame it as *trust owns ordering*.
- **BFT** adds what your roadmap leaves open: **order-first + a global ordering counter**, no trusted node.
- Plus: **atomic sequential-nonce bundles** and a **post-mortem state root**.

**Start CFT. Reach for BFT — and the counter — only when trust drops.**
