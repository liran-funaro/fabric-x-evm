# Original Pitch

Latency issue solution:
Drop the local world state (don't maintain anything).
Take a batch of TXs to execture, then run a two phase approch:
1. Simulate all TXs in parallel using the query service.
Make sure to use the same query service view ID for all TXs.
Then, take all the read-set from all TXs and write them to in memory cache.
Now, you have a hot-cache with all read keys.
1. Execute the TXs one by one, using the cache. After each TX, update the cache according to the write-set.

To submit the TXs, we can use one of two approches:
1. (preffered) Submit a single Fabric-X TX for all EVM TXs batch.
This way, the correct order is gurenteed.
We can keep in the TX metadata all of the EVM TXs that is included and refer to the TX with a triple: #blk, #idx, #sub-idx

2. Submit and wait like we do now. If we choose this option, we can wait for the TX from the Orderer (not the committer) before submitting the next TX. This is optimistic (assumes it will be valid).
If some fail later, we can retry them.

This is for the CFT approch. If this works well, I have a plan for the BFT which we can discuss later.

---

# Refined pitch

Hey Ale — a CFT idea for the latency problem, built on top of your architecture. Keen to hear your thoughts.

**Simulation Problem:** the EVM local world state must stay synced to simulate correctly — the sync-lag/staleness cost.
**Proposal:** drop the local world state entirely; run each batch in two phases:

1. **Warm.** Simulate the batch in parallel on the query service, all under one view. Collect every read key into a cache.
2. **Authoritative.** Re-run the TXs sequentially against that cache, applying each write-set before the next; fetch any miss from the same view.

**Why it helps:** reads stay at one round-trip for the batch, not one per TX — so dropping local state costs nothing on read latency.

**Ordering Problem:** dependent TXs must commit in order; today you enforce that by waiting for each predecessor to commit before submitting the next — a round-trip per dependency layer.
**Proposal:** submit one Fabric-X TX for the whole batch. Atomicity guarantees order in a single commit round-trip — no per-layer wait. The EVM TXs live in metadata, addressed `(blk, idx, sub-idx)`.
Coarse: any MVCC conflict re-runs the batch.

**Fallback** (if you'd rather not merge into one TX): keep your submit-and-wait, but wait for the *Orderer*, not the *Committer* — release a dependent batch as soon as its predecessors appear in an ordered block. That pins the order and shaves the commit-validation wait per layer.
Caveat: ordered ≠ valid — still track the Committer async and re-execute any batch that aborts.

That is the CFT path. If it holds, I have a BFT plan for later.
