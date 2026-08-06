# EVM Design Pipeline Optimization

The problem: warmup, auth, and submit run serially today - a batch is warmed, then authed, then submitted, and nothing overlaps.
The cost of a batch is therefore the sum of the three, so the two phases that are latency-bound rather than CPU-bound - warmup, which waits on the query service, and submit, which waits on ordered delivery - are paid in full on the critical path.

The claim: pipelining the three phases hides the warmup and submit latencies behind auth, leaving auth as the only limiter.
That is the right limiter, because auth is the one phase that cannot be parallelized - it is what establishes the serial order.

The claim rests on a single number: the fraction of txs auth accepts straight from the warmup read-write set without re-executing them (see Auth Worker).
It is workload dependent - 100% for a workload with no conflicts (the synthetic dataset has none), materially lower for one with many (the real-world dataset) - so it has to be measured per workload rather than assumed.

Working assumptions:
- This gateway is the only writer of the state we execute against.
  Every mechanism below detects our OWN uncommitted writes; a write from outside would produce a stale read with nothing to flag it.
  This holds for the current CFT single-gateway deployment, and is one of the things BFT has to revisit.
- A commit notification implies query-service visibility: once the committer tells us a version is committed, a query-service read STARTED after that point returns that version or a newer one, never an older one.
- Gas is not counted.
  The EVM credits the gas fee to a single account, so counting it would make EVERY tx read and write that one shared key - a speculated-write for every tx after the first, which would keep the fast path from firing at all.
  Today the fee is zero, so the credit touches no state. Counting gas needs the treatment in Future Work.

3 phases:
- Warmup
- Auth
- Submit

## Queues:

- `tx-input-queue` - TXs from outside (e.g., clients)
- `warmup-notification-queue` - notification received by the warmup phase
- `auth-input-queue` - warmup-batches or notifications
- `submit-input-queue` - auth-batches or notifications

## Queues Config:

- Size for each queue


## Warmup Config:

- Batch size and timeout
- Max in flight txs
- Max warmup cache size (bytes) (keys + values + versions, not including index)
- Cache entry time-to-refresh

Max in flight txs should be very large in the beginning to allow maximal parallelization.
But can be reduced if resources are exhausted.
Other parameters can be decided using a parameter sweep.

## Warmup Cache:

Most frequently used (MFU) eviction policy.
Use synchronization to maintain the index to allow parallel access from all active warmup workers.
We can use sync here since the bottleneck is the network IO anyway, so it won't degrade performance.
Keeps key->value,version mapping.
The warmup cache holds committed state only - never an uncommitted transaction write.
Key, value, version are immutable. A key update is a new write to the cache using a new value slice - either a fill after a query-service read, or a notification refresh carrying an already-committed value.

## Warmup TXs Worker:

- Consumes txs from `tx-input-queue`, batches them, spawn a warmup-batch-worker for each batch to work in parallel.
- Keeps active in-flight-txs txs atomic counter to maintain the limit, the batch worker decrease the counter when done.
  Hold if the number of active TXs (in active warmup phase) is over max in flight TXs.

## Warmup Batch Worker:

- First, number the batch using an atomic counter: `batch-index` (inc and get). See BatchAssign() below.
- Execute all txs in the batch in parallel, utilizing the warmup cache.
  Fallback to the query service - no need for views (nil view), using a connection pool shared between the workers (the pool needs no sync of our own; a pool rather than one connection because concurrent reads on a single HTTP/2 connection serialize - a sweep measured 4212 tx/s at 1 connection rising to a ~5040 tx/s plateau at 4-16, regressing at 32).
- When done, push batch to `auth-input-queue` along with the full read write set.
  The read write set includes all the content - the key, version, value - not just part of it.
  For example, a read-write will include the read version, read value, write version, and write value.
  See BatchReportDone() below.
- The worker then decrease the in-flight-txs counter.

## Warmup Notification Worker:

See NotificationWorker() below.
This process ensures that all warmup-batches already seen the cache update, thus it is safe to update the auth phase cache.

#### Cache Update:

MFU eviction RETAINS a hot key, so without refresh, a hot key would serve its pre-commit version indefinitely.
We have two mechanisms to refresh the cache:
- On a notification it first refreshes the warmup cache: for every committed key ALREADY RESIDENT it updates the value/version in place and keeps the frequency statistics. A key that is not resident is ignored - the next read fetches it fresh anyway - and if a refreshed key is evicted later, that is the MFU policy's decision.
- Another is the cache entry time to refresh. Once the cache lives more than the config, it marked for refresh, so the next access will refresh the value from the query service.

A cache fill from the query service must never lower a key's cached version.
The visibility assumption covers reads STARTED after a notification, and says nothing about reads already in flight when it arrives - those can land after the refresh still carrying the older version, and would otherwise overwrite a refreshed entry with a stale one.

The notification refresh must complete BEFORE the notification is stamped with a batch number. Batches numbered above the stamp are formed after the refresh and so read the committed value; batches at or below it may still hold a pre-commit read, which is what the sequencer below holds the notification for. Stamping first would put the batch at the watermark on the wrong side of that split.

#### Notification Sequencer Pseudo Code

```pseudocode
struct Slot:
    bool batchReceived = false
    List<Notification> pending = []

class RingBufferSequenceTracker:
    Mutex mu
    AtomicInt batchCounter = 0
    int nextExpected = 1
    Array<Slot> slots
    int capacityGrowFactor

    function Init(initialCapacity, capacityGrowFactor):
        this.slots = new Array<Slot>(initialCapacity)
        this.capacityGrowFactor = capacityGrowFactor

    // -------------------------------------------------------------
    // Batch Lifecycle
    // -------------------------------------------------------------

    function BatchAssign(batch):
        // Post-increment, so batchCounter always holds the number of the LAST batch
        // assigned (not the next one). A notification sampling it therefore names a
        // batch that already exists, which is what makes the release test in
        // NotificationWorker both correct and free of an idle deadlock.
        // Assigned here, before the batch reads anything - see NotificationWorker.
        batch.seqNumber = atomicIncAndGet(batchCounter, 1) // Returns the number of the increment.

    function BatchReportDone(batch):
        // The batch number is not relevant past the sequencer, but we don't have to omit it.
        auth-input-queue.push(batch)

        mu.lock()
        ensureCapacity(batch.seqNumber)
        slots[batch.seqNumber % len(slots)].batchReceived = true

        ready = []
        if batch.seqNumber == nextExpected:
            ready = advanceSequence()
        mu.unlock()

        // Push AFTER releasing mu. auth-input-queue is bounded and can fill (auth
        // feeds submit-input-queue, and the submitter stops consuming while it waits
        // for ordered delivery). Blocking on a full queue while holding mu would
        // freeze every warmup batch worker trying to report done.
        // The whole released run goes in as ONE item - a queue carries whatever type we
        // want - so advancing over many slots costs a single push, not one per slot.
        if not ready.isEmpty():
            auth-input-queue.push(ready)

    // -------------------------------------------------------------
    // Notification Registration
    // -------------------------------------------------------------

    function NotificationWorker():
        for n := warmup-notification-queue:
            // Update only the existing cache items with the latest values/versions for the written keys.
            // Discard non cached writes.
            // MUST run BEFORE sampling batchCounter. Every batch numbered above the
            // sampled value is assigned after this refresh, so it reads the committed
            // value. Sampling first would leave a window in which the batch at the
            // watermark is assigned, reads the pre-commit value, and is still released
            // after this notification - a stale read the fast path would accept.
            updateCache(n.writes)

            // Batches 1..seqNumber were assigned before the refresh, so they may carry
            // a pre-commit read and must reach auth BEFORE this notification does.
            n.seqNumber = atomicRead(batchCounter)

            mu.lock()
            if n.seqNumber < nextExpected:
                // Every batch through n.seqNumber was already pushed to auth, and the
                // queue is FIFO, so forwarding now cannot overtake them. This branch is
                // also what keeps an idle pipeline from parking a notification forever:
                // when the last assigned batch reports done, nextExpected passes it.
                mu.unlock()
                auth-input-queue.push([n]) // same item type as the released-run push
                continue

            // n.seqNumber >= nextExpected: must wait in slot for batch n.seqNumber
            ensureCapacity(n.seqNumber)
            slots[n.seqNumber % len(slots)].pending.append(n)
            mu.unlock()

    // -------------------------------------------------------------
    // Internal Helpers
    // -------------------------------------------------------------

    function ensureCapacity(targetSeqNumber):
        // Cannot be negative since the target is always greater than next.
        targetCapacity = targetSeqNumber - nextExpected
        curSize = len(slots)
        if targetCapacity < curSize:
            return

        // newSize must be strictly GREATER than targetCapacity, not just equal to it:
        // the live window occupies offsets 0..newSize-1 from nextExpected, so a newSize
        // equal to targetCapacity wraps the new slot onto nextExpected's own slot.
        newSize = curSize
        while newSize <= targetCapacity:
            newSize *= capacityGrowFactor
        newSlots = new Array<Slot>(newSize)

        for i = nextExpected to nextExpected + curSize - 1:
            newSlots[i % newSize] = slots[i % curSize]

        slots = newSlots

    // Caller must hold mu. Returns the notifications to forward, rather than pushing
    // them itself, so the caller can push after releasing the lock (see BatchReportDone).
    function advanceSequence() -> List<Notification>:
        ready = []
        while true:
            slot = slots[nextExpected % len(slots)]

            if not slot.batchReceived:
                return ready

            while not slot.pending.isEmpty():
                ready.append(slot.pending.removeFirst())

            // Reset the slot: the ring reuses this index for a later batch, and a
            // leftover true would let a future advance walk past a batch that never
            // arrived, releasing its notifications early.
            slot.batchReceived = false
            nextExpected++
```

## Auth Config:

- Batch size and timeout
- Max read cache size (bytes)

Batch size and timeout can be determined using parameters sweep.

## Auth Cache:

MFU eviction policy.
Does not need any synchronization. It is only used serially. Only accesses by the auth worker which never spawns sub workers.
this allow high performance and it needed since the auth worker process txs one by one.
While keeping its own internal index separate from the warmup cache, it can share the underlying data (keys and values) since they are immutable.
Keys and values are passes on from the warmup phase along with the input txs from the queue, so we reuse them in the cache without copying them.
Each key have two versions - last-known-version and speculated-version.
If the versions doesn't match, we call this entry a "speculated-write", and it cannot be evicted until the two versions agree again.
This is why we only limit the read cache size.
The speculated-write part needs no explicit limit: it is bounded by the number of txs between auth and commit (all the queues) times writes-per-tx.
That bound relies on last-known only ever catching up to speculated, never passing it - which holds because every commit we are notified of is a write we made ourselves.
The reverse would pin the entry forever, since the two versions could never agree again, so it is a reliable signature of an outside writer.

## Auth Worker:

- Consumes `auth-input-queue`.
  At each loop iteration it acts on the type of item it got.

- **On batch**: it execute the txs one by one.
  For each tx, read keys that are missing from the cache are added from the tx read set first - never lowering a version already there - so a cold cache neither forces a re-execution nor sends us to the query service.
  Then every read key is checked against the cache: the tx's read version must equal the key's speculated-version, which is the version that key holds at this tx's position in the serial order.
  If they all match, the read-write set is accepted as is, and the cache is updated from it.
  For a key that is not a speculated-write the two versions agree, so the test there reads simply as "the read is the committed version" - one comparison, no special case.
  The test cannot pass by accident: a warmup read version is always a committed version, so it is at most the current committed version, which is at most the speculated-version.
  A read that is stale for ANY reason is therefore strictly below it - not just a read that lost a race with one of our own writes.

  Otherwise, it is re executed using the auth phase cache, using the query service as a fallback, just like in the warmup phase using a dedicated connection (not shared).
  The cache is updated during the execution with reads and write and mark them accordingly in the cache.
  When the tx processing is done, we move on to the next tx, and eventually batching them accordingly to the batching rule and adding them to the `submit-input-queue`.

- **On warmup notification**: one item may carry a group of notifications, released together by the sequencer; process them in order.
  Take all the keys/versions and update the last-known-version for these keys according to the data in the notification.

- **On submitter notification**: Take all keys/version-diff and update the speculated-version for these keys. That is, `key.speculated-version -= version-diff`.
  Then, pass the notification to the submitter via the `submit-input-queue`.

At the end of each batch processing, we initiate cache eviction.
Never during execution for optimal performance.

## Submitter Config:

- Input queue high and low thresholds

## Submitter Worker:

The submitter aggregates multiple txs read-write set into a single committer tx (read-write set and the metadata (the original EVM txs)).

The submitter process is as follows:
- Store txs in a sync.map (tx ID → tx and all original txs before the aggregation).
- Batch txs.
- Register the tx in the notification service (before submit to prevent TOCTOU).
- Submit.
- Wait for it to appear in the orderer block delivery (not the committer delivery).
  This is essential to ensure they will be ordered correctly.
- Go on to submit the next batch.

During batching, multiple updates to the same key might be batched together.
This reduces multiple key version updates into a single update.
Also, following reads in the same batch will read the next version.
The first read wins (it will be set as the read version for this batch), and the write version is always that plus 1.

For such cases, the submitter keeps a map from key->version-diff :

`diffMap[key] += versionDiff`

TX received version (read or write) will be decremented by this diff.
Then, a submitter-notification is sent to auth-input-queue.
When the notification is returned back via the `submit-input-queue`, it means that all following
TXs that received from auth will already have a corrected version.
So we fix the map:

`diffMap[key] -= versionDiff`

If `diffMap[key]==0` we can drop the entry.

Batch size: The submitter doesn't have a fixed size batch.
It attempts to submit them one by one, and adapts the batch size according to the load.
That is, if the queue fills up to a high threshold, it increases the batch size, and if it is lower then the low threshold it decrease the batch size.


## Notification Worker:

Process the notification stream, and validate all the txs are committed (issuing rollback otherwise).
When a txs are committed, it collect all the keys/versions/values of these txs from the write sets stored by the submitter.
A notification is therefore a list of committed keys with their committed version AND value.
The value is there for the warmup refresh: a version without its value would attach a fresh version to a stale value, which passes MVCC validation and commits a wrong result. Auth needs only the version, to raise the key's last-known-version.
Then, we add this notification to the `warmup-notification-queue`.
It also removes the tx from the submit map.

#### Notification Timeout:

Every registered tx needs a timeout, because a notification that never arrives is not self-correcting: the tx stays in the submit map forever, its keys never have their last-known-version raised so they stay speculated-writes and can never be evicted from the auth cache, and every tx reading those keys re-executes forever.

On timeout we do NOT assume the worst. We ask the query service for the txID status (GetTransactionStatus takes a list of txIDs, so one call can adjudicate every tx that timed out together):
- COMMITTED: proceed exactly as if the notification had arrived. The keys/versions/values are still in the submit map, so the normal refresh/stamp/forward flow applies.
- Any ABORTED/MALFORMED/REJECTED status: roll back.
- STATUS_UNSPECIFIED means not validated yet, i.e. still in flight. Keep waiting and re-arm the timeout; do not roll back.

The third case is why the status has to be asked for rather than inferred from the timeout alone: a slow commit and a lost notification are indistinguishable from our side, and rolling back a tx that is merely slow costs a re-execution and, if it then commits after all, a spurious rollback of everything that followed it.

## Rollback:

Stage 1 is stop-the-world: a rollback IS a restart. We drop all state and begin again.

Triggered by any abort - an abort notification, or an aborted status from the timeout adjudication.

1. Stop submitting, immediately, and stop forming new batches. One abort usually means the txs behind it abort too - they were validated against its writes - so there is no point pushing any of them.
2. Wait ONLY for the txs already submitted. Those cannot be cancelled, so their outcome has to be learned. The submitter holds one batch at a time and waits for ordered delivery, so this set is small and bounded. A tx whose notification does not arrive is adjudicated by the query service as usual.
3. Collect the EVM txs that still have to run: the aborted ones, plus every tx anywhere in the pipeline that did not commit. Keep the original EVM txs only - never their read-write sets, whose read versions are exactly what turned out to be wrong.
4. Abandon the rest of the pipeline where it stands - do NOT let it finish. Clear everything and start from scratch: every queue, BOTH caches, all in-flight batch state, the per-key version corrections held by the submitter, and the batch sequencing state. Nothing survives the restart except the txs from step 3.
5. Restart, with the step 3 txs going first, ahead of any newly arrived txs.

Correctness comes from the restart itself, not from any rollback-specific reasoning. The only thing that survives is a set of EVM txs that were either never submitted to the orderer or submitted and not committed - which is exactly the process's initial condition. A fresh start is correct by construction, so a restart is too, and there is no partially-rolled-back state to reason about. This is why the caches are cleared wholesale rather than pruned.

This explicitly does NOT preserve the original input order: a tx submitted after an aborted one may commit before it. That is fine under CFT, where the gateway owns the order and is free to re-order on retry. It is precisely what BFT cannot do - there the executors are bound to reproduce the ordered sequence - so this is one of several places the design needs revisiting for BFT.

For contrast, this is what stage 2 will have to solve. A partial rollback cannot appeal to the initial condition: it would have to drop the aborted tx's writes while keeping the writes of earlier txs that are still in flight, and since the cache holds one entry per key, dropping by key erases a key an earlier tx also wrote - so it would have to record WHICH tx wrote each entry and re-apply the survivors in order.

The cost is the abandoned work plus cold caches: whatever was warmed or authed but not yet submitted is thrown away and redone, and the batches after a restart refetch everything from the query service. That is the intended trade - cheaper than pushing txs that are about to abort - but it means this holds up only while aborts are rare. Making rollback incremental is stage 2.

## Future Work:

- **Gas accounting**, to lift the assumption that gas is not counted.
  Keep the accumulator out of the state during warmup and auth: each tx carries its gas as a delta in the internal tx structure - the amount added, not the resulting balance - so neither phase reads or writes it, and deltas commute, leaving nothing to order and nothing to conflict on.
  Only the submitter knows the accumulator's real value and version.
  Per batch it sums the deltas of the txs it aggregates and emits one read-write for the accumulator, the same way it already collapses repeated writes to any other key.
- **Notification head-of-line blocking.**
  A notification waits for every batch numbered at or below its stamp, so one slow warmup batch holds up all notifications: last-known-version stops rising, speculated-writes accumulate and cannot be evicted, and every tx touching them re-executes.
  Correctness only needs a notification to outrank the batches that read one of ITS keys, but matching batches to keys costs bookkeeping and overhead on the hot path, so stage 1 takes the coarse guarantee.
- **Serial fallback reads on re-execution.**
  A re-execution can diverge from the warmup execution and read keys that are not in the warmup read set.
  Those miss the pre-fill and are fetched from the query service inline, on the auth phase's single connection - the exact cost the warmup phase exists to avoid.
  Collecting the misses and re-warming them concurrently instead is left for later.
- **Incremental rollback**, instead of the stop-the-world restart above. See Rollback.
- **A tx that keeps aborting.** Open.
