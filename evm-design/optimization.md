# EVM Design optimization

3 phases:
- Warmup
- Auth
- Submit

## Warmup:

#### Config:

- batch size and timeout
- Input queue size
- max in flight txs
- Max cache size (bytes)

Max in flight txs should be very large in the beginning to allow maximal parallelization.
But can be reduced if resources are exhausted.
Batch size and timeout should be decided using parameters sweep.

#### Cache:

Most frequently used (MFU) eviction policy.
Use sync.Map to maintain the index to allow parallel access from all active warmup workers.
We can use sync here since the bottleneck is the network io anyway, so it won't degrade performance.
Keeps key->value,version mapping.
The warmup cache holds committed state only - never an uncommitted transaction write.
Key, value, version are immutable. A key update is a new write to the cache using a new value slice - either a fill after a query-service read, or a notification refresh carrying an already-committed value.

#### TXs Worker:

Consumes txs from input-queue, batches them, submit in parallel to a batch worker.
Each batch is numbered using an atomic counter: batch-index.
Keeps active in-flight-txs txs atomic counter to maintain the limit, the batch worker decrease the counter when done.

#### Notification Worker:

Consumes the warmup notification-queue (fed by the submitter notification worker).
On a notification it first refreshes the warmup cache: for every committed key ALREADY RESIDENT it updates the value/version in place and keeps the frequency statistics. A key that is not resident is ignored - the next read fetches it fresh anyway - and if a refreshed key is evicted later, that is the MFU policy's decision.
This refresh is the only thing keeping the warmup cache coherent: MFU eviction preferentially RETAINS a hot key, so without it a committed hot key would serve its pre-commit version indefinitely.
A cache fill from the query service must never lower a key's cached version, so a read that races ahead of the query service's commit visibility cannot overwrite a refreshed entry with a stale one.
It then samples the batch-index counter and stamps the notification with the NEXT batch-number (the number the TXs worker will give the next batch it forms), then forwards it to the auth phase notification-queue.
The ORDER of those two steps is what makes the auth watermark sound, and it matters because this worker and the TXs worker run concurrently: the refresh must complete BEFORE the counter is sampled.
Then every batch from the stamped number onward is formed after the refresh and reads the committed value, while every batch below it was already formed and is covered by auth's write-state check.
Sampling the counter first would leave a window where the batch at the watermark is formed and reads the key before the refresh lands - stale, and above the watermark, so auth would accept it.

#### Batch Worker:

Execute all txs in the batch in parallel, utilizing the warmup cache.
Fallback to the query service - no need for views (nil view), using a connection pool shared between the workers (the pool needs no sync of our own; a pool rather than one connection because concurrent reads on a single HTTP/2 connection serialize - a sweep measured 4212 tx/s at 1 connection rising to a ~5040 tx/s plateau at 4-16, regressing at 32).
When done, push batch to warmup-batch-queue along with the batch number, and the full read write set.
The read write set includes all the content - the key, version, value - not just part of it.
For example, a read-write will include the read version, read value, write version, and write value.
The batch also includes the batch number.
The worker then decrease the in-flight-txs counter.

## Auth:

#### Config:

- Batch size and timeout
- Input queue size
- Max read cache size (bytes)

Batch size and timeout can be determined using parameters sweep.

#### Cache:

MFU eviction policy.
Does not need any sync. It is only used serially. Only accesses by the auth worker which never spawns sub workers.
this allow high performance and it needed since the auth worker process txs one by one.
While keeping its own internal index separate from the warmup cache, it can share the underlying data (keys and values) since they are immutable.
Keys and values are passes on from the warmup phase along with the input txs from the queue, so we reuse them in the cache without copying them.
In addition, each key have a state - read or write.
If it is a write, it cannot be evicted until demoted to a read.
This is why we only limit the read cache size.
The write half needs no explicit limit: it is bounded by the number of txs between auth and commit (auth-queue + submit-queue + submitted-but-not-yet-notified) times writes-per-tx.

#### Auth Worker:

Consumes two inputs (loop over select of two queues).
warmup-batch-queue and notification-queue (the latter fed by the warmup notification worker).
Note that a plain select over both queues does NOT honour case order - Go picks uniformly at random among the ready cases - so if the notification-queue is to be served first, it needs a nested select: try it with a default, and only then block on both.
At each loop iteration it acts on whatever it got.

**On batch**: it execute the txs one by one.
For each tx, it first check if the read set contains keys from the write cache, meaning, they are marked as write in the cache.
If all of its read keys are not in the write cache, then the read-write set is accepted as is. The cache is updated using the existing read-write set, marking the cache entry accordingly (read or write).

Otherwise, it is re executed using the auth phase cache, using the query service as a fallback, just like in the warmup phase using a dedicated connection (not shared).
If there are read-keys of the TX that are not exist in the cache, we add them before executing the TX to avoid fetching them from the query service.
The cache is updated during the execution with reads and write and mark them accordingly in the cache.
When the tx processing is done, we move on to the next tx, and eventually batching them accordingly to the batching rule and adding them to the submit-queue.

**On notification**: process the notification.
It was built by the submitter notification worker, then refreshed into the warmup cache and stamped with a batch-number by the warmup notification worker; the auth phase uses only its keys/versions.
The auth worker process put this notification in a notification-map (batch-number → list of keys/versions).
Then, after each batch processing, we maintain the previous-batch-number that all previous batches were processed (included).
If a batch is more than one increment over the previous maximum (can be inserted out of order), then we keep it in a done-batch set.

When a new batch is finish and it is one increment over the previous, we increment as normal, and look for the next batch in the done set, and the next ones and so on until the next can't be found.

Once we finalized the previous-batch-number, we go over all notification items from notification-map starting from the previous-batch-number we started with before the update, up to the one we landed on.
And take all the keys/versions and demote them to read if the version matches the cache.
If it doesn't match, it was overwritten by an active tx.
Removing the notification from the map when it was processed.

This watermark is what makes the accept-as-is fast path safe, and it is why the stamped number is the warmup phase's NEXT batch-number at the moment it refreshed the committed keys in its cache.
Every batch numbered BELOW it was formed before that refresh, so it may carry a pre-commit read of a committed key: holding the demotion until all of them have been authed guarantees each is validated while the key is still marked write, so it re-executes against the auth cache instead of accepting its stale warmup read.
Every batch numbered AT OR ABOVE it was formed after the refresh, so it reads the key at its committed version and needs no such protection.
Both halves are required. Without the warmup refresh, a batch above the watermark still reads the pre-commit version, and since the key is demoted to read by then, the fast path accepts that stale read version and the committer aborts it - repeatably, because the retry re-warms from the same never-refreshed entry.

At the end of each batch processing, we initiate cache eviction.
Never during execution for optimal performance.

## Submitter:

#### Config:

- Input queue size, high and low thresholds

#### Submit Worker:

The submitter doesn't have a fixed size batch.
It attempts to submit them one by one, and adapts the batch size according to the load.
That is, if the queue fills up to a high threshold, it increases the batch size, and if it is lower then the low threshold it decrease the batch size.
Batching means aggregating multiple txs read-write set into a single committer tx (read-write set and the metadata (the original EVM txs)).
The submitter process is as follows.
Store txs in a sync.map (tx ID → tx and all original txs before the aggregation).
Batch txs. Register the tx in the notification service (before submit to prevent TOCTOU).
Submit.
Wait for it to appear in the orderer block delivery (not the committer delivery).
Then, go on to submit the next batch.
This is essential to ensure they will be ordered correctly.

#### Notification Worker:

We have a separate notification receive worker.
It process the notification stream, and validate all the txs are committed (issuing rollback otherwise).
When a txs are committed, it collect all the keys/versions/values of these txs from the read-write sets stored by the
submitter.
A notification is therefore a list of committed keys with their committed version AND value; the batch-number field is stamped later by the warmup notification worker.
The value is there for the warmup refresh: a version without its value would attach a fresh version to a stale value, which passes MVCC validation and commits a wrong result. The auth demotion needs only the version.
Then, we add this notification to the WARMUP phase notification-queue - not directly to the auth phase.
It also removes the tx from the submit map.

#### Notification Timeout:

Every registered tx needs a timeout, because a notification that never arrives is not self-correcting: the tx stays in the submit map forever, its writes are never demoted so they can never be evicted from the auth cache, and every tx reading those keys re-executes forever.

On timeout we do NOT assume the worst. We ask the query service for the txID status (GetTransactionStatus takes a list of txIDs, so one call can adjudicate every tx that timed out together):
- COMMITTED: proceed exactly as if the notification had arrived. The keys/versions/values are still in the submit map, so the normal refresh/stamp/demote flow applies.
- Any ABORTED/MALFORMED/REJECTED status: roll back.
- STATUS_UNSPECIFIED means not validated yet, i.e. still in flight. Keep waiting and re-arm the timeout; do not roll back.

The third case is why the status has to be asked for rather than inferred from the timeout alone: a slow commit and a lost notification are indistinguishable from our side, and rolling back a tx that is merely slow costs a re-execution and, if it then commits after all, a spurious rollback of everything that followed it.

## Rollback:

Not designed yet. This records only what it has to handle.

The warmup cache needs nothing: it is refreshed only from committed notifications, so a rolled-back batch never entered it.

The auth cache does. The rolled-back writes must be dropped, and they are findable because they are still marked write - never demoted, since they never committed.
Any later tx that was validated against one of those writes is void too.
For the TXs we probably do not have to compute that set: the later tx's recorded read version never materialized, so the committer aborts it too and its own notification reports it - the cascade is discovered, not derived.
For the CACHE we do have to. The cache holds one entry per key, so dropping a rolled-back write by key erases the key even when an earlier still-in-flight tx also wrote it, and the fast path then starts serving a committed value that the earlier write should still shadow. An entry therefore has to record WHICH tx wrote it, and rollback has to re-apply the surviving in-flight writes in order rather than only delete.
Affected txs then re-enter the warmup input-queue and are re-warmed from scratch. Re-entry need not preserve any position: a same-sender successor drained ahead of them is excluded as nonce-too-high and stays pending, so the order self-corrects at the cost of a cycle.

Open: what to do with a tx that keeps aborting.
