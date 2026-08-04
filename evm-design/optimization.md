# EVM Design optimization
3 phases

Warmup
Auth
Submit

## Warmup:

#### Config:

batch size and timeout,
Input queue size.
max in flight txs,
Max cache size (bytes).
Max in flight txs should be very large in the begining to allow maximal parallelization.
But can be reduced if resources are exhausted.
Batch size and timeout should be decided using parameters sweep.

#### Cache:

Most frequently used (MFU) eviction policy.
Use sync.Map to maintain the index to allow parallel access from all active warmup workers.
We can use sync here since the bottleneck is the network io anyway, so it won't degrade performance.
Keeps key->value,version mapping.
Key, value, version are immutable. A key update is a new write to the cache using a new value slice.

#### Worker:

Consumes txs from input-queue, batches them, submit in parallel to a batch worker.
Each batch is numbered using an atomic counter: batch-index.
Keeps active in-flight-txs txs atomic counter to maintain the limit, the batch worker decrease the counter when done.

#### Batch worker:

Exexutre all txs in the batch in parallel, utilizing the warmup cache.
Fallback to the query service - no need for views (nil view), using a connecn pool shared between the workers (a single connection can support many quey RPC in parallel from different workers, no sync needed).
When done, push batch to warmup-batch-queue along with the batch number, and the full read write set.
The read write set includes all the content - the key, version, value - not just part of it.
For example, a read-write will include the read version, read value, write version, and write value.
The batch also includes the batch number.
The worker then decrease the in-flight-txs counter.

## Auth:

#### Config:

Batch size and timeout.
Input queue size,
Max read cache size (bytes).
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

#### Worker:

Consumes two inputs (loop over select of two queues)
warmup-batch-queue and notification-queue (explained later).
The notification-queue is first to give it priority to avoid starving it and since it is quick to process and sparse.
At each loop iteration it acts on whatever it got.

**On batch**: it exexutre the txs one by one.
For each tx, it first check if the read set contains keys from the write cache, meaning, they are marked as write in the cache.
If all of its read keys are not in the write cache, then the read-write set is accepted as is. The cache is updated using the existing read-write set, marking the cache entry accordingly (read or write).

Otherwise, it is re executed using the auth phase cache, using the query service as a fallback, just like in the warmup phase using a dedicated connection (not shared).
If there are read-keys of the TX that are not exist in the cache, we add them before executing the TX to avoid fetcing them for the query service.
The cache is updated during the execution with reads and write and mark them accordingly in the cache.
When the tx processing is done, we move on to the next tx, and eventually batching them accordingly to the batching rule and adding them to the submit-queue.

**On notification event**: process the notification.
Notification is a list of committed keys/versions, and a batch number.
We have a separate notification receive worker. It process the notification stream, and validate all the txs are committed (issuing rollback otherwise).
When a txs are committed, it collect all the keys/versions of these txs (stored by the submitter) and register the current value of the batch-number atomic counter.
Then, we add this notification to the notification-queue.
It also removes the tx from the submit map.
The auth worker process put this notification in a notification-map (batch-number → list of keys/versions).
Then, after each batch processing, we maintain the previous-batch-number that all previous batches were processed (included).
If a batch is more than one increment over the previous maximum (can be inserted out of order), then we keep it in a done-batch set.

When a new batch is finish and it is one increment over the previous, we increment as normal, and look for the next batch in the done set, and the next ones and so on until the next can't be found.

Once we finelized the previous-batch-number, we go over all notification items from notification-map starting from the previous-batch-number we started with before the update, up to the one we landed on.
And take all the keys/versions and demote them to read if the version matches the cache.
If it doesn't match, it was overwritten by an active tx.
Removing the notification from the map when it was processed.

At the end of each batch processing, we initate cache eviction.
Never during execution for optimal performance.

## Submitter:

#### Config:

Input queue size, high and low thresholds

#### Worker:

The submitter doesn't have a fixed size batch.
It attempts to submit them on by one, and adopts the batch size according to the load.
That is, if the queue fills up to a high threshold, it increases the bath size, and if it is lower then the low threshold it decrease the batch size.
Batching means aggregating multiple txs read-write set into a single committer tx (read-write set and the metadata (the original EVM txs)).
The submitter process is as follows.
Store txs in a sync.map (tx ID → tx and all original txs before the aggregation).
Batch txs. Register the tx in the notification service (before submit to prevent toctuo).
Submit.
Wait for it to appear in the orderer block delivery (not the committer delivery).
Then, go on to submit the next batch.
This is essential to ensure they will be ordered correctly.
