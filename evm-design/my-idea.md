> Companion design: [oev-executor-design.md](oev-executor-design.md) — full EOV/OEV execution & trust design derived from these notes.

Execute-order-validate (EOV) assumes that collisions are extereamly rare.
Dependent TXs exists, but the probability of having two TXs that have a dependency executed concurrently is extreamly low.
Thus, a EVM gateway acts as an endorser and simulate a TX and submit it vis the exectue-order-validate mechanism.
If there is a MVCC error, it can retry until succeed.
The gateway can also batch TXs from the same user if it submits multiple sequential nonce TXs, and generate
a single RW-set TX, thus eliminating the case when one succeed and the others fail.
Also ensuring they are executed in the correct order.

If the dependency assumption breaks, then it does not matter if the origin of the TXs are EVM or any other source.
The system will fail to process TXs when most TXs have a dependency.
Thus, we must have a order-execture-validate (OEV) fallback.
The gateway or the client can submit a OEV TX.
The endorsers consume the Orderer blocks, fetches only the OEV TXs from each block, then execute all of them one-by-one.
It generates a single RW-set TX (EOV) that have a dependency on global block value (each TX reads it and increment it).
This ensures that the each OEV block TXs are orderered correctly.

Why group the entire block?
We can submit all the EOV TXs one by one to the Orderer.
We must ensure they are committed in order, thus, the RW-set TX order is the same as the OEV TXs.
This is hard to do in a distributed system, so we batch them togather, waiting to receive a batch before submitting the next.
This ensures the order.

Why we can't process the RW-set TXs out of order, and retry only the ones that failed.

Lets assume 3 TXs:

OEV Txs in a block:
TX1: A+=X+B
TX2: X++

External TX (directly via the EOV mechanism, not via the OEV mechanism)
TX3: B++

Let's assume A=B=X=1
If the TX3 arrives last, TX1 and TX2 succeed, then the final value is A=3, X=2, B=2
If TX3 comes first, TX1 fails due to MVCC validation on the version of B.
So after TX3 and TX2 executition, the values are B=2, X=2, A=1.
Then if we retry TX1, the final value is B=2, X=2, A=5, which violates the seriazability principle.
Because there is no order in which TX1 comes before TX2 which results this values.

Thus, we must fail all the following TXs if one fail.
So grouping into batches makes sense for two reasons:
1. Instead of waiting for each TX to be ordered, we wait for a batch (reduces waiting intances)
2. We fail in a lower granularity.

# CFT

Add a CFT option for the EVM.
Use the same two phase solution (run all in parallel to cache the keys, then run them one by one with internal memory).
Offer two options for this mode

Option 1: submit non-dependent TXs in parallel,
then submit the following depepdent batch when we see them in the block (they are ordererd).
But we don't need to process the input TXs (only validate they are committed) as we already pre processed
the RW-set according to the previous TXs.

Option 2: submit the entire batch in a single RW-set, like in the OEV option.
