1. Remove any references to other documents. The reader should get a full context without having to read these documents. If there was a requirment in the other documents that was addressed in the doc, then formalized this requirement in the begining of the doc, and refer to it later.
2. If there is a diviation from the source documents, you can add a author notes in a new document that explain the diviation. No need to address it in the doc. If there is a need, you can mention the original intent as an rejected alternative, or to explain why alternative approch is not needed or fails.
3. Assume the target audience is not EVM proficient. They are Fabric-X experts, but know close to nothing on EVM. When a new subject introduced, it needs to be explained.
4. There is no need to go into deep-dive details for the trust model. We can mention it is the same as Fabric-X, and decided using the MSP rule. The OEV might tuch multiple namespaces, thus it will need to satisfy all the namespaces endorsement policies. This is fine since all endorsers will executre the OEV TXs.
5. I'm not sure on the gas section since I'm not an EVM expert. We should ensure we only claim sure items, not maybes. We can say that the gas is calculated post-mortem for monitoring. Maybe EVM can reject TXs that used too much gas, but it cannot reject TXs that will be the one that overuse the gas.
Retries is addressed, but the issue is that they consume enegergy for no fault of the submitter, so why should it pay for it?
The final conclustion that since we use trusted enviroment (all users are known), we should only monitor gas, but not enforce it.
6. Nonce sequantial - We should say more about merging sequantial user TXs with sequantial nonce. The user submits multiple TXs with sequantial nonce. If the EVM batches them in time (via some kind of batching mechanism - for example like in the query service), it can create a single EVM TX which describes all of these TXs. It will contain them as is (not modify them), but store them as a single TX to be executed sequantially without interleaving TXs.
This can be executed EOV or OEV. If EOV, then the endorser will execute it and produce a single RW-set for all of these EVM TXs.
If OEV, then the post-ordering-executor (which is essentially also an endorser), can execute will execute them.
It will be safe since they are bundled as a single Fabric-X TX so there won't be any interleaving TXs and they
are in the correct order.
Question that should be addressed: is there any bookeeping per TX that this bundeling can interfeer with?
If so, we can add a sub index for the TX: #blk, #tx-idx, #sub-tx-idx.
7. Merkle tree build: it is built post-morten when the blocks are fully committed. Does this make sence?
