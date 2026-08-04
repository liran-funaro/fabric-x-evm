Context of the Discussion: EVM Gateway on Fabric-X (Updated)

Objective

The user is design-engineering an Ethereum (EVM) gateway to sit on top of Fabric-X. The architectural goal is to map the account-based, EVM-driven state paradigm into the high-performance, single-channel, namespace-driven environment of Fabric-X, maximizing compatibility without compromising execution performance.

User Persona & Background


• Core maintainer of the Fabric-X Committer..
• High expertise in blockchain architectures, decoupled execution pipelines, transaction dependency graphs, and state commitment mechanics..
• Current Focus: Learning and analyzing how alternative systems (Solana/Neon EVM, LayerZero/Zero, and QMDB) approach parallel execution and state translation to apply those lessons to this gateway design..
Core Technical Vectors Addressed

1. Ethereum Protocol Mechanics & Client Trust

• Client Lifecycle: Transactions originate from end-user wallets (e.g., MetaMask), are cryptographically signed locally (via ECDSA), and sent to a single Execution Client (EL) endpoint via JSON-RPC (eth_sendRawTransaction). The network propagates the payload via a P2P gossip mesh (hashes + full block uploads).
• The Nonce Constraint: Ethereum world state explicitly stores an incremental transaction counter (nonce) per account. It enforces strict contiguous sequence ($nonce_{next} = nonce_{current} + 1$).
Low nonces are rejected to prevent replay attacks.
High nonces are held/queued in the mempool to enforce deterministic order of stateful smart contract execution.
• Trust Boundaries: The RPC endpoint is an untrusted router. Because payloads are locally signed, a malicious endpoint cannot alter transaction parameters (destination, value, data) without breaking the cryptographic signature. However, a malicious node can execute drop attacks (censorship) or front-running/MEV sandwich attacks. Production configurations mitigate drop attacks via parallel broadcasting (Promise.allSettled or fallback transports) to multiple independent node endpoints simultaneously.
2. Architectural Clashes: Ethereum vs. Fabric-X


• State Trees: Ethereum relies on a global Merkle Patricia Trie (MPT) committed directly in the block header, generating high disk I/O amplification. Fabric-X leverages a high-throughput namespace/flat database layout..
• Ordering vs. Concurrency: Ethereum transactions are inherently sequential per account due to nonces. Fabric-X achieves ultra-high throughput via optimistic parallel processing and transaction dependency graphs..

3. Reference Solutions Being Evaluated


• Solana (Neon EVM): Solves the ordering issue entirely at the stateful Gateway Proxy layer. The proxy maintains a mempool queue, dry-runs/simulates transactions locally to discover state read/write sets, and sequentializes them before translating them into native Solana transactions..
• LayerZero (Zero Network / QMDB): Solves the performance and ordering bottleneck at the engine layer via the FAFO (First Action, First Out) Scheduler and QMDB (Quick Merkle Database). The scheduler builds deterministic dependency graphs statically from incoming blocks, running conflicting same-account nonces single-file on a single thread while scattering independent accounts across multiple cores. QMDB optimizes the state trie by using an append-only structure and processing Merkleization entirely in-memory within fixed-size immutable subtrees (twigs)..

Current Architecture Blueprint for the Gateway

To implement the Fabric-X EVM Gateway without choking the core committer's parallel capability, the current design consensus relies on a two-tier strategy:


1. Stateful Gateway Proxy Layer: An edge component exposing standard Ethereum JSON-RPC endpoints. It tracks pending nonces per account in memory. If a client transmits a high nonce out-of-order, the proxy buffers it locally, releasing it to Fabric-X only when the preceding transaction safely clears..
2. Fabric-X Transaction Graph Mapping: When passing transactions down to the Fabric-X orderers/committers, the gateway explicitly flags dependencies via the internal Fabric-X dependency graph. Transactions from the same Ethereum address form a linear dependency chain ($Tx_{n} \rightarrow Tx_{n+1}$), while transactions from different accounts remain decoupled to scale horizontally across Fabric-X parallel committers..

Imported from: Gemini

# EVM contention
No. There is no common global key or centralized counter that gets mutated by every single transaction on Ethereum.

From a state machine perspective, transactions that modify completely disjoint sets of accounts have no conceptual data contention. If Alice sends 1 ETH to Bob, and Charlie sends 1 ETH to David, their target storage slots, account balances, and nonces are entirely isolated.

However, Ethereum behaves as if there is total contention because of historical protocol design choices regarding execution and block structures.

1. Why Ethereum Behaves Sequentially (The Virtual Contention)

Even though there isn't a single database key causing a lock, Ethereum enforces strict global sequential execution due to two major architectural constraints:

A. The EVM Execution Model (Single-Threaded Abstraction)

The standard Ethereum Virtual Machine (EVM) architecture processes a block's transaction payload line by line, one after another. When a validator executes a block, the state transitions occur sequentially ($S_0 \rightarrow S_1 \rightarrow S_2$).

Because standard Solidity/EVM transactions do not declare which state keys they will read or write ahead of time, a node cannot know if Transaction 2 will conflict with Transaction 1 until it actually runs Transaction 1. Rather than hazarding race conditions or complex runtime scheduling, standard execution clients (like Geth) process the block on a single thread.

B. The Global Merkle Patricia Trie (MPT) Root

This is the closest thing to a "global key update" bottleneck, though it happens at the block level, not per individual transaction:

Every block header must contain a single stateRoot hash representing the root of the entire World State trie.


• To calculate this root, the client must execute all transactions in the block. .
• Even if Transaction 1 and Transaction 2 touch completely different accounts, their modifications mutate different leaves of the exact same global Trie..
• The node must recalculate the cryptographically bound parent hashes all the way up to the single global root node. .
So while the transactions don't contend with each other during compute if isolated, they all converge on the same data structure modification at the end of the block production cycle.

2. Real Contentions: True Shared State

There are elements in Ethereum that do create hot keys and massive transaction contention across the network:


• EIP-1559 Base Fee: Every block evaluates the total gas used in the previous block to dynamically adjust a global variable called the baseFeePerGas. While this changes per block (not per transaction), it applies a uniform, global state variable change that alters the validity parameters of all incoming transactions..
• Smart Contract Hot Spots (e.g., Uniswap Pools, Stablecoins): If 5,000 users are trying to swap tokens on the exact same Uniswap V3 pool at the exact same time, they are all contending to read and modify the same persistent storage slots (liquidity, tick indices, asset balances) of that single contract address. In this scenario, true state contention occurs, forcing strict sequential ordering based on the tips (priority fees) paid to the block builder..
The Monad / Speculative Execution Shift (Parallel EVM)

Because there is no theoretical key conflict for unrelated transactions, modern engineering projects (like Monad or Aptos's Block-STM) have proven that you can eliminate this sequential constraint while retaining full EVM compatibility.

They use Optimistic Concurrency Control (OCC) / Speculative Execution:


1. A multi-core node executes all transactions in a block in parallel on separate threads, assuming no conflict..
2. During execution, it records the exact input/output memory footprint (read/write sets) for each transaction..
3. If the engine detects that Transaction 3 tried to read a storage slot that Transaction 1 was simultaneously modifying, it flags a conflict, rolls back Transaction 3, and re-executes it sequentially once Transaction 1 finishes..
What This Means for Your Fabric-X Gateway

Since Fabric-X already utilizes an optimization engine via transaction dependency graphs, you are structurally closer to a Parallel EVM environment than a standard sequential Ethereum client.

For your gateway:


• Do not introduce an artificial global lock..
• Allow transactions from separate Ethereum addresses targeting separate smart contract namespaces to stream into Fabric-X concurrently..
• Only enforce strict ordering rules when incoming transactions share a matching Origin Account (Nonce tracking) or point to the exact same Smart Contract Storage Namespace..

# Gas
Yes, you have hit on the exact reason why decoupled execution and high-performance committer pipelines (like Fabric-X or parallel EVMs) require a post-mortem state approach to block tracking.

In Ethereum, there are two distinct types of block gas variables, and understanding how they interact directly answers your question.

1. The Block Gas Limit vs. The Block Gas Used

The Block Gas Limit (The Pre-Execution Cap)

The Block Gas Limit is a fixed ceiling ($30\text{ million gas}$) established before a block is executed.


• It represents the absolute maximum processing budget allocated for the block..
• Block proposers pull transactions from the mempool and add their individual gasLimit parameters together. The moment the cumulative sum of transaction gasLimits reaches the block's cap, the builder stops adding transactions..
• Therefore, the maximum possible work boundary is known upfront..
The Block Gas Used (The Post-Mortem Metric)

The Block Gas Used is the exact metric you are describing. It can only be calculated post-mortem, after the block has been entirely processed and committed.

A transaction's gasLimit is just a user-defined safety cap (e.g., "I am willing to spend up to 200,000 gas on this swap"). The actual compute might only consume 120,000 gas. Furthermore, EVM execution can trigger gas refunds (e.g., clearing out an existing storage slot via SELFDESTRUCT or SSTORE), which are calculated as the call stack unwinds at the very end of transaction processing.

Therefore, the final block.gasUsed recorded in the block header is strictly a running total compiled after the execution engine settles the state changes.

2. Managing Block Gas in Fabric-X with Invalidated/Resubmitted TXs

Because Fabric-X uses parallel execution engines and evaluates transaction dependency graphs, transactions might abort due to state conflict (optimistic concurrency failure) or missing namespace dependencies. If they fail validation at commit time, they must be evicted from the batch, rescheduled, or re-executed.

If you are constructing an Ethereum-compatible block out of a Fabric-X commit batch, your gateway must handle the gas accounting through this exact post-mortem lifecycle:


[Fabric-X Committer Layer]
           │
           ▼ (Parallel multi-core processing execution)
┌────────────────────────────────────────────────────────┐
│  Tx 1 (Success: 50k gas)                               │
│  Tx 2 (State Conflict! -> Evicted/Rescheduled)        │
│  Tx 3 (Success: 120k gas)                              │
└────────────────────────────────────────────────────────┘
           │
           ▼ (Commit Phase complete)
[Gateway Post-Mortem Aggregator]
   └── Calculates: Block Gas Used = 50k + 120k = 170k gas
   └── Generates: Merkle State Root & Final Block Header
The Gateway Post-Mortem Protocol Steps:


1. The Ingestion Cap: The gateway tracks the maximum declared gasLimit of transactions sent to Fabric-X to ensure it doesn't accidentally ingest more raw work than the block envelope can hold..
2. The Execution Delta: Fabric-X parallel threads process the transactions. If Tx_2 encounters a conflict, it is dropped or pushed to the next block cycle..
3. The Rollup & Summation: Once the committer successfully seals the batch of valid transactions, the gateway queries the final execution receipts. It sums up the actual gas consumed only by the transactions that successfully survived the commitment phase..
4. Header Generation: The gateway writes this aggregated sum to the gasUsed field of the mock Ethereum block header and calculates the final EIP-1559 base fee adjustments for the next block based on this exact post-mortem calculation..
This aligns perfectly with your parallel architecture: execution and state updates dictate the gas properties, rather than an arbitrary static prediction.
