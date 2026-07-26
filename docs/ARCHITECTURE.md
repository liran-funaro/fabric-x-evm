# Architecture Overview

## Table of Contents

- [Architecture Overview](#architecture-overview)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Key Design Decisions](#key-design-decisions)
    - [Full Ethereum Ecosystem Compatibility](#full-ethereum-ecosystem-compatibility)
    - [MVCC Validation for Transaction Consistency](#mvcc-validation-for-transaction-consistency)
    - [State reads and synchronization](#state-reads-and-synchronization)
    - [Gas as Metering, Not Payment](#gas-as-metering-not-payment)
    - [Fabric Ordering Service Instead of PoW/PoS](#fabric-ordering-service-instead-of-powpos)
  - [Core Components](#core-components)
    - [Gateway](#gateway)
    - [Endorser](#endorser)
  - [Data Flow](#data-flow)
    - [Transaction Execution Flow](#transaction-execution-flow)
  - [State Management](#state-management)
    - [SnapshotDB: The EVM-Fabric Bridge](#snapshotdb-the-evm-fabric-bridge)
    - [Synchronization Architecture](#synchronization-architecture)
    - [Storage Layer](#storage-layer)
  - [Performance, Throughput, and Concurrency Management](#performance-throughput-and-concurrency-management)
    - [Overview](#overview)
    - [Concurrency Challenges](#concurrency-challenges)
      - [MVCC Conflicts and Goodput](#mvcc-conflicts-and-goodput)
    - [Scaling Strategy: Phased Approach](#scaling-strategy-phased-approach)
      - [Phase 1: Single Gateway (One Node, One Organization)](#phase-1-single-gateway-one-node-one-organization)
      - [Phase 2: Gateway Replicas (One Organization, Multiple Nodes)](#phase-2-gateway-replicas-one-organization-multiple-nodes)
      - [Phase 3: Multi-Organization Deployment](#phase-3-multi-organization-deployment)
      - [Phase 4: BFT Deployment](#phase-4-bft-deployment)

## Introduction

Fabric-EVM makes Hyperledger Fabric-x and Fabric compatible with the Ethereum ecosystem by embedding an Ethereum Virtual Machine (EVM) that executes Solidity smart contracts within Fabric's permissioned environment. This integration combines the rich Ethereum tooling and contract ecosystem with Fabric's robust endorsement and consensus model.

The system enables developers to deploy and invoke existing Ethereum contracts without modification, using familiar tools like MetaMask, Web3.js, and Hardhat. Externally, the system exposes standard Ethereum JSON-RPC endpoints, making it indistinguishable from an Ethereum node. Internally, however, all transactions follow Fabric's endorsement, ordering, and MVCC validation flow, ensuring enterprise-grade governance and security.

**Key Benefits**:
- **Ethereum Compatibility**: Deploy Solidity contracts and use existing Ethereum tooling
- **Fabric Security**: Leverage Fabric's permissioned model, endorsement policies, and BFT consensus
- **High Performance**: Pre-order execution with parallel transaction processing where state doesn't conflict
- **Enterprise Governance**: Fabric's identity and access control layer on top of EVM execution

## Key Design Decisions

### Full Ethereum Ecosystem Compatibility

**API Compatibility**: The system exposes standard Ethereum JSON-RPC endpoints (eth_*, net_*, web3_*), making it fully compatible with existing Ethereum tooling. Clients like MetaMask, Web3.js, Ethers.js, and Hardhat can interact with Fabric-EVM without any modifications. This preserves the entire Ethereum developer experience while running on Fabric infrastructure.

**Bytecode Compatibility**: The system uses an unmodified `go-ethereum` EVM implementation, ensuring 100% compatibility with Solidity-compiled bytecode. All Ethereum hard forks are enabled from block 0, supporting the full range of EVM opcodes and precompiles. Contracts deployed on Ethereum can be deployed on Fabric-EVM without recompilation or modification.

### MVCC Validation for Transaction Consistency

Fabric's Multi-Version Concurrency Control (MVCC) ensures transaction consistency without requiring global locks. During endorsement, each transaction captures a read-write set with version information for all accessed keys. At commit time, Fabric validates that all read versions still match the current ledger state. If another transaction modified any read key, the transaction fails validation and is marked invalid.

This approach enables high concurrency: transactions touching disjoint state can execute in parallel and commit successfully. Only transactions with overlapping read-write dependencies face potential conflicts, which can be mitigated through retry logic or dependency tracking.

### State reads and synchronization

Endorsers keep **no local world state**. Instead of following committed blocks into a local
database, an endorser reads the keys it needs on demand from the Fabric-X **query service** under a
pinned, consistent (SERIALIZABLE) view. This removes the sync-lag/staleness of a locally-mirrored
state DB — reads always see the authoritative latest committed state — and eliminates the
per-endorser state synchronizer entirely.

The system therefore runs a single synchronizer:

- **Gateway Synchronizer**: Indexes committed blocks into SQLite for efficient historical queries
  via Ethereum RPC endpoints. (Endorsers no longer run a synchronizer; they read via the query
  service.)

> **Protocol support**: because the query service is Fabric-X infrastructure (and its read wire
> format carries only the Fabric-X per-key monotonic version), this design targets **Fabric-X
> only**. The plain-`fabric` protocol is not supported.

### Gas as Metering, Not Payment

Gas is tracked during EVM execution to bound computation and prevent runaway contracts, but **no fees are charged**. Gas serves purely as a metering mechanism to enforce per-transaction limits and protect the network from resource exhaustion. This aligns with Fabric's permissioned model where participants are known and trusted, eliminating the need for economic incentives found in public blockchains.

Configurable gas limits can be set at the channel or application level to control resource usage while maintaining deterministic execution.

### Fabric Ordering Service Instead of PoW/PoS

Unlike Ethereum's Proof-of-Work or Proof-of-Stake consensus, Fabric-EVM uses Fabric's **crash fault-tolerant (CFT) or Byzantine fault-tolerant (BFT) ordering service**. This provides:

- **Deterministic Finality**: Transactions are final once committed; no probabilistic confirmation delays
- **High Throughput**: Pre-order execution allows parallel endorsement before ordering
- **Permissioned Consensus**: Only authorized orderers participate in consensus
- **Fast Block Times**: Blocks are produced based on configuration (time or transaction count), not mining difficulty

The ordering service establishes a total order across all transactions, which are then validated and committed by peers according to their endorsement policies.

## Core Components

### Gateway

The Gateway (located at `gateway/`) serves as the primary entry point for clients, exposing an Ethereum-compatible JSON-RPC API.

- **API Layer** (`gateway/api/`): Implements Ethereum JSON-RPC endpoints (eth_*, net_*, web3_*)
- **Core** (`gateway/core/`): Orchestrates endorsement, transaction submission, and block processing
- **Storage** (`gateway/storage/`): SQLite-based persistence for blocks, transactions, and logs
- **Synchronizer**: Continuously fetches committed blocks from Fabric peers, parses them, and indexes EVM transactions

**Key Responsibilities**:
- Accept Ethereum transactions via JSON-RPC
- Coordinate endorsement from multiple endorsers
- Submit endorsed transactions to Fabric orderers
- Synchronize and index committed blockchain data
- Serve blockchain queries (blocks, transactions, logs, state)

### Endorser

The Endorser (located at `endorser/`) simulates EVM transaction execution and produces signed endorsements. It mirrors the Gateway's layering, plus one extra split — `execution` is kept independent of `core`/`api` so it can be reused without pulling in endorsement or signing:

- **API** (`endorser/api/`): The published `Service` contract a Gateway calls (`ProcessEVMTransaction`, `ProcessCall`, `ProcessStateQuery`). Today `core.Endorser` implements it in-process; a future gRPC client/server pair implements it over the wire without changing the contract.
- **Core** (`endorser/core/`): The `Endorser` type — turns a proposal into a signed `ProposalResponse`, classifying execution outcomes (OK, revert, client error, server error) into Fabric response statuses.
- **Execution** (`endorser/execution/`): `EVMEngine`/`Executor` (EVM instantiation and execution) and `StateDB`/`DualStateDB` (the SnapshotDB that captures read-write sets), plus `BatchExecutor` (the two-phase warm/authoritative engine for ordered batches). Depends only on the `ReadStore`/`KVSSnapshotter` ports it defines — never on `core`, `api`, or `query` — so it can run standalone (see below).
- **Query** (`endorser/query/`): the state read path. A `QueryClient` (`BeginView`/`GetRows`/`EndView`) with a gRPC implementation over the Fabric-X query service (production) and an in-memory implementation (tests); a `View` implements `execution.ReadStore` and a `Store` implements `execution.KVSSnapshotter`, so the engine reads through it unchanged.
- **Storage** (`endorser/storage/`): `LightKVS`/`RevertibleLightKVS` (in-memory versioned KV) — used to back the in-memory query client and the Hardhat snapshot/revert test RPCs.
- **Config** (`endorser/config/`): Per-endorser configuration and validation.
- **App** (`endorser/app/`): Composition root wiring config → query store → execution → core for a single endorser; reused by both the embedded (gateway+endorser) and standalone launch modes.
- **No state synchronizer**: the endorser does not follow committed blocks. It reads current committed state on demand from the query service under a pinned view.

**Key Responsibilities**:
- Execute EVM transactions against versioned state snapshots
- Track state reads/writes as Fabric read-write sets
- Synchronize ledger state for accurate simulation
- Handle three proposal types:
  - `ProposalTypeEVMTx`: State-changing transactions
  - `ProposalTypeCall`: Read-only contract calls (eth_call)
  - `ProposalTypeState`: Direct state queries (balance, code, storage, nonce)
- Return signed proposal responses for endorsement

## Data Flow

### Transaction Execution Flow

```
Client (Metamask/Web3)
    ↓ [Ethereum Transaction via JSON-RPC]
Gateway API
    ↓ [Parse & Validate]
Gateway Core (EndorsementClient)
    ↓ [Create Fabric Proposal]
Endorser(s)
    ↓ [Simulate EVM Execution]
EVMEngine → SnapshotDB
    ↓ [Capture Read-Write Set]
Endorser API
    ↓ [Sign Proposal Response]
Gateway Core
    ↓ [Collect Endorsements]
Fabric Orderer
    ↓ [Order & Distribute]
Fabric Peers (Commit)
    ↓ [MVCC Validation & Write to Ledger]
    ↓
    ├─→ Endorser Synchronizer(s)
    │       ↓ [Fetch Committed Blocks]
    │       ↓ [Update Ledger State]
    │   Endorser VersionedDB
    │
    └─→ Gateway Synchronizer
            ↓ [Fetch Committed Blocks]
            ↓ [Parse & Extract EVM Transactions]
        Gateway Storage (SQLite)
```

As the diagram shows, the transaction execution flow includes the following key steps:

1. **Client Submission**: A client (e.g., MetaMask) signs an Ethereum transaction and submits it via JSON-RPC to the Gateway. The transaction contains the contract address, ABI-encoded function call, and sender's signature.

2. **Gateway Validation**: The Gateway API validates the Ethereum transaction signature, extracts the sender address, and verifies the transaction format.

3. **Proposal Creation**: The Gateway Core creates a Fabric SignedProposal containing:
   - Proposal type indicator (`ProposalTypeEVMTx`)
   - Serialized Ethereum transaction bytes
   - Fabric channel, namespace, and version metadata
   - Random Fabric nonce for uniqueness

4. **Endorsement Request**: The Gateway forwards the proposal to configured endorsers (typically multiple for fault tolerance and policy satisfaction).

5. **EVM Simulation**: Each endorser:
   - Deserializes the Ethereum transaction
   - Validates the Ethereum signature and extracts `msg.sender`
   - Checks that the transaction nonce matches the sender's ledger nonce (replay protection)
   - Creates an isolated EVM execution context with current block metadata
   - Instantiates a fresh SnapshotDB pointing to the current ledger state

6. **State Access During Execution**: As the EVM executes:
   - **Reads** (`SLOAD`): SnapshotDB fetches values from the Fabric ledger at the current block height, recording each read with its version for MVCC validation
   - **Writes** (`SSTORE`): SnapshotDB accumulates changes in memory without persisting, building the write set
   - **Read-Your-Writes**: SnapshotDB returns in-memory writes for subsequent reads within the same transaction, ensuring EVM semantics

7. **Read-Write Set Capture**: After execution completes, the endorser:
   - Extracts the complete read-write set from SnapshotDB
   - Captures any EVM logs emitted during execution
   - Increments the sender's nonce in the write set
   - Packages the result (return value, logs, rwset) into a Fabric ProposalResponse

8. **Endorsement Signing**: The endorser signs the ProposalResponse with its Fabric identity, creating a cryptographic endorsement that commits to the execution result.

9. **Endorsement Collection**: The Gateway collects responses from all endorsers, verifying that:
   - All endorsers produced identical read-write sets (deterministic execution)
   - The endorsement policy is satisfied (e.g., majority of endorsers)

10. **Transaction Assembly**: The Gateway packages the proposal and endorsements into a Fabric transaction envelope, signs it with its own identity, and submits it to the ordering service.

11. **Ordering**: The Fabric orderer sequences the transaction into a block along with other transactions, establishing a total order across the network.

12. **Commit and Validation**: Fabric peers receive the ordered block and:
    - Validate endorsement signatures and policy compliance
    - Perform MVCC validation: check that all read versions match current ledger state
    - If validation passes, apply the write set to the ledger
    - If validation fails (e.g., due to concurrent conflicting transaction), mark the transaction as invalid

13. **State Synchronization**: After commit:
    - **Endorser Synchronizers** fetch the new block and update their local VersionedDB, ensuring future simulations see the latest state
    - **Gateway Synchronizer** fetches the block, parses EVM transactions and logs, and indexes them in SQLite

14. **Client Response**: The Gateway detects the transaction's commit status and returns an Ethereum-style receipt to the client, including transaction hash, status, gas used, and emitted logs.

## State Management

State management in Fabric-EVM operates across multiple layers, each serving a distinct purpose in the transaction lifecycle. The architecture separates execution-time state access from persistent storage, enabling both accurate EVM simulation and efficient historical queries.

### SnapshotDB: The EVM-Fabric Bridge

SnapshotDB is a custom implementation of the go-ethereum `StateDB` interface that replaces Ethereum's native Merkle Patricia Trie with Fabric's versioned ledger. This component is critical for maintaining EVM semantics while capturing the read-write sets required for Fabric's MVCC validation.

**Key Characteristics**:

- **Versioned Reads**: When the EVM executes an `SLOAD` opcode, SnapshotDB fetches the value from Fabric's ledger at a specific block height, recording both the value and its version number. This version tracking is essential for MVCC validation at commit time.

- **In-Memory Writes**: `SSTORE` operations accumulate changes in memory without touching the ledger. This allows the EVM to execute speculatively while building a complete write set that will only be persisted after successful endorsement and ordering.

- **Read-Your-Writes Semantics**: Within a single transaction, subsequent reads return in-memory writes rather than ledger values. This ensures that EVM contracts behave correctly when they read state they've just modified, preserving standard Ethereum execution semantics.

- **Isolation**: Each transaction simulation creates a fresh SnapshotDB instance, ensuring complete isolation between concurrent endorsements. Multiple endorsers can simulate different transactions in parallel without interference.

**State Key Mapping**: EVM state is mapped to Fabric ledger keys using the following deterministic formats:
```
acc:<address>:bal      # Account balance
acc:<address>:nonce    # Account nonce
acc:<address>:code     # Contract code
str:<address>:<slot>   # Storage slot
```
where `<address>` is the hex-encoded Ethereum address and `<slot>` is the hex-encoded 32-byte storage slot index. This mapping ensures that all endorsers reconstruct identical read-write sets for the same transaction.

**Note**: Fabric transaction metadata (inputs and events) will be added to the read-write set using keys like `input/<fabric-tx-id>` and `event/<fabric-tx-id>` to ensure they are included in the transaction's state dependencies.

### Synchronization Architecture

The endorser read path and the gateway index play complementary roles in keeping the network consistent:

**1. Endorser reads (query service, no synchronizer)**

Endorsers hold no local state and run no synchronizer. For each endorsement they:

- **Open a view**: `BeginView` pins a consistent (SERIALIZABLE) snapshot of the latest committed state on the query service
- **Read on demand**: `GetRows` fetches only the keys the transaction touches, each with its per-key MVCC version, into an in-view cache (a key is fetched at most once)
- **Simulation accuracy**: reads always reflect the authoritative latest committed state (the query service reads the committer database directly), so read-write sets carry current versions and pass MVCC validation — with no sync-lag window
- **Release**: `EndView` closes the view when the endorsement completes

Because there is no locally-mirrored state to fall behind, the staleness that a synced VersionedDB would introduce is eliminated.

**2. Gateway Synchronizer**

The gateway runs a single synchronizer focused on indexing rather than execution:

- **Block Parsing**: Fetches committed blocks and extracts EVM-specific data (transactions, receipts, logs)
- **Transaction Indexing**: Stores transaction metadata in SQLite, enabling fast lookups by hash, block number, or sender address
- **Log Indexing**: Indexes EVM logs by contract address and topics, supporting efficient `eth_getLogs` queries with complex filters
- **Historical Queries**: Provides the data layer for all Ethereum RPC endpoints that query historical blockchain state

The gateway's synchronizer is optimized for query performance rather than execution, using relational database indexes to support the diverse query patterns required by Ethereum clients.

### Storage Layer

The gateway maintains a local SQLite database that serves as the query backend for Ethereum RPC endpoints:

**Schema Design**:
- **Blocks Table**: Stores block metadata (number, hash, parent hash, timestamp) with indexes on both number and hash for fast lookups
- **Transactions Table**: Contains full transaction details including Ethereum hash, Fabric transaction ID, status, sender/recipient addresses, and raw transaction bytes
- **Logs Table**: Indexes EVM event logs with separate columns for up to 4 topics, enabling efficient filtering by event signature and indexed parameters

This separation of concerns—VersionedDB for execution, SQLite for queries—allows each component to optimize for its specific workload without compromise.

## Performance, Throughput, and Concurrency Management

### Overview

Fabric-EVM's performance characteristics differ fundamentally from traditional Ethereum due to Fabric's execute-order-validate (EOV) paradigm. While Ethereum follows an order-execute model where transactions are sequenced before execution, Fabric-EVM simulates transactions in parallel before ordering, enabling significantly higher throughput when transactions don't conflict. However, this approach introduces concurrency challenges that must be carefully managed to ensure both high performance and Ethereum client compatibility.

**Key Design Tenet**: The EVM Gateway must not break Ethereum clients. It must remain fully compatible with Ethereum's APIs, their behavior, and failure model. In particular, because "MVCC conflict" does not exist in Ethereum RPC semantics, surfacing MVCC failures to clients would be a breaking change. The gateway masks such conflicts by owning the order (below) and re-executing on the rare abort.

### Execution model: drain-all two-phase merged batches

The gateway does not run a fixed worker pool or a read/write dependency graph. Instead:

- **Pending pool.** `eth_sendRawTransaction` validates a transaction and adds it to an in-memory pending pool — no dependency detection, no per-transaction ordering.
- **Drain-all loop.** A single executor goroutine repeatedly drains the *entire* pending pool as one batch. Execution concurrency therefore equals the batch size and self-tunes to load; there is no worker-count knob.
- **Two-phase execution.** For each batch, a parallel *warm* pass runs every transaction against one pinned query-service view (so all state reads are issued concurrently and the query service batches them), then a serial *authoritative* pass re-runs them in order so a later transaction observes earlier ones' writes, producing one merged read/write-set.
- **One atomic commit per batch.** The batch commits as a single Fabric transaction (envelope `ProposalTypeEVMBatch`) whose metadata carries every EVM transaction unchanged; the committer performs one MVCC check and one commit for the whole batch. Throughput is therefore measured in EVM transactions per second — one committer transaction carries many.
- **Commit tracking and rollback.** The executor waits for that committer transaction's outcome (delivered by the notification service / committed blocks, keyed by Fabric transaction ID): on success the batch's transactions are dropped from the pending pool; on an MVCC abort they are returned to the pool and re-drained. A transaction whose nonce exceeds its sender's current nonce is excluded from the batch and left pending until its predecessor fills the gap.
- **Addressing.** Each EVM transaction in a merged batch is individually addressable as `(block number, transaction index, sub-index)` with its own receipt; the block indexer assigns a flat, contiguous `transactionIndex` per EVM transaction for Ethereum-client compatibility.

The phased scaling roadmap below predates this model and is retained as historical context; the drain-all two-phase design supersedes its "dependency manager" and worker-pool milestones.

### Concurrency Challenges

#### MVCC Conflicts and Goodput

Hyperledger Fabric's Multi-Version Concurrency Control (MVCC) validates transactions at commit time by checking that all read versions still match the current ledger state. When multiple transactions access overlapping state:

1. **Parallel Simulation**: Transactions are simulated concurrently against the same state snapshot
2. **Sequential Ordering**: The ordering service establishes a total order
3. **MVCC Validation**: Only the first transaction commits successfully; subsequent transactions with overlapping reads fail validation

This creates a fundamental tension: **throughput** (total transactions processed) can be high, but **goodput** (successfully committed transactions) may be low under contention. A naive implementation that blindly retries failed transactions wastes resources and degrades performance.

**Example Scenario**: Two transactions both read and modify the same ERC-20 token balance. Both simulate successfully in parallel, but after ordering, only the first commits. The second fails MVCC validation and must be retried, doubling the work for that transaction.

### Scaling Strategy: Phased Approach

The system's concurrency management evolves through multiple phases, each building on the previous to increase throughput and decentralization.

#### Phase 1: Single Gateway (One Node, One Organization)

**Goal**: Throughput equals goodput through intelligent conflict avoidance.

**Milestones**:

1. **Correct Nonce Handling**: Implement ledger-tracked nonces with endorser-side validation to support retries and out-of-order submissions
2. **Basic Single-Node Functionality**:
   - In-memory mempool (transaction queue) absorbing transactions from `eth_sendRawTransaction`
   - Retry loop to mask MVCC conflicts from clients
   - Immediate return to clients with transaction hash
3. **High-Throughput Enablement**:
   - In-memory dependency manager tracking read-write dependencies between queued transactions
   - Endorse and submit only when previous dependent transactions commit
   - Optimal scheduling to minimize conflicts
4. **Tuning and Advanced Features**:
   - Retry for endorsements that fail because endorsers are temporarily out of sync
   - Handle client bursts (multiple sequential nonces submitted rapidly)
   - Handle same-client transaction sequencing
   - Support replacement transactions (same nonce, different payload or gas)
   - Backpressure and mempool queue management to prevent unbounded growth

**Benchmarking Metrics**:
- End-to-end throughput (transactions per second)
- Latency (time from submission to commit)
- Goodput rate (successful commits / total attempts)
- Synchronization lag (endorser/gateway ability to keep up with ledger evolution)

#### Phase 2: Gateway Replicas (One Organization, Multiple Nodes)

**Goal**: High throughput through horizontal scaling while maintaining consistency.

**Milestones**:

1. **Preparation**: Persist mempool to survive node crashes and enable cross-replica visibility
2. **Basic Multi-Node Deployment**:
   - Instantiate dependency manager as a shared service
   - All gateway replicas connect to the centralized dependency manager
   - Replicas collaborate and share load without duplicating work:
     - Coordinated endorsement collection
     - Coordinated transaction submission
   - Dependency manager acts as the mechanism for load distribution
3. **Tuning and Advanced Features**:
   - Speculative execution: emit batches of transactions rather than single dependency-free transactions
   - Streaming behavior for dependency manager to reduce latency

**Benchmarking**: Re-evaluate Phase 1 workload, expecting improvements in throughput and resilience.

#### Phase 3: Multi-Organization Deployment

**Goal**: Decentralization while maintaining high throughput.

**Milestones**:

1. **Basic CFT Deployment**:
   - Multiple dependency managers, one per organization
   - Distributed mempool (e.g., over etcd or directly with ARMA)
   - Basic dependency collection:
     - Nodes coordinate over etcd
     - Each transaction in mempool gets read-write set for dependency graph construction
     - All nodes consult local dependency manager and are aware of dependencies
     - One node per transaction is tasked to collect endorsements and submit
     - Dependency manager uses "blocks" to avoid non-determinism
     - Work assignment protocol to avoid duplication (e.g., which gateway is responsible for a transaction? What if it doesn't make progress?)
2. **Tuning and Advanced Features**:
   - Speculative execution across organizations
   - Handle `TxBatch` abstraction for deterministic sequencing of related transactions

**Benchmarking**: Re-evaluate same workload, expecting unchanged or only slightly lower throughput compared to Phase 2 (trading some performance for decentralization).

#### Phase 4: BFT Deployment

**Goal**: Byzantine fault tolerance for maximum decentralization.

**Approach**: To be determined, likely using ARMA (Atomic Replication with Majority Agreement) for consensus across organizations.

**Benchmarking**: Re-evaluate same workload, expecting unchanged or only slightly lower throughput compared to Phase 3 (trading some performance for decentralization).