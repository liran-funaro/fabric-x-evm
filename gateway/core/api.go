/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cmn "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/notification"
)

type Signer interface {
	Sign(msg []byte) ([]byte, error)
	Serialize() ([]byte, error)
}

type Submitter interface {
	Submit(context.Context, sdk.Endorsement) error
	Close() error
}

var logger = flogging.MustGetLogger("gateway.core")

// Gateway is the component that bridges Fabric-x and the EVM. Its API is the
// Ethereum JSON RPC. When the user submits a transaction targeting an Ethereum
// contract, the gateway requests endorsement from a set of EVM endorsers. It then
// submits a signed transaction with the read/writeset to the Fabric orderers.
type Gateway struct {
	batchSubmitter *BatchSubmitter
	endorsers      *EndorsementClient
	store          Store
	chainID        *big.Int
	// cache is the cross-batch VersionedCache shared with the endorser engines'
	// read path (see gateway/core.NewCachedSnapshotter). Exactly one instance
	// per gateway; nil until Task 4 wires the executor to write through it, at
	// which point every read stays neutral until ApplyWrites is first called.
	cache           *VersionedCache
	ChainConfig     *params.ChainConfig
	Signer          types.Signer
	pending         *PendingPool
	arrivals        chan struct{} // non-blocking wake-up for the idle executor loop
	wg              sync.WaitGroup
	stopOnce        sync.Once
	endorsementChan chan sdk.Endorsement // Channel to send endorsements to BatchSubmitter

	// Pipelined commit tracking (see executor.go). The executor submits a
	// batch and advances immediately -- it does NOT block on commit. inflight
	// is the ordered registry of submitted-but-unconfirmed batches (oldest
	// first); each carries the included tx hashes and the merged RWS it wrote
	// to the cache, so resolveInflight can finalize (commit) or roll back
	// (abort/timeout) and a later cascade (Task 5) can rebuild the cache from
	// the survivors. inflightSlots is a counting semaphore bounding how many
	// batches may be outstanding (see SetMaxInflight): the executor acquires a
	// slot before submitting and resolveInflight releases it.
	inflightMu    sync.Mutex
	inflight      []*inflightBatch
	inflightSlots chan struct{}
	maxInflight   int

	// maxInflightObserved is the peak len(inflight) ever recorded by
	// trackInflight; pure test observability (see MaxInflightObserved). atomic so
	// the accessor can read it without taking inflightMu.
	maxInflightObserved atomic.Int64 // peak len(inflight); test observability (MaxInflightObserved)
	// cascadeCount counts real rollback cascades (a suffix actually detached in
	// cascadeFrom), not the idempotent not-found no-ops; pure test observability
	// (see CascadeCount).
	cascadeCount atomic.Int64 // number of real cascade events; test observability (CascadeCount)

	// commit-latency observability (see CommitLatencyStats): submit->commit-
	// notification wall time, accumulated by resolveInflight over committed
	// batches only. Always-on and cheap (one timestamp per batch in trackInflight
	// + atomic adds here), like maxInflightObserved. Read alongside
	// MaxInflightObserved vs the in-flight cap, this answers whether the commit
	// path is on the critical path (window saturated) or hidden behind execution
	// (window never fills) -- the gap the ENDORSE-TIMING execution split cannot see.
	commitCount        atomic.Int64 // committed batches observed
	commitLatencyNanos atomic.Int64 // sum of submit->commit latency (ns) over commitCount
	commitLatencyMax   atomic.Int64 // max single submit->commit latency (ns)

	// commitTimeout backstops each in-flight batch: if no commit/abort
	// notification arrives within it, resolveInflight treats the batch as
	// unconfirmed (rolled back) so a lost notification cannot wedge the
	// in-flight window forever. Defaulted by New to commitTimeoutDefault;
	// tests may override it. When a per-TxID notifier is wired (see notifier),
	// the notifier owns the timeout and this AfterFunc backstop is not armed
	// (see trackInflight); it remains the sole backstop on the unwired path.
	commitTimeout time.Duration

	// notifier, when non-nil, resolves each in-flight batch by its real per-TxID
	// commit/abort/timeout status from the Fabric-X sidecar notification stream
	// (register-then-submit; see txNotifier and SetNotifier). Watch(txID) feeds
	// it. nil on the block-sync / unwired path (and in core unit tests), where
	// Watch is a no-op and the trackInflight AfterFunc is the backstop.
	notifier *txNotifier

	// committedVersion reads a key's committed monotonic version from the source
	// of truth the endorsers read (the query-service view in query-service mode).
	// ok is false if the key has no committed value. It backs queryCommitStatus,
	// the notifier's sidecar-timeout fallback. nil ⇒ the fallback is unavailable
	// and every timeout is treated as "not committed" (conservative rollback ->
	// cascade -> MVCC self-corrects). Set once at wiring time (see
	// SetCommittedVersionReader), before Start, so it is never written
	// concurrently with the executor goroutine's reads.
	committedVersion func(ctx context.Context, key string) (version uint64, ok bool, err error)

	// maxBatchSize bounds how many pending txs one drain cycle folds into a
	// single merged committer tx (see executeCycle / PendingPool.DrainUpTo).
	// 0 means unbounded -- the pure drain-all model, which assumes arrival is
	// paced upstream (e.g. by the query service on the submit path). Set a
	// positive bound via SetMaxBatchSize when submission can burst faster than
	// the drain cycle (so a burst is not swallowed into one oversized Fabric
	// tx that exceeds the orderer's max message size). Atomic so it can be set
	// safely while the executor goroutine is running.
	maxBatchSize atomic.Int64

	// pipelined selects the warm(N+1) ‖ auth(N) pipelined executor loop
	// (runExecutorPipelined) over the serial one (runExecutor's default). It
	// overlaps the I/O-bound concurrent warm pass of the next batch with the
	// CPU-bound serial authoritative pass of the current one, collapsing the
	// per-batch wall from warm+auth to max(warm,auth)+boundary. Default false
	// (serial); it is a boot config flag (gateway YAML `pipelined:`, plumbed via
	// SetPipelined before Start). Read once at Start, so it is never written
	// concurrently with the executor goroutine.
	//
	// IDENTICAL IN EFFECT TO SERIAL -- but only within one batch boundary.
	// auth(N) reopens warm(N+1)'s now-stale snapshot onto a FRESH committed view
	// (see AuthMergedBatch / ReopenableReadStore), so authoritative read-versions
	// match committed state and the old stale-read MVCC abort cascade is gone for
	// reads that resolve within the pipeline's depth-1 window.
	//
	// The naive pipeline also paid an extra auth-phase READ cost on hot-key
	// traffic: hot keys served from the in-flight write-cache during warm(N+1)
	// were evicted the moment batch N committed, forcing auth(N+1) to re-fetch
	// them from the query service -- a new miss in the auth phase. Two mechanisms
	// remove it so auth(N+1) reads batch N's writes from the write cache
	// regardless of eviction timing: committed writes are held one extra boundary
	// (VersionedCache.DrainEvictionsDeferred) AND auth replays a frozen per-batch
	// snapshot of the warm-pass write-set as a read fallback below the live cache
	// (VersionedCache.SnapshotEntries -> cachedView.warmWrites).
	//
	// RESIDUAL (depth-1): both mechanisms extend exactly ONE boundary, so a hot
	// key written by batch N, NOT rewritten by N+1, and read by N+2 falls outside
	// the window -- auth(N+2) then reads a version that can be stale vs committed
	// and aborts. Immaterial when hot keys are rewritten every batch; on
	// conflict-heavy traffic with SMALL batches it reopens the abort livelock.
	//
	// Measured on ec2 (32-core, full stack, 20000-tx replay, batch size 128-4096):
	// synthetic (conflict-free) is a WIN at every batch size (1.17-1.49x, all
	// 20000/20000, 0 rb). Historic (hot-key USDC) LIVELOCKS at bs<=256 (e.g. bs=128:
	// 975/20000 committed, 2682 rolled-back batches, 16 tx/s) and is a clean win at
	// bs>=512 (1.04-1.28x); at bs=1024 both commit 20000/20000, 0 rollbacks (~1.25x).
	// Default stays false (serial) as the conservative baseline; if enabled, keep
	// bs>=512 (see report/pipeline_report.html). See SetPipelined.
	pipelined bool
}

type Store interface {
	BlockNumber(ctx context.Context) (uint64, error)
	BlockNumberByHash(ctx context.Context, hash []byte) (*uint64, error)
	LatestBlock(ctx context.Context, full bool) (*domain.Block, error)
	GetBlockByNumber(ctx context.Context, num uint64, full bool) (*domain.Block, error)
	GetBlockByHash(ctx context.Context, hash []byte, full bool) (*domain.Block, error)
	GetBlockTxCountByHash(ctx context.Context, hash []byte) (int64, error)
	GetBlockTxCountByNumber(ctx context.Context, num uint64) (int64, error)
	GetTransactionByHash(ctx context.Context, hash []byte) (*domain.Transaction, error)
	GetTransactionByBlockHashAndIndex(ctx context.Context, hash []byte, idx int64) (*domain.Transaction, error)
	GetTransactionByBlockNumberAndIndex(ctx context.Context, num uint64, idx int64) (*domain.Transaction, error)
	GetLogs(ctx context.Context, filter domain.LogFilter) ([]domain.Log, error)
	GetLogsByTxHash(ctx context.Context, txHash []byte) ([]domain.Log, error)
}

// New creates a new Ethereum Gateway.
// batchSubmitter handles all endorsement submissions and is owned by the Gateway.
// endorsementChan is the channel to send endorsements to the BatchSubmitter.
// cache is the cross-batch VersionedCache shared with this gateway's endorser
// engines (the SAME instance passed to their cacheWrap -- see
// endorser/app.NewEndorserCore); the caller wiring endorsers and the gateway
// together owns creating it exactly once.
func New(ec *EndorsementClient, batchSubmitter *BatchSubmitter, store Store, chainID int64, endorsementChan chan sdk.Endorsement, cache *VersionedCache) (*Gateway, error) {
	cid := big.NewInt(chainID)
	return &Gateway{
		endorsers:       ec,
		batchSubmitter:  batchSubmitter,
		store:           store,
		chainID:         cid,
		ChainConfig:     cmn.BuildChainConfig(chainID),
		Signer:          types.LatestSignerForChainID(cid),
		pending:         NewPendingPool(),
		arrivals:        make(chan struct{}, 1),
		endorsementChan: endorsementChan,
		inflightSlots:   make(chan struct{}, defaultMaxInflight),
		maxInflight:     defaultMaxInflight,
		commitTimeout:   commitTimeoutDefault,
		cache:           cache,
	}, nil
}

// SetMaxInflight bounds how many submitted-but-unconfirmed committer txs the
// pipelined executor keeps outstanding before it stops draining new batches.
// n <= 0 restores the default (defaultMaxInflight). Call before Start -- it
// re-sizes the semaphore, which is not safe once the executor is running.
func (g *Gateway) SetMaxInflight(n int) {
	if n <= 0 {
		n = defaultMaxInflight
	}
	g.maxInflight = n
	g.inflightSlots = make(chan struct{}, n)
}

// SetCommitTimeout sets the per-in-flight-batch backstop: how long trackInflight
// waits for a commit/abort before resolving the batch (rollback in the unwired
// path, or the notifier's fallback in query-service mode -- SetNotifier(...,0)
// derives its client backstop from this value). d <= 0 restores commitTimeoutDefault.
// Call before Start.
func (g *Gateway) SetCommitTimeout(d time.Duration) {
	if d <= 0 {
		d = commitTimeoutDefault
	}
	g.commitTimeout = d
}

// SetMaxBatchSize bounds how many pending txs a single drain cycle folds into
// one merged committer tx. n <= 0 restores the unbounded drain-all default.
// Safe to call at any time (the field is atomic); it takes effect on the next
// drain cycle. See the maxBatchSize field and executeCycle.
func (g *Gateway) SetMaxBatchSize(n int) {
	g.maxBatchSize.Store(int64(n))
}

// SetPipelined selects the pipelined executor loop (warm(N+1) overlapped with
// auth(N)) when p is true, or the serial loop (default) when false. It is a boot
// config flag (gateway YAML `pipelined:`); call before Start -- the flag is read
// once when the executor goroutine launches and must not change while it runs.
// The serial path is byte-identical either way at the submit boundary (both go
// through submitBatch).
//
// auth reopens warm's stale snapshot onto a fresh committed view (see
// execution.AuthMergedBatch) and replays batch N's writes for one boundary, so
// the pipeline is identical in effect to serial for reads within that depth-1
// window: the earlier stale-read rollback livelock is fixed and the follow-on
// auth-phase refetch cost removed (committed writes held one boundary + a frozen
// warm-write snapshot replayed in auth; see the pipelined field comment). The
// coverage is NOT unconditional, though -- a hot key written by N, skipped by
// N+1, read by N+2 falls outside depth-1, so on conflict-heavy traffic with
// small batches the abort livelock returns. Measured on ec2 (20000-tx replay,
// batch size 128-4096): synthetic wins at every batch size (1.17-1.49x);
// historic livelocks at bs<=256 and is a clean win at bs>=512 (1.04-1.28x; at
// bs=1024 both 20000/20000, 0 rollbacks, ~1.25x). Default stays false; if
// enabled, keep bs>=512. See the pipelined field comment and
// report/pipeline_report.html.
func (g *Gateway) SetPipelined(p bool) {
	g.pipelined = p
}

// SetNotifier wires per-TxID commit resolution. It builds the gateway's
// txNotifier over subscribe (the register channel the notification Subscribe
// loop reads) with the query-service fallback (queryCommitStatus) and
// resolveInflight, stores it so Watch(txID) registers each submitted batch
// before submit, and returns it as the notification.TxStatusHandler the caller
// feeds to notification.NewProcessor. timeout is the client-side backstop for a
// dead stream; timeout <= 0 uses commitTimeout. Call before Start.
func (g *Gateway) SetNotifier(subscribe chan<- []string, timeout time.Duration) notification.TxStatusHandler {
	if timeout <= 0 {
		timeout = g.commitTimeout
	}
	if timeout <= 0 {
		timeout = commitTimeoutDefault
	}
	g.notifier = newTxNotifier(subscribe, timeout, g.queryCommitStatus, g.resolveInflight)
	return g.notifier
}

// SetCommittedVersionReader injects the committed-version reader backing the
// notifier's sidecar-timeout fallback (see the committedVersion field and
// queryCommitStatus). Wired only in query-service mode; left unset (nil)
// elsewhere, which makes the fallback a conservative rollback. Call before Start.
func (g *Gateway) SetCommittedVersionReader(fn func(ctx context.Context, key string) (version uint64, ok bool, err error)) {
	g.committedVersion = fn
}

// Watch registers txID with the per-TxID notifier so its real commit/abort/timeout
// outcome resolves the in-flight batch (register-then-submit -- called from
// executeCycle before SubmitFabricTx). No-op when no notifier is wired (block-sync
// / unwired path and core unit tests), where the trackInflight timeout backstop
// resolves the batch instead.
func (g *Gateway) Watch(txID string) {
	if g.notifier != nil {
		g.notifier.Watch(txID)
	}
}

// MaxInflightObserved returns the peak number of simultaneously outstanding
// (submitted-but-unresolved) committer batches seen so far. Test observability:
// a value > 1 proves the pipelined executor submitted a later batch before an
// earlier one was resolved (it did not serialize on commit).
func (g *Gateway) MaxInflightObserved() int { return int(g.maxInflightObserved.Load()) }

// CascadeCount returns how many rollback cascades actually detached a suffix.
// Test observability: 0 means no in-flight batch was rolled back.
func (g *Gateway) CascadeCount() int { return int(g.cascadeCount.Load()) }

// CommitLatencyStats returns the number of committed committer batches observed
// and the average and maximum submit->commit-notification wall time across them
// (zero durations when count is 0). This is the commit-path cost the endorser's
// ENDORSE-TIMING split cannot see; read with MaxInflightObserved vs MaxInflight
// to tell whether commit is on the critical path (peak in-flight == cap) or
// hidden behind execution (peak < cap). Lock-free observability.
func (g *Gateway) CommitLatencyStats() (count int, avg, max time.Duration) {
	n := g.commitCount.Load()
	if n == 0 {
		return 0, 0, 0
	}
	return int(n), time.Duration(g.commitLatencyNanos.Load() / n), time.Duration(g.commitLatencyMax.Load())
}

// MaxInflight returns the configured in-flight window size (the committer-batch
// backpressure cap; see SetMaxInflight). Test observability, paired with
// MaxInflightObserved and CommitLatencyStats.
func (g *Gateway) MaxInflight() int { return g.maxInflight }

// InflightLen returns the number of currently outstanding (submitted-but-unresolved)
// committer batches. Test observability.
func (g *Gateway) InflightLen() int {
	g.inflightMu.Lock()
	defer g.inflightMu.Unlock()
	return len(g.inflight)
}

// queryCommitStatus is the notifier's fallback: it decides whether the in-flight
// batch for txID has committed by comparing its written keys' committed versions
// (from the injected committedVersion reader) against the spec versions the cache
// predicted at submit time. The batch committed iff EVERY written key reached its
// spec version (committedVersion >= specVersion): all keys of a batch commit
// atomically in one Fabric tx, and Fabric-X versions are per-key monotonic, so a
// committed version >= the spec version means this (earliest-uncommitted) writer's
// write landed. On any read error, a missing key, an unknown txID, or a nil reader
// it returns false (conservative rollback -> cascade -> MVCC self-corrects).
//
// Note on spec == 0: a first-write-of-an-absent-key has spec version 0, so
// committedVersion >= 0 is trivially true for that key and cannot by itself
// distinguish committed from uncommitted. This is acceptable only under the RMW
// invariant that every written key is also read first and the sender's nonce
// write is always present with spec > 0 -- so a batch's commit decision never
// rests on a spec==0 key alone. spec==0 keys are intentionally NOT rejected
// here: doing so would force a rollback for every batch that creates a new key,
// which is over-conservative.
func (g *Gateway) queryCommitStatus(txID string) bool {
	// Copy the batch's spec versions out under the lock; the batch's specVers map
	// is never mutated after trackInflight records it, but copy anyway so we never
	// touch registry-owned memory once the lock is released.
	g.inflightMu.Lock()
	var specVers map[string]uint64
	found := false
	for _, b := range g.inflight {
		if b.txID == txID {
			specVers = make(map[string]uint64, len(b.specVers))
			for k, v := range b.specVers {
				specVers[k] = v
			}
			found = true
			break
		}
	}
	g.inflightMu.Unlock()

	if !found {
		return false // already resolved / not one of our batches
	}
	if g.committedVersion == nil {
		return false // no live query service -> conservative rollback
	}
	if len(specVers) == 0 {
		// Defensive: an empty spec set makes the loop below vacuously "committed",
		// which would drop the batch's txs without a cascade -- the unrecoverable
		// direction. Roll back conservatively instead. Unreachable in the EVM
		// workload (every tx bumps the sender nonce, so specVers is non-empty), but
		// fallback safety must not silently depend on that.
		return false
	}

	// queryCommitStatus runs synchronously inside txNotifier.Handle, which the
	// notification Processor invokes SERIALLY -- a hung query call would stall
	// the entire notification stream. Bound it to the gateway's existing
	// commit-timeout backstop so it can never block longer than a batch's
	// stall backstop.
	timeout := g.commitTimeout
	if timeout <= 0 {
		timeout = commitTimeoutDefault
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for key, spec := range specVers {
		v, ok, err := g.committedVersion(ctx, key)
		if err != nil || !ok || v < spec {
			return false
		}
	}
	return true
}

// Start launches the single drain-all executor goroutine (see executor.go).
// There is no worker pool: one batch is in flight at a time, and its size is
// however many txs were pending when the cycle started (bounded by
// maxBatchSize if set; see SetMaxBatchSize).
func (g *Gateway) Start(ctx context.Context) {
	g.wg.Add(1)
	go g.runExecutor(ctx)
}

// SendTransaction runs geth-style pre-flight validation, then adds the tx to
// the pending pool for the executor loop to pick up on its next drain.
// Mirrors eth_sendRawTransaction's failure model.
func (g *Gateway) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	if err := ValidateTx(ctx, tx, g.ChainConfig, g.Signer, g); err != nil {
		return err
	}
	if g.pending.Has(tx.Hash()) {
		return domain.ErrTransactionAlreadyPending
	}
	g.AddPending(tx)
	return nil
}

// AddPending adds tx directly to the pending pool and wakes the idle executor
// if needed, skipping SendTransaction's pre-flight validation (including
// nonce checks) and duplicate rejection -- PendingPool.Add itself is already
// a safe no-op for a hash that's already pending. Exported for callers that
// must bypass that validation, e.g. testimpl.NonceBypassGateway for
// wrap-around replay scenarios where the same signed transactions are
// resubmitted and normal nonce validation would reject them.
func (g *Gateway) AddPending(tx *types.Transaction) {
	g.pending.Add(tx)
	select {
	case g.arrivals <- struct{}{}:
	default: // executor is already awake (busy or already notified); don't block
	}
}

// CallContract is a query. It doesn't require a signature of the end user and doesn't change the ledger or nonce.
// We requests endorsement from a single endorser, return the payload, and discard the signed response.
// This is the same way queries are handled in Fabric.
func (g *Gateway) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return g.endorsers.CallContract(ctx, call, blockNumber)
}

// ExecuteEthTx requests endorsements for the submitted ethereum-style transaction.
func (g *Gateway) ExecuteEthTx(ctx context.Context, tx *types.Transaction) (sdk.Endorsement, error) {
	return g.endorsers.ExecuteTransaction(ctx, tx)
}

// SubmitFabricTx submits a Fabric envelope via the BatchSubmitter.
func (g *Gateway) SubmitFabricTx(ctx context.Context, end sdk.Endorsement) error {
	// Send endorsement to BatchSubmitter via channel
	select {
	case g.endorsementChan <- end:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context canceled while sending endorsement: %w", ctx.Err())
	}
}

// ChainID returns the configured chainID for this deployment.
func (g *Gateway) ChainID(ctx context.Context) (*big.Int, error) {
	return g.chainID, nil
}

// BlockNumber is the current blockheight as observed by this gateway.
func (g *Gateway) BlockNumber(ctx context.Context) (uint64, error) {
	return g.store.BlockNumber(ctx)
}

// BlockNumberByHash resolves a block hash to a block number.
func (g *Gateway) BlockNumberByHash(ctx context.Context, hash common.Hash) (*uint64, error) {
	return g.store.BlockNumberByHash(ctx, hash.Bytes())
}

// GetBlockByNumber returns the block at the specified number.
// If full is true, the block includes transactions.
// num == math.MaxUint64 means "latest".
func (g *Gateway) GetBlockByNumber(ctx context.Context, num uint64, full bool) (*domain.Block, error) {
	if num == math.MaxUint64 {
		return g.store.LatestBlock(ctx, full)
	}
	return g.store.GetBlockByNumber(ctx, num, full)
}

// GetBlockByHash returns block metadata based on the block hash.
// If full is true, the block includes transactions.
func (g *Gateway) GetBlockByHash(ctx context.Context, hash common.Hash, full bool) (*domain.Block, error) {
	return g.store.GetBlockByHash(ctx, hash.Bytes(), full)
}

// GetBlockTxCountByHash counts the transactions in a specific block.
func (g *Gateway) GetBlockTxCountByHash(ctx context.Context, hash common.Hash) (int64, error) {
	return g.store.GetBlockTxCountByHash(ctx, hash.Bytes())
}

// GetBlockTxCountByNumber counts the transactions in a specific block.
func (g *Gateway) GetBlockTxCountByNumber(ctx context.Context, num uint64) (int64, error) {
	return g.store.GetBlockTxCountByNumber(ctx, num)
}

// State

// BalanceAt returns the balance of an account.
func (g *Gateway) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	return g.endorsers.BalanceAt(ctx, account, blockNumber)
}

func (g *Gateway) StorageAt(ctx context.Context, account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	return g.endorsers.StorageAt(ctx, account, key, blockNumber)
}

func (g *Gateway) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return g.endorsers.CodeAt(ctx, account, blockNumber)
}

func (g *Gateway) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	return g.endorsers.NonceAt(ctx, account, blockNumber)
}

// Transactions

// TransactionByHash retrieves transaction data from either the pending pool or database.
// It first checks if the transaction is pending (accepted but not yet committed),
// then queries the database for committed transactions.
//
// Return values represent three possible states:
// - State 1 (Pending): Transaction in the pending pool → tx with BlockNumber=0 (becomes null in JSON)
// - State 2 (Completed): Transaction in database → tx with block data populated
// - State 3 (Not Found): Transaction in neither location → nil
//
// The pending status is signaled by BlockNumber=0, which the API layer converts to null.
func (g *Gateway) TransactionByHash(ctx context.Context, hash common.Hash) (*domain.Transaction, error) {
	// Check if the transaction is pending (accepted but not yet committed).
	if pendingTx, ok := g.pending.Get(hash); ok {
		// Transaction is pending - return it with zero block fields
		// The API layer will convert these to nil in the JSON response
		rawTx, err := pendingTx.MarshalBinary()
		if err != nil {
			return nil, err
		}

		from, err := types.Sender(g.Signer, pendingTx)
		if err != nil {
			return nil, err
		}

		var toAddr []byte
		if to := pendingTx.To(); to != nil {
			toAddr = to.Bytes()
		}

		return &domain.Transaction{
			TxHash:      hash.Bytes(),
			BlockHash:   nil, // nil signals pending to API layer
			BlockNumber: 0,   // 0 signals pending to API layer
			TxIndex:     0,   // Value doesn't matter - API layer checks BlockNumber==0 for pending
			RawTx:       rawTx,
			FromAddress: from.Bytes(),
			ToAddress:   toAddr,
		}, nil
	}

	// Transaction not pending, check database for committed transaction
	tx, err := g.store.GetTransactionByHash(ctx, hash.Bytes())
	if err != nil {
		return nil, err
	}
	if tx == nil {
		// Transaction not found pending or in the database
		return nil, nil
	}

	// Fetch logs for the transaction (needed for receipts)
	logs, err := g.store.GetLogsByTxHash(ctx, hash.Bytes())
	if err != nil {
		return nil, err
	}
	tx.Logs = logs

	// Transaction found in database
	return tx, nil
}

// GetTransactionByBlockHashAndIndex retrieves a transaction based on block hash in the transaction index in that block.
func (g *Gateway) GetTransactionByBlockHashAndIndex(ctx context.Context, hash common.Hash, idx int64) (*domain.Transaction, error) {
	return g.store.GetTransactionByBlockHashAndIndex(ctx, hash.Bytes(), idx)
}

// GetTransactionByBlockNumberAndIndex retrieves a transaction based on block number in the transaction index in that block.
func (g *Gateway) GetTransactionByBlockNumberAndIndex(ctx context.Context, num uint64, idx int64) (*domain.Transaction, error) {
	return g.store.GetTransactionByBlockNumberAndIndex(ctx, num, idx)
}

func (g *Gateway) GetLogs(ctx context.Context, query domain.LogFilter) ([]domain.Log, error) {
	return g.store.GetLogs(ctx, query)
}

// Stop performs an orderly shutdown of the gateway. It waits for the executor
// loop to finish, then stops and closes the batch submitter.
//
// The executor loop (runExecutor) only exits once the ctx passed to Start is
// canceled, so the caller must cancel that ctx before (or concurrently with)
// calling Stop; otherwise Stop blocks forever in wg.Wait(). The existing
// caller (gateway/app) already does this: it cancels the context and only
// then calls Stop from its shutdown path.
func (g *Gateway) Stop() error {
	var err error
	g.stopOnce.Do(func() {
		// Wait for the executor goroutine to finish.
		g.wg.Wait()

		// Stop and close batch submitter
		g.batchSubmitter.Stop()
		err = g.batchSubmitter.Close()
	})

	return err
}
