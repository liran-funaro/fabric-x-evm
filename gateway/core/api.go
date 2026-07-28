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

	// commitTimeout backstops each in-flight batch: if no commit/abort
	// notification arrives within it, resolveInflight treats the batch as
	// unconfirmed (rolled back) so a lost notification cannot wedge the
	// in-flight window forever. Defaulted by New to commitTimeoutDefault;
	// tests may override it. Task 6 replaces the blind rollback with a
	// query-service status check.
	commitTimeout time.Duration

	// maxBatchSize bounds how many pending txs one drain cycle folds into a
	// single merged committer tx (see executeCycle / PendingPool.DrainUpTo).
	// 0 means unbounded -- the pure drain-all model, which assumes arrival is
	// paced upstream (e.g. by the query service on the submit path). Set a
	// positive bound via SetMaxBatchSize when submission can burst faster than
	// the drain cycle (so a burst is not swallowed into one oversized Fabric
	// tx that exceeds the orderer's max message size). Atomic so it can be set
	// safely while the executor goroutine is running.
	maxBatchSize atomic.Int64
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

// SetMaxBatchSize bounds how many pending txs a single drain cycle folds into
// one merged committer tx. n <= 0 restores the unbounded drain-all default.
// Safe to call at any time (the field is atomic); it takes effect on the next
// drain cycle. See the maxBatchSize field and executeCycle.
func (g *Gateway) SetMaxBatchSize(n int) {
	g.maxBatchSize.Store(int64(n))
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
