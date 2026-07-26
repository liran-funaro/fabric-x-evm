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
	"github.com/hyperledger/fabric-x-common/api/committerpb"
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
	batchSubmitter  *BatchSubmitter
	endorsers       *EndorsementClient
	store           Store
	chainID         *big.Int
	ChainConfig     *params.ChainConfig
	Signer          types.Signer
	pending         *PendingPool
	arrivals        chan struct{} // non-blocking wake-up for the idle executor loop
	wg              sync.WaitGroup
	stopOnce        sync.Once
	endorsementChan chan sdk.Endorsement // Channel to send endorsements to BatchSubmitter

	// commitWaiters correlates a submitted committer tx (by FabricTxID) with
	// the executor goroutine awaiting its commit/abort outcome. See
	// executor.go: registerCommitWaiter/awaitCommit/HandleTx.
	commitMu      sync.Mutex
	commitWaiters map[string]chan committerpb.Status

	// commitTimeout is the stall backstop applied by awaitCommit. Defaulted
	// by New to commitTimeoutDefault; tests may override it to force a fast
	// timeout. See executor.go.
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
func New(ec *EndorsementClient, batchSubmitter *BatchSubmitter, store Store, chainID int64, endorsementChan chan sdk.Endorsement) (*Gateway, error) {
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
		commitWaiters:   make(map[string]chan committerpb.Status),
		commitTimeout:   commitTimeoutDefault,
	}, nil
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
