/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"

	fc "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
	"github.com/hyperledger/fabric-x-evm/gateway/storage"
	"github.com/hyperledger/fabric-x-evm/gateway/storage/trie"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/state/sqlite"
)

// Chain owns the block storage and state trie. It implements blocks.BlockHandler
// (for block ingestion) and core.Store (via the embedded *storage.Store, for API queries).
type Chain struct {
	*storage.Store
	db       *sql.DB
	ts       *trie.Store
	prevHash common.Hash // Ethereum hash of last committed block; seeded from DB on startup
}

// NewChain opens the SQLite database and trie store, seeds state from the latest committed
// block, and returns a ready Chain. dbConnStr uses the modernc SQLite DSN format;
// triePath is the directory for the PebbleDB trie (empty string = in-memory).
// The caller must register the SQLite driver (e.g. _ "modernc.org/sqlite") before calling.
func NewChain(dbConnStr, triePath string, withTrie bool) (*Chain, error) {
	db, err := sqlite.Open(dbConnStr)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	blockStore := storage.NewStore(db)
	if err := blockStore.Init(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init block store: %w", err)
	}

	// Seed trie root and parent hash from the latest committed block so state
	// resumes correctly after a restart.
	var initialRoot, prevHash common.Hash
	if latest, err := blockStore.LatestBlock(context.Background(), false); err == nil && latest != nil {
		initialRoot = common.BytesToHash(latest.StateRoot)
		prevHash = common.BytesToHash(latest.BlockHash)
	}

	var ts *trie.Store
	if withTrie {
		ts, err = trie.New(triePath, initialRoot)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("open trie store: %w", err)
		}
	}

	return &Chain{Store: blockStore, db: db, ts: ts, prevHash: prevHash}, nil
}

// Handle implements blocks.BlockHandler. It commits the block's write sets to the trie,
// then persists the block and its transactions to the database.
func (c *Chain) Handle(ctx context.Context, b blocks.Block) error {
	ebl := ConvertToDomain(b)

	ebl.ParentHash = c.prevHash.Bytes()
	if c.ts != nil {
		stateRoot, err := c.ts.Commit(ctx, b)
		if err != nil {
			return err // irrecoverable
		}
		ebl.StateRoot = stateRoot.Bytes()
	} else {
		ebl.StateRoot = types.EmptyRootHash[:]
	}
	c.prevHash = common.BytesToHash(ebl.BlockHash)

	if err := c.Store.InsertBlock(ctx, ebl); err != nil {
		return err
	}

	return nil
}

// EnsureGenesisBlock inserts an empty block 0 if the store has no blocks yet, so
// eth_getBlockByNumber("latest") is never null before the first transaction commits.
// Real Fabric/fabric-x channels always deliver a genesis block already; this is only
// for backends (like fabrictest) that don't.
func (c *Chain) EnsureGenesisBlock(ctx context.Context) error {
	latest, err := c.Store.LatestBlock(ctx, false)
	if err != nil {
		return err
	}
	if latest != nil {
		return nil
	}

	genesisHash := crypto.Keccak256([]byte("fxevm-testnode-genesis"))
	if err := c.Store.InsertBlock(ctx, domain.Block{
		BlockNumber: 0,
		BlockHash:   genesisHash,
		ParentHash:  make([]byte, common.HashLength),
		StateRoot:   types.EmptyRootHash[:],
	}); err != nil {
		return err
	}
	c.prevHash = common.BytesToHash(genesisHash)
	return nil
}

// Close releases the trie and database resources.
func (c *Chain) Close() error {
	if c.ts != nil {
		c.ts.Close()
	}
	return c.db.Close()
}

// ConvertToDomain maps a Fabric SDK block to the gateway domain model,
// extracting and decoding the embedded Ethereum transactions.
// This is a standalone function so it can be reused by other components like Gateway.
func ConvertToDomain(b blocks.Block) domain.Block {
	ebl := domain.Block{
		BlockNumber:  b.Number,
		BlockHash:    b.Hash,
		ParentHash:   b.ParentHash,
		Timestamp:    b.Timestamp,
		Transactions: make([]domain.Transaction, 0),
	}

	logIndex := int64(0) // logIndex is the index of the log in the block
	txIndex := int64(0)  // txIndex is a flat, block-global EVM tx index (eth transactionIndex):
	// it increments once per emitted domain.Transaction, across both the
	// single-tx path and every batch sub-tx, so it stays contiguous and unique
	// within the block even though several domain txs can share one Fabric
	// tx.Number. SubIndex (below) separately tracks position within the
	// Fabric tx itself.
	for _, tx := range b.Transactions {
		// TODO: filter on namespace?

		if len(tx.InputArgs) < 2 {
			// no embedded eth tx
			continue
		}

		switch {
		case bytes.Equal(tx.InputArgs[0], []byte{byte(fc.ProposalTypeEVMTx)}):
			// Legacy single-tx envelope: InputArgs = [{type}, ethTxBytes].
			status := uint8(0)
			if tx.Valid && !fc.IsRevertEvent(tx.Events) {
				status = 1
			}

			etx, err := convertTransaction(tx.InputArgs[1], b.Hash, b.Number, txIndex, tx.ID, status, tx.Status, tx.Events, &logIndex)
			if err != nil {
				panic(err) // we surface this for now instead of swallowing it
			}
			txIndex++

			ebl.Transactions = append(ebl.Transactions, etx)

		case bytes.Equal(tx.InputArgs[0], []byte{byte(fc.ProposalTypeEVMBatch)}):
			// Merged-batch envelope: InputArgs = [{type}, ethTx1, ethTx2, ...],
			// one entry in the outcomes slice (decoded from tx.Events) per
			// sub-tx, index = sub-index. See endorser/core.Endorser.ExecuteBatch
			// and endorser/execution.PerTxOutcome.
			outcomes, err := decodeBatchOutcomes(tx.Events)
			if err != nil {
				panic(err)
			}

			for sub, ethTxBytes := range tx.InputArgs[1:] {
				var outcome execution.PerTxOutcome
				if sub < len(outcomes) {
					outcome = outcomes[sub]
				}

				status := uint8(0)
				if tx.Valid && outcome.Status == fc.StatusOK {
					status = 1
				}

				// Sub-tx events never round-trip through convertTransaction's
				// wrapped-event log path: a successful sub-tx's Event is raw
				// json([]execution.Log), not a ChaincodeEvent, so logs are
				// decoded separately below (and skipped entirely on revert).
				etx, err := convertTransaction(ethTxBytes, b.Hash, b.Number, txIndex, tx.ID, status, tx.Status, nil, &logIndex)
				if err != nil {
					panic(err)
				}
				etx.SubIndex = int64(sub)
				txIndex++

				if status == 1 {
					etx.Logs = decodeBatchLogs(outcome.Event, b.Number, b.Hash, etx.TxHash, etx.TxIndex, &logIndex)
				}

				ebl.Transactions = append(ebl.Transactions, etx)
			}

		default:
			continue // non-eth tx
		}
	}

	return ebl
}

// decodeBatchOutcomes unwraps a merged-batch Fabric tx's Events to recover the
// per-sub-tx outcomes the endorser packed into the merged ExecutionResult.Event
// (see endorser/core.Endorser.ExecuteBatch). Like the legacy single-tx path, the
// SDK Endorse builder wraps that Event in one outer ChaincodeEvent (EventName
// "log") before it lands as the committed tx.Events.
func decodeBatchOutcomes(events []byte) ([]execution.PerTxOutcome, error) {
	if len(events) == 0 {
		return nil, nil
	}

	var outer peer.ChaincodeEvent
	if err := proto.Unmarshal(events, &outer); err != nil {
		return nil, fmt.Errorf("unwrap batch event: %w", err)
	}
	if len(outer.Payload) == 0 {
		return nil, nil
	}

	var outcomes []execution.PerTxOutcome
	if err := json.Unmarshal(outer.Payload, &outcomes); err != nil {
		return nil, fmt.Errorf("decode batch outcomes: %w", err)
	}
	return outcomes, nil
}

// decodeBatchLogs decodes a successful sub-tx's raw eth-logs event
// (json.Marshal([]execution.Log), unwrapped -- see EVMEngine.runOn) into
// domain.Log entries, assigning a block-global, monotonically increasing
// LogIndex shared across every sub-tx in the block.
func decodeBatchLogs(event []byte, blockNumber uint64, blockHash, txHash []byte, txIndex int64, logIndex *int64) []domain.Log {
	if len(event) == 0 {
		return nil
	}

	var rawLogs []execution.Log
	if err := json.Unmarshal(event, &rawLogs); err != nil {
		// Malformed per-tx log payload: surface nothing rather than panic the
		// whole block ingestion over one sub-tx's event.
		return nil
	}

	logs := make([]domain.Log, 0, len(rawLogs))
	for _, l := range rawLogs {
		logs = append(logs, domain.Log{
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
			TxHash:      txHash,
			TxIndex:     txIndex,
			LogIndex:    *logIndex,
			Address:     l.Address,
			Topics:      l.Topics,
			Data:        l.Data,
		})
		*logIndex++
	}
	return logs
}

// convertTransaction converts an Ethereum transaction to a domain.Transaction.
func convertTransaction(ethTxBytes []byte, blockHash []byte, blockNumber uint64, txIndex int64, txID string, ethStatus uint8, validationCode int, events []byte, logIndex *int64) (domain.Transaction, error) {
	ethTx := &types.Transaction{}
	if err := ethTx.UnmarshalBinary(ethTxBytes); err != nil {
		return domain.Transaction{}, fmt.Errorf("invalid tx: %w", err)
	}

	var signer types.Signer
	if id := ethTx.ChainId(); id.Sign() > 0 {
		signer = types.LatestSignerForChainID(id)
	} else {
		signer = types.HomesteadSigner{}
	}
	from, err := types.Sender(signer, ethTx)
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("invalid sender: %w", err)
	}
	var to []byte
	var contractAddr []byte
	if ethTx.To() != nil {
		to = ethTx.To().Bytes()
	} else {
		contractAddr = crypto.CreateAddress(from, ethTx.Nonce()).Bytes()
	}

	hash := ethTx.Hash().Bytes()

	var logs []domain.Log
	if len(events) > 0 && !fc.IsRevertEvent(events) {
		rawLogs, err := fc.UnmarshalLogs(events)
		if err != nil {
			// ?
			//return fmt.Errorf("parse logs: %w", err)
		}

		// Convert common.Log to domain.Log with full context
		logs = []domain.Log{}
		for _, l := range rawLogs {
			logs = append(logs, domain.Log{
				BlockNumber: blockNumber,
				BlockHash:   blockHash,
				TxHash:      hash,
				TxIndex:     txIndex,
				LogIndex:    *logIndex,
				Address:     l.Address,
				Topics:      l.Topics,
				Data:        l.Data,
			})
			*logIndex++
		}
	}

	return domain.Transaction{
		TxHash:          hash,
		BlockHash:       blockHash,
		BlockNumber:     blockNumber,
		TxIndex:         txIndex,
		RawTx:           ethTxBytes,
		FromAddress:     from.Bytes(),
		ToAddress:       to,
		ContractAddress: contractAddr,
		Status:          ethStatus,
		FabricTxID:      txID,
		FabricTxStatus:  validationCode,
		Logs:            logs,
	}, nil
}
