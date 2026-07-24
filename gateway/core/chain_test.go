/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"crypto/ecdsa"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	co "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// --- helpers ---

func createTestEthTx(t *testing.T, key *ecdsa.PrivateKey, to common.Address, value *big.Int) *types.Transaction {
	t.Helper()
	tx := types.NewTransaction(0, to, value, 21000, big.NewInt(1000), []byte("test data"))
	signer := types.NewEIP155Signer(big.NewInt(4011))
	signed, err := types.SignTx(tx, signer, key)
	require.NoError(t, err)
	return signed
}

func marshaledEthTx(t *testing.T, key *ecdsa.PrivateKey, to common.Address, value *big.Int) []byte {
	t.Helper()
	b, err := createTestEthTx(t, key, to, value).MarshalBinary()
	require.NoError(t, err)
	return b
}

// --- convertToDomain ---

func TestConvertToDomain_ValidTx(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	ethb := marshaledEthTx(t, key, common.HexToAddress("0x1111111111111111111111111111111111111111"), big.NewInt(100))

	b := blocks.Block{
		Number:     42,
		Hash:       []byte("block-hash"),
		ParentHash: []byte("parent-hash"),
		Timestamp:  12345,
		Transactions: []blocks.Transaction{{
			ID:        "tx-1",
			Number:    0,
			Valid:     true,
			Status:    0,
			InputArgs: [][]byte{{byte(co.ProposalTypeEVMTx)}, ethb},
		}},
	}

	got := ConvertToDomain(b)

	assert.Equal(t, uint64(42), got.BlockNumber)
	assert.Equal(t, []byte("block-hash"), got.BlockHash)
	assert.Equal(t, []byte("parent-hash"), got.ParentHash)
	assert.Equal(t, int64(12345), got.Timestamp)
	require.Len(t, got.Transactions, 1)
	assert.Equal(t, uint8(1), got.Transactions[0].Status)
	assert.Equal(t, "tx-1", got.Transactions[0].FabricTxID)
}

func TestConvertToDomain_InvalidTxStatus(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	ethb := marshaledEthTx(t, key, common.HexToAddress("0x1111111111111111111111111111111111111111"), big.NewInt(100))

	b := blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{{
			ID:        "tx-bad",
			Valid:     false, // invalid tx
			InputArgs: [][]byte{{byte(co.ProposalTypeEVMTx)}, ethb},
		}},
	}

	got := ConvertToDomain(b)

	require.Len(t, got.Transactions, 1)
	assert.Equal(t, uint8(0), got.Transactions[0].Status)
}

func TestConvertToDomain_SkipsInsufficientInputArgs(t *testing.T) {
	b := blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: "tx-no-args", Valid: true, InputArgs: nil},
			{ID: "tx-one-arg", Valid: true, InputArgs: [][]byte{[]byte("only-one")}},
		},
	}

	got := ConvertToDomain(b)

	assert.Len(t, got.Transactions, 0)
}

func TestConvertToDomain_SkipsInvalidEthBytes(t *testing.T) {
	b := blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{{
			ID:        "tx-bad-bytes",
			Valid:     true,
			InputArgs: [][]byte{nil, []byte("not-an-eth-tx")},
		}},
	}

	got := ConvertToDomain(b)

	assert.Len(t, got.Transactions, 0)
}

// TestConvertToDomainBatch verifies that a block containing a legacy single-tx
// EVM Fabric tx FOLLOWED BY a merged ProposalTypeEVMBatch Fabric tx (Task 1's
// envelope: InputArgs = [{type}, ethTx1, ethTx2, ...]) is unpacked into one
// domain.Transaction per EVM tx (1 + 2 = 3 total), with a flat, block-global,
// contiguous, unique TxIndex (0,1,2) — NOT the shared Fabric tx.Number, which
// two sub-txs of the same batch would otherwise collide on (violating both eth
// transactionIndex semantics and the storage layer's UNIQUE(block_number,
// tx_index) index) — alongside a per-Fabric-tx SubIndex (0,0,1), per-sub-tx
// receipt status (from execution.PerTxOutcome.Status: 200 -> 1, 201 -> 0), and
// — for the successful batch sub-tx only — its eth logs.
func TestConvertToDomainBatch(t *testing.T) {
	leadKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	key1, err := crypto.GenerateKey()
	require.NoError(t, err)
	key2, err := crypto.GenerateKey()
	require.NoError(t, err)

	leadEthb := marshaledEthTx(t, leadKey, common.HexToAddress("0x0000000000000000000000000000000000000009"), big.NewInt(9))
	ethb1 := marshaledEthTx(t, key1, common.HexToAddress("0x1111111111111111111111111111111111111111"), big.NewInt(100))
	ethb2 := marshaledEthTx(t, key2, common.HexToAddress("0x2222222222222222222222222222222222222222"), big.NewInt(200))

	// sub-tx 0: success, emitting one log. The endorser marshals successful
	// sub-tx logs as raw json([]execution.Log) -- NOT wrapped in a ChaincodeEvent
	// (see EVMEngine.runOn).
	successLogs, err := json.Marshal([]execution.Log{{
		Address: common.HexToAddress("0x3333333333333333333333333333333333333333").Bytes(),
		Topics:  [][]byte{common.HexToHash("0xaaaa").Bytes()},
		Data:    []byte("log-data"),
	}})
	require.NoError(t, err)

	// sub-tx 1: EVM revert. Its event content is irrelevant to the parser --
	// only PerTxOutcome.Status drives whether logs are decoded -- so any bytes
	// stand in for the endorser's common.MarshalRevert(...) payload.
	outcomes := []execution.PerTxOutcome{
		{Status: 200, Event: successLogs},
		{Status: 201, Event: []byte("revert-marker")},
	}
	outcomesPayload, err := json.Marshal(outcomes)
	require.NoError(t, err)

	// The SDK Endorse builder wraps ExecutionResult.Event in one outer
	// ChaincodeEvent (EventName "log") before it lands as the committed
	// blocks.Transaction.Events -- see common.IsRevertEvent's doc comment.
	eventsBytes, err := proto.Marshal(&peer.ChaincodeEvent{
		Payload:   outcomesPayload,
		EventName: "log",
	})
	require.NoError(t, err)

	b := blocks.Block{
		Number:     99,
		Hash:       []byte("batch-block-hash"),
		ParentHash: []byte("batch-parent-hash"),
		Timestamp:  54321,
		Transactions: []blocks.Transaction{
			{
				ID:        "tx-single-0",
				Number:    0,
				Valid:     true,
				Status:    0,
				InputArgs: [][]byte{{byte(co.ProposalTypeEVMTx)}, leadEthb},
			},
			{
				ID:        "tx-batch-1",
				Number:    1,
				Valid:     true,
				Status:    0,
				InputArgs: [][]byte{{byte(co.ProposalTypeEVMBatch)}, ethb1, ethb2},
				Events:    eventsBytes,
			},
		},
	}

	got := ConvertToDomain(b)

	require.Len(t, got.Transactions, 3)

	lead := got.Transactions[0]
	assert.Equal(t, int64(0), lead.TxIndex)
	assert.Equal(t, int64(0), lead.SubIndex)

	sub0 := got.Transactions[1]
	assert.Equal(t, int64(1), sub0.TxIndex)
	assert.Equal(t, int64(0), sub0.SubIndex)
	assert.Equal(t, uint8(1), sub0.Status)
	require.Len(t, sub0.Logs, 1)
	assert.Equal(t, common.HexToAddress("0x3333333333333333333333333333333333333333").Bytes(), sub0.Logs[0].Address)
	assert.Equal(t, []byte("log-data"), sub0.Logs[0].Data)
	assert.Equal(t, uint64(99), sub0.Logs[0].BlockNumber)
	assert.Equal(t, int64(1), sub0.Logs[0].TxIndex) // log's TxIndex tracks the flat index, not tx.Number

	sub1 := got.Transactions[2]
	assert.Equal(t, int64(2), sub1.TxIndex)
	assert.Equal(t, int64(1), sub1.SubIndex)
	assert.Equal(t, uint8(0), sub1.Status)
	assert.Empty(t, sub1.Logs)

	// TxIndex is contiguous and unique across all 3 domain txs -- in
	// particular the two batch sub-txs (which share Fabric tx.Number == 1) do
	// NOT collide.
	seen := map[int64]bool{}
	for _, etx := range got.Transactions {
		assert.False(t, seen[etx.TxIndex], "duplicate TxIndex %d", etx.TxIndex)
		seen[etx.TxIndex] = true
	}
	assert.Equal(t, []int64{0, 1, 2}, []int64{lead.TxIndex, sub0.TxIndex, sub1.TxIndex})

	// The two batch sub-txs decode two distinct eth transactions.
	assert.NotEqual(t, sub0.TxHash, sub1.TxHash)
}

func TestConvertToDomain_EmptyBlock(t *testing.T) {
	b := blocks.Block{Number: 5}
	got := ConvertToDomain(b)

	assert.Equal(t, uint64(5), got.BlockNumber)
	assert.Len(t, got.Transactions, 0)
}

// --- convertTransaction ---

func TestConvertTransaction_RegularTransfer(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	ethTx := createTestEthTx(t, key, to, big.NewInt(100))
	ethb, _ := ethTx.MarshalBinary()

	logIndex := int64(0)
	domainTx, err := convertTransaction(ethb, []byte("block-hash"), 42, 5, "fabric-tx-123", 1, 0, nil, &logIndex)

	require.NoError(t, err)
	assert.Equal(t, ethTx.Hash().Bytes(), domainTx.TxHash)
	assert.Equal(t, []byte("block-hash"), domainTx.BlockHash)
	assert.Equal(t, uint64(42), domainTx.BlockNumber)
	assert.Equal(t, int64(5), domainTx.TxIndex)
	assert.Equal(t, to.Bytes(), domainTx.ToAddress)
	assert.Nil(t, domainTx.ContractAddress)
	assert.Equal(t, "fabric-tx-123", domainTx.FabricTxID)
	assert.Equal(t, uint8(1), domainTx.Status)
	assert.Equal(t, 0, domainTx.FabricTxStatus)
	assert.NotNil(t, domainTx.FromAddress)
	assert.NotNil(t, domainTx.RawTx)
}

func TestConvertTransaction_ContractCreation(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	ethTx := types.NewContractCreation(0, big.NewInt(0), 1000000, big.NewInt(1000), []byte("contract code"))
	signer := types.NewEIP155Signer(big.NewInt(4011))
	signed, err := types.SignTx(ethTx, signer, key)
	require.NoError(t, err)
	ethb, _ := signed.MarshalBinary()

	logIndex := int64(0)
	domainTx, err := convertTransaction(ethb, []byte("block-hash"), 42, 3, "fabric-tx-456", 1, 0, nil, &logIndex)

	require.NoError(t, err)
	assert.Nil(t, domainTx.ToAddress)
	assert.NotNil(t, domainTx.ContractAddress)
	from := crypto.PubkeyToAddress(key.PublicKey)
	assert.Equal(t, crypto.CreateAddress(from, signed.Nonce()).Bytes(), domainTx.ContractAddress)
}

func TestConvertTransaction_InvalidSignature(t *testing.T) {
	ethTx := types.NewTransaction(0,
		common.HexToAddress("0x1234567890123456789012345678901234567890"),
		big.NewInt(100), 21000, big.NewInt(1000), []byte("test"),
	)
	ethb, _ := ethTx.MarshalBinary()

	logIndex := int64(0)
	_, err := convertTransaction(ethb, []byte("block-hash"), 42, 1, "fabric-tx-789", 1, 0, nil, &logIndex)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sender")
}

func TestConvertTransaction_ValidationCodes(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	ethb := marshaledEthTx(t, key, common.HexToAddress("0x1234567890123456789012345678901234567890"), big.NewInt(100))

	tests := []struct {
		name           string
		ethStatus      uint8
		validationCode int
	}{
		{"valid", 1, 0},
		{"mvcc_conflict", 0, 11},
		{"endorsement_failure", 0, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logIndex := int64(0)
			domainTx, err := convertTransaction(ethb, []byte("bh"), 42, 1, "tx", tt.ethStatus, tt.validationCode, nil, &logIndex)
			require.NoError(t, err)
			assert.Equal(t, tt.ethStatus, domainTx.Status)
			assert.Equal(t, tt.validationCode, domainTx.FabricTxStatus)
		})
	}
}
