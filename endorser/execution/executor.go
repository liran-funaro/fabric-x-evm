/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	fxcommon "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// EVMConfig holds the configuration for EVM execution.
type EVMConfig struct {
	ChainConfig *params.ChainConfig
	// MaxTxGas caps msg.GasLimit before execution. 0 means unlimited.
	MaxTxGas uint64
	// DebugLogs wraps the per-tx StateDB in StateDBLogger when true.
	DebugLogs bool
	// WarmWorkers caps the concurrency of ExecuteBatch's warm pass. 0 (the default)
	// means one worker per transaction in the batch (design RQ1: concurrency = batch
	// size): warm reads block on the query service (I/O-bound, not CPU-bound), so
	// issuing them all at once fills the query service's read-batch window rather than
	// leaving its max-batch-wait exposed on each small wave. Set a positive value only
	// to cap concurrency for a fast, non-blocking backend (e.g. an in-memory KVS),
	// where unbounded warm goroutines would add scheduler churn with no I/O to overlap.
	WarmWorkers int
}

// KVSSnapshotter is the port execution uses to obtain a versioned read snapshot
// of the world state. query.Store (the Fabric-X query-service reader) implements it
// in production; storage.LightKVS implements it for tests.
type KVSSnapshotter interface {
	NewSnapshot(blockNumber uint64) (ReadStore, error)
}

// EVMEngine manages EVM execution and state reads for an endorser.
// It creates isolated per-transaction snapshots for execution and reads state directly
// for ChainStateReader calls.
type EVMEngine struct {
	namespace         string
	monotonicVersions bool

	// kvs provides versioned storage with snapshot isolation
	kvs       KVSSnapshotter
	evmConfig EVMConfig

	// stateDecorator, when non-nil, wraps the per-transaction StateDB just
	// before execution in EVERY path -- Execute and both ExecuteBatch passes
	// (warm and authoritative) -- so injected per-tx behavior applies uniformly
	// however the tx is executed. Nil in production (no wrapping). Test harnesses
	// set it via SetStateDecorator to replay historical traces on a fresh chain
	// (e.g. balance/nonce priming keyed on the individual tx); see
	// testimpl.EVMEngineWrapper.SetBalancePriming.
	stateDecorator func(ExtendedStateDB, *types.Transaction) ExtendedStateDB

	// execPool recycles reusableExec (a StateDB+Executor+primed EVM) across the
	// single-tx paths (Execute and ExecuteBatch's N==1 case), which otherwise
	// rebuild all per-tx machinery on every call. sync.Pool makes reuse safe
	// under concurrent Execute calls: each caller gets its own instance. Only
	// used on the fast path (no decorator, no debug logging).
	execPool sync.Pool

	// codeHashCache memoizes keccak256(contract code) across every StateDB this
	// engine builds -- the warm pass's per-worker DBs and the serial auth pass
	// alike -- so an immutable contract's code hash is computed once per run
	// instead of on every CALL/DELEGATECALL. Concurrency-safe; see the
	// codeHashCache type in statedb.go. One per engine so namespaces stay
	// isolated (each engine serves a single namespace).
	codeHashCache *codeHashCache
}

// reusableExec bundles a StateDB with the Executor (and primed EVM) bound to it,
// so a pooled unit can be reset in place and re-run without rebuilding either.
type reusableExec struct {
	sdb *StateDB
	ex  *Executor
}

// NewEVMEngine creates a new EVMEngine.
func NewEVMEngine(namespace string, kvs KVSSnapshotter, evmConfig EVMConfig, monotonicVersions bool) *EVMEngine {
	return &EVMEngine{
		namespace:         namespace,
		kvs:               kvs,
		monotonicVersions: monotonicVersions,
		evmConfig:         evmConfig,
		codeHashCache:     newCodeHashCache(),
	}
}

// SetStateDecorator installs a per-transaction StateDB decorator applied in
// every execution path (see the stateDecorator field). Intended for test
// harnesses; production leaves it nil. Set it before the engine serves traffic.
func (e *EVMEngine) SetStateDecorator(fn func(ExtendedStateDB, *types.Transaction) ExtendedStateDB) {
	e.stateDecorator = fn
}

// Execute runs a state-changing transaction and returns the EVM result,
// the Fabric read-write set, and any EVM logs emitted.
// State is always read from the latest block: endorsement must simulate against current state
// so that the resulting read-write set passes MVCC validation at commit time.
// Reverts produce a valid endorsement (Status 201 + revert event) instead of an error.
//
// Execute is the N==1 case of ExecuteBatch: it opens its own snapshot and builds its
// own state exactly as ExecuteBatch's single-tx path does, then shares runOn's
// revert/logs/success classification with it so the two can never drift apart.
func (e *EVMEngine) Execute(ctx context.Context, tx *types.Transaction) (endorsement.ExecutionResult, error) {
	reader, err := e.kvs.NewSnapshot(0)
	if err != nil {
		return endorsement.ExecutionResult{}, err
	}
	defer reader.Close()

	// Fast path (production): run on a pooled, reused StateDB+Executor so
	// repeated Execute calls do not rebuild all per-tx machinery.
	if e.stateDecorator == nil && !e.evmConfig.DebugLogs {
		return e.executeReusing(reader, tx)
	}

	state, err := e.newState(reader)
	if err != nil {
		return endorsement.ExecutionResult{}, err
	}

	return e.runOn(state, tx)
}

// committedFaultStatus classifies a committed-but-faulted EVM outcome -- a
// revert or an *ExecFailure (out of gas, invalid opcode, ...) -- and reports
// its endorsable status code. Both are DISTINCT from a pre-execution
// rejection (*TxRejected): geth's Executor.ApplyMessage bumps the sender's
// nonce and deducts gas for either one exactly as it would for a success (see
// its comment), so state.Result() already reflects that write, and the
// caller must endorse it as a committed outcome rather than abort. Returns
// (0, false) for anything else (a pre-execution rejection, or a genuine
// non-EVM error), which the caller must still treat as an error.
func committedFaultStatus(err error) (int32, bool) {
	if errors.Is(err, vm.ErrExecutionReverted) {
		return fxcommon.StatusEVMRevert, true
	}
	if _, ok := errors.AsType[*ExecFailure](err); ok {
		return fxcommon.StatusExecFailure, true
	}
	return 0, false
}

// runOn executes tx against the given state (already constructed over a reader)
// and returns the endorsement result. It contains the revert/logs/success
// classification previously inlined in Execute, now shared by Execute and
// ExecuteBatch so both paths classify a transaction identically.
func (e *EVMEngine) runOn(state ExtendedStateDB, tx *types.Transaction) (endorsement.ExecutionResult, error) {
	// Apply the optional per-tx StateDB decorator (test-only; nil in production).
	// Wrapping here covers Execute and both ExecuteBatch passes uniformly.
	if e.stateDecorator != nil {
		state = e.stateDecorator(state, tx)
	}
	ex, err := NewExecutor(state, noopCloser{}, nil, e.evmConfig)
	if err != nil {
		return endorsement.ExecutionResult{}, err
	}
	return e.classify(ex, state, tx)
}

// classify runs tx on the given (already-built) Executor and turns the raw EVM
// outcome into an endorsement result: a committed-but-faulted outcome (revert /
// ExecFailure) becomes a Go-error-free result with the right status, everything
// else propagates. Extracted from runOn so the batch fast path can drive it with
// a REUSED Executor+StateDB (reset between txs) while keeping identical
// classification. state must be the same StateDB the Executor was built over.
func (e *EVMEngine) classify(ex *Executor, state ExtendedStateDB, tx *types.Transaction) (endorsement.ExecutionResult, error) {
	ret, err := ex.Send(tx)
	if err != nil {
		status, ok := committedFaultStatus(err)
		if !ok {
			// Pre-execution rejection (*TxRejected) or a genuine server-side
			// fault: never committed, no RWS to endorse. The caller
			// (Execute, or ExecuteBatch's N==1/N>1 paths) decides whether a
			// *TxRejected should exclude-not-abort; anything else aborts.
			return endorsement.ExecutionResult{}, err
		}
		// A committed-but-faulted outcome (revert or ExecFailure): endorsed
		// with no Go error, exactly like a success, so the batch/single-tx
		// caller never treats it as an abort. Reuse the revert event marker:
		// chain.go's legacy single-tx path only needs to know "not a plain
		// success" from the event shape (see fc.IsRevertEvent), while the
		// batch path already gets the real status from PerTxOutcome.Status
		// rather than sniffing the event.
		event, mErr := fxcommon.MarshalRevert(ret, "", tx.Hash().Hex())
		if mErr != nil {
			return endorsement.ExecutionResult{}, fmt.Errorf("marshal fault event: %w", mErr)
		}
		rws := state.Result()
		if serr := state.Error(); serr != nil {
			// Result()'s read-set backfill (see StateDB.Result) can itself fail a
			// store read -- e.g. the same stale query-service view that
			// Send/ApplyMessage already guard against, just hit one read later.
			// Treat it identically: abort rather than endorse an RWS missing a
			// read the backfill couldn't complete.
			return endorsement.ExecutionResult{}, serr
		}
		return endorsement.ExecutionResult{
			RWS:     rws,
			Event:   event,
			Status:  status,
			Message: err.Error(),
			Payload: ret,
		}, nil
	}

	var logs []byte
	if l := state.Logs(); len(l) > 0 {
		logs, err = json.Marshal(l)
		if err != nil {
			return endorsement.ExecutionResult{}, fmt.Errorf("marshal logs: %w", err)
		}
	}

	rws := state.Result()
	if serr := state.Error(); serr != nil {
		// See the comment on the committed-fault branch above: Result()'s
		// backfill can fail a store read even though Send/ApplyMessage's own
		// checkpoints already passed.
		return endorsement.ExecutionResult{}, serr
	}
	return endorsement.Success(rws, logs, ret), nil
}

// PerTxOutcome carries one sub-tx's outcome within a merged batch endorsement:
// its execution status (200 success, 201 revert, ...) and its event blob (nil
// if it emitted none). A downstream stage decodes a []PerTxOutcome from the
// merged ExecutionResult.Event to build one receipt per sub-tx — the status
// travels explicitly here rather than being inferred from the event's shape,
// because a batch can freely mix successful and reverted sub-txs.
type PerTxOutcome struct {
	Status int32  `json:"status"`
	Event  []byte `json:"event"`
}

// ExecuteMergedBatch runs the batch two-phase (via ExecuteBatch) and folds the
// per-tx results into one merged ExecutionResult (status 200) plus one
// PerTxOutcome per sub-tx, so the caller can sign a single endorsement over
// the whole batch while still recovering each sub-tx's status and event.
func (e *EVMEngine) ExecuteMergedBatch(ctx context.Context, txs []*types.Transaction) (endorsement.ExecutionResult, []PerTxOutcome, error) {
	results, err := e.ExecuteBatch(ctx, txs)
	if err != nil {
		return endorsement.ExecutionResult{}, nil, err
	}
	rws, events := MergeResults(results)
	outcomes := make([]PerTxOutcome, len(results))
	for i := range results {
		outcomes[i] = PerTxOutcome{Status: results[i].Status, Event: events[i]}
	}
	return endorsement.ExecutionResult{RWS: rws, Status: 200, Message: "OK"}, outcomes, nil
}

// noopCloser is a reader stand-in for runOn's internal Executor: the real
// ReadStore's lifecycle (open/close) is owned by the caller (Execute or
// ExecuteBatch), not by the short-lived Executor runOn builds around state.
type noopCloser struct{}

func (noopCloser) Get(string, string) (*blocks.WriteRecord, error) { return nil, nil }
func (noopCloser) Close() error                                    { return nil }

// Call executes a read-only call (eth_call semantics) against the state at blockNumber
// (0 / nil = latest). The EVM block context is not reconstructed for historical blocks —
// with all forks enabled from block 0 this is harmless.
func (e *EVMEngine) Call(msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	ex, err := e.newExecutor(blockNumber)
	if err != nil {
		return nil, err
	}
	defer ex.Close()

	return ex.Call(msg)
}

func (e *EVMEngine) BalanceAt(_ context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	snap, reader, err := e.newSnapshotAt(blockNumber)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return snap.GetBalance(account).ToBig(), nil
}

func (e *EVMEngine) StorageAt(_ context.Context, account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	snap, reader, err := e.newSnapshotAt(blockNumber)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return snap.GetState(account, key).Bytes(), nil
}

func (e *EVMEngine) CodeAt(_ context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	snap, reader, err := e.newSnapshotAt(blockNumber)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return snap.GetCode(account), nil
}

func (e *EVMEngine) NonceAt(_ context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	snap, reader, err := e.newSnapshotAt(blockNumber)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return snap.GetNonce(account), nil
}

// newExecutor creates a fresh executor with an isolated StateDB.
// blockNumber selects the Fabric block height for the state snapshot (nil = latest).
func (e *EVMEngine) newExecutor(blockNumber *big.Int) (*Executor, error) {
	var stateBlockNum uint64
	if blockNumber != nil {
		stateBlockNum = blockNumber.Uint64()
	}

	// Begin a new reader to get snapshot isolation
	reader, err := e.kvs.NewSnapshot(stateBlockNum)
	if err != nil {
		return nil, err
	}

	// Create StateDB with the reader
	stateDB, err := NewStateDB(context.TODO(), reader, e.namespace, stateBlockNum, e.monotonicVersions)
	if err != nil {
		reader.Close()
		return nil, err
	}
	stateDB.codeHashCache = e.codeHashCache
	var state ExtendedStateDB = stateDB
	if e.evmConfig.DebugLogs {
		state = NewStateDBLogger(stateDB)
	}

	ex, err := NewExecutor(state, reader, blockNumber, e.evmConfig)
	if err != nil {
		reader.Close()
		return nil, err
	}
	return ex, nil
}

// newSnapshotAt returns an ExtendedStateDB over the state at the given Fabric block height (0 = latest).
// The caller must close the returned reader when done.
func (e *EVMEngine) newSnapshotAt(blockNumber *big.Int) (ExtendedStateDB, ReadStore, error) {
	blockNum := uint64(0)
	if blockNumber != nil {
		blockNum = blockNumber.Uint64()
	}

	// Begin a new reader to get snapshot isolation
	reader, err := e.kvs.NewSnapshot(blockNum)
	if err != nil {
		return nil, nil, err
	}

	// Create StateDB with the reader
	stateDB, err := NewStateDB(context.TODO(), reader, e.namespace, blockNum, e.monotonicVersions)
	if err != nil {
		reader.Close()
		return nil, nil, err
	}
	stateDB.codeHashCache = e.codeHashCache
	return stateDB, reader, nil
}

// Executor is an EVM execution context. Historically one-per-transaction, it can
// now be reused across a batch's transactions (see EVMEngine.ExecuteBatch): the
// block context, signer and EVM are fixed for a whole batch, so they are built
// once here and reused, and the per-tx StateDB is reset in place between txs.
type Executor struct {
	state    ExtendedStateDB
	reader   ReadStore // reader that must be closed when done
	ChainCfg *params.ChainConfig
	BlockCtx vm.BlockContext
	maxTxGas uint64

	// signer is derived solely from ChainCfg + block number/time (all fixed for
	// this Executor), so it is computed once here rather than per PrepareMessage.
	signer types.Signer

	// evm, when non-nil, is reused across ApplyMessage calls instead of building
	// a fresh vm.EVM per tx. Safe because geth's stateTransition sets a fresh
	// TxContext per tx (SetTxContext), call depth returns to 0 after each
	// top-level call, and the jump-dest cache is code-keyed; Release (which
	// recycles the stack arena) is never called between txs. It is bound to
	// `state`, so reuse requires resetting that StateDB in place, not swapping it.
	evm *vm.EVM
}

// NewExecutor creates an Executor with the provided StateDB and reader.
// blockNumber sets the EVM block context (nil = 0). evmConfig.ChainConfig must be set.
// The caller is responsible for closing the reader when done with the Executor.
// The stateDB parameter accepts ExtendedStateDB interface to allow DualStateDB for testing.
func NewExecutor(stateDB ExtendedStateDB, reader ReadStore, blockNumber *big.Int, evmConfig EVMConfig) (*Executor, error) {
	if evmConfig.ChainConfig == nil {
		return nil, fmt.Errorf("evmConfig.ChainConfig must be set")
	}

	if blockNumber == nil {
		blockNumber = new(big.Int)
	}
	const defaultBlockTime = uint64(1_000_000)
	const defaultGasLimit = uint64(300_000_000)

	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     func(uint64) common.Hash { return common.Hash{} },
		Coinbase:    common.HexToAddress("0x0"),
		BlockNumber: blockNumber,
		Time:        defaultBlockTime,
		Difficulty:  big.NewInt(0),  // disabled post-merge
		Random:      &common.Hash{}, // Warning: PREVRANDAO stub must not be relied on by smart contracts.
		GasLimit:    defaultGasLimit,
		BaseFee:     big.NewInt(0),
	}

	// Cancun requires a non-nil BlobBaseFee; state_transition.go dereferences it directly
	// for blob transactions.
	if evmConfig.ChainConfig.IsCancun(blockNumber, defaultBlockTime) {
		excess := uint64(0)
		blockCtx.BlobBaseFee = eip4844.CalcBlobFee(evmConfig.ChainConfig, &types.Header{ExcessBlobGas: &excess})
	}

	return &Executor{
		state:    stateDB,
		reader:   reader,
		ChainCfg: evmConfig.ChainConfig,
		BlockCtx: blockCtx,
		maxTxGas: evmConfig.MaxTxGas,
		signer:   types.MakeSigner(evmConfig.ChainConfig, blockCtx.BlockNumber, blockCtx.Time),
	}, nil
}

// primeEVM binds a reusable vm.EVM to this Executor so ApplyMessage no longer
// builds a fresh one per tx. Call once after construction on the batch fast
// path; the EVM is bound to h.state, so that StateDB must be reset in place
// (not replaced) between txs.
func (h *Executor) primeEVM() {
	h.evm = vm.NewEVM(h.BlockCtx, h.state, h.ChainCfg, vm.Config{})
}

// Close releases the reader's snapshot reference.
// This should be called when the Executor is done to allow garbage collection.
func (h *Executor) Close() error {
	if h.reader != nil {
		return h.reader.Close()
	}
	return nil
}

// callMsgToMessage converts an ethereum.CallMsg into a core.Message.
// The baseFee parameter is used to calculate the effective gas price for EIP-1559 transactions.
// If baseFee is nil, legacy gas pricing is used.
// skipNonceCheck and skipTxCheck control whether nonce and EOA checks should be skipped.
func callMsgToMessage(msg ethereum.CallMsg, baseFee *big.Int, skipNonceCheck, skipTxCheck bool) *core.Message {
	var (
		gasPrice  *big.Int
		gasFeeCap *big.Int
		gasTipCap *big.Int
	)

	if baseFee == nil {
		// Legacy gas pricing
		if msg.GasPrice != nil {
			gasPrice = msg.GasPrice
		} else {
			gasPrice = new(big.Int)
		}
		gasFeeCap, gasTipCap = gasPrice, gasPrice
	} else {
		// EIP-1559 gas pricing
		if msg.GasPrice != nil {
			// Legacy gas field provided, convert to 1559 gas typing
			gasPrice = msg.GasPrice
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// Use 1559 gas fields
			if msg.GasFeeCap != nil {
				gasFeeCap = msg.GasFeeCap
			} else {
				gasFeeCap = new(big.Int)
			}
			if msg.GasTipCap != nil {
				gasTipCap = msg.GasTipCap
			} else {
				gasTipCap = new(big.Int)
			}
			// Calculate effective gas price for EVM execution
			gasPrice = new(big.Int)
			if gasFeeCap.BitLen() > 0 || gasTipCap.BitLen() > 0 {
				gasPrice = new(big.Int).Add(gasTipCap, baseFee)
				if gasPrice.Cmp(gasFeeCap) > 0 {
					gasPrice = gasFeeCap
				}
			}
		}
	}

	// Handle nil Value
	value := msg.Value
	if value == nil {
		value = new(big.Int)
	}

	// Handle nil blob gas fee cap
	blobGasFeeCap := msg.BlobGasFeeCap
	if blobGasFeeCap == nil {
		blobGasFeeCap = new(big.Int)
	}

	return &core.Message{
		From:                  msg.From,
		To:                    msg.To,
		Value:                 uint256.MustFromBig(value),
		Nonce:                 0, // CallMsg doesn't have a nonce
		GasLimit:              msg.Gas,
		GasPrice:              uint256.MustFromBig(gasPrice),
		GasFeeCap:             uint256.MustFromBig(gasFeeCap),
		GasTipCap:             uint256.MustFromBig(gasTipCap),
		Data:                  msg.Data,
		AccessList:            msg.AccessList,
		BlobGasFeeCap:         uint256.MustFromBig(blobGasFeeCap),
		BlobHashes:            msg.BlobHashes,
		SetCodeAuthorizations: msg.AuthorizationList,
		SkipNonceChecks:       skipNonceCheck,
		SkipTransactionChecks: skipTxCheck,
	}
}

// Call executes a read-only call (eth_call semantics).
// An empty revert is treated as a non-error: many Ethereum tools probe contracts this way.
func (h *Executor) Call(msg ethereum.CallMsg) ([]byte, error) {
	ret, err := h.execute(callMsgToMessage(msg, h.BlockCtx.BaseFee, true, true))
	if errors.Is(err, vm.ErrExecutionReverted) && len(ret) == 0 {
		return nil, nil // empty revert on a call is not an error
	}
	return ret, err
}

// PrepareMessage is the transaction gate: it recovers the sender (validating the
// signature), checks the nonce against ledger state, and converts tx to a
// core.Message. Exported for testimpl wrappers that build a message without the
// production free-gas defaults.
func (h *Executor) PrepareMessage(tx *types.Transaction) (*core.Message, error) {
	from, err := types.Sender(h.signer, tx)
	if err != nil {
		return nil, err
	}

	// Validate that the transaction nonce matches the ledger state nonce.
	// This adds an explicit read dependency on the ledger key for the nonce.
	ledgerNonce := h.state.GetNonce(from)
	if tx.Nonce() < ledgerNonce {
		return nil, core.ErrNonceTooLow
	} else if tx.Nonce() > ledgerNonce {
		return nil, core.ErrNonceTooHigh
	}

	return core.TransactionToMessage(tx, h.signer, h.BlockCtx.BaseFee)
}

// Send validates nonce, converts tx to a message, applies production defaults, and executes.
func (h *Executor) Send(tx *types.Transaction) ([]byte, error) {
	msg, err := h.PrepareMessage(tx)
	if serr := h.state.Error(); serr != nil {
		// A backing-store read failed while preparing (e.g. a stale query-service
		// view during the nonce read). The prepare verdict (including any nonce
		// rejection) was computed from missing state, so surface the read error
		// itself -- NOT a *TxRejected -- so the caller aborts the batch and
		// retries on a fresh view instead of spuriously excluding the tx.
		return nil, serr
	}
	if err != nil {
		// Invalid transaction rejected before execution (bad signature, nonce, ...).
		return nil, &TxRejected{err: err}
	}

	// Return the raw EVM result: on a revert that ret is the revert data (used to
	// build the revert event); on other faults geth leaves it empty.
	return h.execute(msg)
}

// execute applies production defaults then runs the EVM via ApplyMessage.
// Gas prices are always zeroed (free gas) so buyGas never requires ETH balance.
// If MaxTxGas is set, msg.GasLimit is capped before execution.
func (h *Executor) execute(msg *core.Message) ([]byte, error) {
	if msg.GasLimit == 0 {
		msg.GasLimit = 5_000_000
	}

	// Free gas: zero all prices so buyGas never requires ETH balance from the sender.
	msg.GasPrice = new(uint256.Int)
	msg.GasFeeCap = new(uint256.Int)
	msg.GasTipCap = new(uint256.Int)

	// Cap gas limit for DoS protection.
	if h.maxTxGas > 0 && msg.GasLimit > h.maxTxGas {
		msg.GasLimit = h.maxTxGas
	}

	return h.ApplyMessage(msg)
}

// ApplyMessage runs msg on the EVM exactly as provided, without production defaults.
// Use this in test infrastructure (testimpl) when real gas pricing is needed.
func (h *Executor) ApplyMessage(msg *core.Message) ([]byte, error) {
	// Reuse the bound EVM if one was primed (batch path); otherwise build a
	// fresh one (single-tx path). stateTransition sets a fresh TxContext per tx,
	// so a reused EVM starts each tx clean.
	evm := h.evm
	if evm == nil {
		evm = vm.NewEVM(h.BlockCtx, h.state, h.ChainCfg, vm.Config{})
	}

	// Snapshot before execution mirrors geth's approach and allows reverting on error.
	snapshot := h.state.Snapshot()

	// The block gas pool must reflect the enclosing block gas limit, not the tx gas
	// limit. Otherwise a tx with gas limit above the block gas limit incorrectly
	// passes preCheck and executes.
	gp := core.NewGasPool(h.BlockCtx.GasLimit)

	// Use ApplyMessage to execute the transaction
	result, err := core.ApplyMessage(evm, msg, gp)
	if serr := h.state.Error(); serr != nil {
		// A backing-store read failed mid-execution (e.g. a stale query-service
		// view whose lifetime the batch exceeded). The EVM ran on zero-valued
		// reads, so both result and err are meaningless. Discard them and
		// surface the read error -- NOT a *TxRejected or *ExecFailure -- so the
		// caller aborts the batch and retries on a fresh view.
		h.state.RevertToSnapshot(snapshot)
		return nil, serr
	}
	if err != nil {
		// Pre-execution rejection: the message can't be applied to this state
		// (nonce, funds, intrinsic gas, ...) and would never be accepted in a block.
		// Snapshot revert mirrors geth.
		h.state.RevertToSnapshot(snapshot)
		return nil, &TxRejected{err: err}
	}

	if result.Err != nil {
		// The transaction is valid but its EVM execution failed. A revert is a
		// committed outcome carrying a reason; other faults (out of gas, invalid
		// opcode, ...) are ExecFailures. Both are distinct from a pre-execution
		// rejection.
		if errors.Is(result.Err, vm.ErrExecutionReverted) {
			if reason, uErr := abi.UnpackRevert(result.ReturnData); uErr == nil {
				return result.ReturnData, fmt.Errorf("%w: %v", vm.ErrExecutionReverted, reason)
			}
			return result.ReturnData, result.Err
		}
		return result.ReturnData, &ExecFailure{err: result.Err}
	}
	return result.ReturnData, nil
}

// TxRejected tags an invalid transaction rejected before execution (nonce, funds,
// intrinsic gas, signature, ...): it can never be included in a block, so the
// caller sees an error rather than an endorsement.
type TxRejected struct{ err error }

// NewTxRejected wraps err as a TxRejected fault. Exported for tests and callers
// outside this package that need to construct one (the err field itself stays
// unexported so callers always go through Unwrap()).
func NewTxRejected(err error) *TxRejected { return &TxRejected{err: err} }

func (e *TxRejected) Error() string { return e.err.Error() }
func (e *TxRejected) Unwrap() error { return e.err }

// ExecFailure tags a valid transaction whose EVM execution faulted (out of gas,
// invalid opcode, ...) — in Ethereum it is mined with a failed receipt, distinct
// from a revert (which carries a reason) and from a pre-execution rejection. Both
// wrap the original go-ethereum error so errors.Is/errors.As still match the
// underlying value (e.g. vm.ErrOutOfGas).
type ExecFailure struct{ err error }

// NewExecFailure wraps err as an ExecFailure fault. Exported for tests and
// callers outside this package; see NewTxRejected for why err stays unexported.
func NewExecFailure(err error) *ExecFailure { return &ExecFailure{err: err} }

func (e *ExecFailure) Error() string { return e.err.Error() }
func (e *ExecFailure) Unwrap() error { return e.err }
