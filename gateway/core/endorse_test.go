/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

type stubEndorser struct {
	callPayload []byte
	callErr     error
	nonce       uint64
	nonceErr    error
	balance     *big.Int
	storage     []byte
	code        []byte
	execResp    *peer.ProposalResponse
	execErr     error
	gotInv      endorsement.Invocation
	warmErr     error // WarmBatch returns this (with a nil handle) when set
	warmClosed  int   // incremented each time a handle from WarmBatch is Closed
}

// stubWarmed is a no-op api.WarmedBatch handle whose Close is observable by the
// stub that produced it, so tests can assert AuthBatch (or an abandoned handle)
// released the warm pass.
type stubWarmed struct{ s *stubEndorser }

func (w *stubWarmed) Close() error { w.s.warmClosed++; return nil }

func (s *stubEndorser) Execute(ctx context.Context, inv endorsement.Invocation, ethTx *types.Transaction) (*peer.ProposalResponse, error) {
	return s.execResp, s.execErr
}
func (s *stubEndorser) ExecuteBatch(ctx context.Context, inv endorsement.Invocation, txs []*types.Transaction) (*peer.ProposalResponse, error) {
	s.gotInv = inv
	return s.execResp, s.execErr
}
func (s *stubEndorser) WarmBatch(ctx context.Context, txs []*types.Transaction) (api.WarmedBatch, error) {
	if s.warmErr != nil {
		return nil, s.warmErr
	}
	return &stubWarmed{s: s}, nil
}
func (s *stubEndorser) AuthBatch(ctx context.Context, inv endorsement.Invocation, warmed api.WarmedBatch) (*peer.ProposalResponse, error) {
	s.gotInv = inv
	if warmed != nil {
		_ = warmed.Close()
	}
	return s.execResp, s.execErr
}
func (s *stubEndorser) Call(ctx context.Context, msg *ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	return s.callPayload, s.callErr
}
func (s *stubEndorser) BalanceAt(ctx context.Context, _ ethcommon.Address, _ *big.Int) (*big.Int, error) {
	return s.balance, nil
}
func (s *stubEndorser) StorageAt(ctx context.Context, _ ethcommon.Address, _ ethcommon.Hash, _ *big.Int) ([]byte, error) {
	return s.storage, nil
}
func (s *stubEndorser) CodeAt(ctx context.Context, _ ethcommon.Address, _ *big.Int) ([]byte, error) {
	return s.code, nil
}
func (s *stubEndorser) NonceAt(ctx context.Context, _ ethcommon.Address, _ *big.Int) (uint64, error) {
	return s.nonce, s.nonceErr
}

func newClient(stub *stubEndorser) *EndorsementClient {
	return &EndorsementClient{endorsers: []api.Service{stub}}
}

// stubSigner is a gateway Signer that returns fixed bytes, enough for
// NewInvocation to build a proposal without real crypto.
type stubSigner struct{}

func (stubSigner) Sign([]byte) ([]byte, error) { return []byte("sig"), nil }
func (stubSigner) Serialize() ([]byte, error)  { return []byte("creator"), nil }

// signingClient is a client wired with a signer so ExecuteTransaction can build
// an invocation.
func signingClient(stub *stubEndorser) *EndorsementClient {
	return &EndorsementClient{
		endorsers: []api.Service{stub},
		signer:    stubSigner{},
		channel:   "ch",
		namespace: "ns",
		nsVersion: "1.0",
	}
}

func TestCallContract_Status201ReturnsRevertError(t *testing.T) {
	payload := []byte{0x08, 0xc3, 0x79, 0xa0, 0xde, 0xad, 0xbe, 0xef}
	c := newClient(&stubEndorser{
		callPayload: payload,
		callErr: &common.CallError{
			Status:  common.StatusEVMRevert,
			Message: "execution reverted: out of stock",
			Data:    payload,
		},
	})

	_, err := c.CallContract(context.Background(), ethereum.CallMsg{}, nil)

	var revert *domain.RevertError
	if !errors.As(err, &revert) {
		t.Fatalf("expected *RevertError, got %T (%v)", err, err)
	}
	if revert.Reason != "execution reverted: out of stock" {
		t.Errorf("Reason = %q", revert.Reason)
	}
	if !bytes.Equal(revert.Data, payload) {
		t.Errorf("Data = %x, want %x", revert.Data, payload)
	}
	if !errors.Is(err, domain.ErrExecutionReverted) {
		t.Error("errors.Is(err, ErrExecutionReverted) = false")
	}
}

func TestCallContract_Status500IsGenericError(t *testing.T) {
	c := newClient(&stubEndorser{
		callErr: &common.CallError{Status: common.StatusServerError, Message: "endorser dead"},
	})

	_, err := c.CallContract(context.Background(), ethereum.CallMsg{}, nil)

	var revert *domain.RevertError
	if errors.As(err, &revert) {
		t.Errorf("non-revert error must not be *RevertError, got %v", revert)
	}
	if err == nil {
		t.Fatal("expected error")
	}
	var exec *domain.ExecutionError
	if errors.As(err, &exec) {
		t.Errorf("backend fault must not be *ExecutionError, got %v", exec)
	}
}

func TestCallContract_Status400ReturnsExecutionError(t *testing.T) {
	c := newClient(&stubEndorser{
		callErr: &common.CallError{Status: common.StatusExecFailure, Message: "out of gas"},
	})

	_, err := c.CallContract(context.Background(), ethereum.CallMsg{}, nil)

	var exec *domain.ExecutionError
	if !errors.As(err, &exec) {
		t.Fatalf("expected *ExecutionError, got %T (%v)", err, err)
	}
	if exec.Message != "out of gas" {
		t.Errorf("Message = %q, want %q", exec.Message, "out of gas")
	}
}

func TestCallContract_Status200ReturnsPayload(t *testing.T) {
	want := []byte{0xde, 0xad}
	c := newClient(&stubEndorser{callPayload: want})

	got, err := c.CallContract(context.Background(), ethereum.CallMsg{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("payload = %x, want %x", got, want)
	}
}

// A rejected tx (400) also maps to *ExecutionError, like a failed execution.
func TestCallContract_TxRejectedReturnsExecutionError(t *testing.T) {
	c := newClient(&stubEndorser{
		callErr: &common.CallError{Status: common.StatusTxRejected, Message: "nonce too low"},
	})

	_, err := c.CallContract(context.Background(), ethereum.CallMsg{}, nil)

	var exec *domain.ExecutionError
	if !errors.As(err, &exec) {
		t.Fatalf("expected *ExecutionError, got %T (%v)", err, err)
	}
	if exec.Message != "nonce too low" {
		t.Errorf("Message = %q, want %q", exec.Message, "nonce too low")
	}
}

// A plain (non-CallError) error is a transport failure: it is wrapped, not
// turned into a revert or execution error.
func TestCallContract_TransportErrorIsWrapped(t *testing.T) {
	c := newClient(&stubEndorser{callErr: errors.New("connection refused")})

	_, err := c.CallContract(context.Background(), ethereum.CallMsg{}, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	var revert *domain.RevertError
	if errors.As(err, &revert) {
		t.Errorf("transport error must not be *RevertError, got %v", revert)
	}
	var exec *domain.ExecutionError
	if errors.As(err, &exec) {
		t.Errorf("transport error must not be *ExecutionError, got %v", exec)
	}
	if err.Error() != "process call: connection refused" {
		t.Errorf("error = %q, want %q", err.Error(), "process call: connection refused")
	}
}

// The state readers forward straight to the endorser.
func TestEndorsementClient_StateReadersDelegate(t *testing.T) {
	stub := &stubEndorser{
		balance: big.NewInt(99),
		storage: []byte{0xaa},
		code:    []byte{0xbb},
		nonce:   5,
	}
	c := newClient(stub)
	ctx := context.Background()
	addr := ethcommon.Address{}

	if bal, _ := c.BalanceAt(ctx, addr, nil); bal.Cmp(stub.balance) != 0 {
		t.Errorf("BalanceAt = %v, want %v", bal, stub.balance)
	}
	if got, _ := c.StorageAt(ctx, addr, ethcommon.Hash{}, nil); !bytes.Equal(got, stub.storage) {
		t.Errorf("StorageAt = %x, want %x", got, stub.storage)
	}
	if got, _ := c.CodeAt(ctx, addr, nil); !bytes.Equal(got, stub.code) {
		t.Errorf("CodeAt = %x, want %x", got, stub.code)
	}
	if got, _ := c.NonceAt(ctx, addr, nil); got != stub.nonce {
		t.Errorf("NonceAt = %d, want %d", got, stub.nonce)
	}
}

// An endorsable result is assembled into the endorsement (proposal + responses).
func TestExecuteTransaction_Success(t *testing.T) {
	pResp := &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusOK}}
	c := signingClient(&stubEndorser{execResp: pResp})
	tx := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0)})

	end, err := c.ExecuteTransaction(context.Background(), tx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(end.Responses) != 1 || end.Responses[0] != pResp {
		t.Errorf("Responses = %v, want [%v]", end.Responses, pResp)
	}
	if end.Proposal == nil {
		t.Error("Proposal = nil, want non-nil")
	}
}

// A rejected tx surfaces as a Go error (the caller must fix and resubmit).
func TestExecuteTransaction_RejectedStatusErrors(t *testing.T) {
	pResp := &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusTxRejected, Message: "nonce too low"}}
	c := signingClient(&stubEndorser{execResp: pResp})
	tx := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0)})

	if _, err := c.ExecuteTransaction(context.Background(), tx); err == nil {
		t.Fatal("expected error for rejected status")
	}
}

// ExecuteBatch builds one invocation carrying the whole batch (type byte plus
// every tx's marshaled bytes, in order) and returns a single-response
// Endorsement for it.
func TestExecuteBatchBuildsMergedInvocation(t *testing.T) {
	pResp := &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusOK}}
	stub := &stubEndorser{execResp: pResp}
	c := signingClient(stub)

	tx1 := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0), Nonce: 1})
	tx2 := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0), Nonce: 2})
	txBytes1, err := tx1.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal tx1: %v", err)
	}
	txBytes2, err := tx2.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal tx2: %v", err)
	}

	end, included, terminal, rws, err := c.ExecuteBatch(context.Background(), []*types.Transaction{tx1, tx2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(end.Responses) != 1 || end.Responses[0] != pResp {
		t.Errorf("Responses = %v, want [%v]", end.Responses, pResp)
	}
	if end.Proposal == nil {
		t.Error("Proposal = nil, want non-nil")
	}
	// pResp carries no Payload, so outcomes can't be decoded: the fallback is
	// to treat every tx as included (nothing to exclude on).
	if len(included) != 2 || included[0] != tx1 || included[1] != tx2 {
		t.Errorf("included = %v, want [tx1 tx2] (fallback: no decodable outcomes -> all included)", included)
	}
	if len(terminal) != 0 {
		t.Errorf("terminal = %v, want [] (fallback: no decodable outcomes -> nothing terminal)", terminal)
	}
	// No decodable Payload -> no merged RWS.
	if len(rws.Reads) != 0 || len(rws.Writes) != 0 {
		t.Errorf("rws = %v, want empty (no decodable Payload)", rws)
	}

	args := stub.gotInv.Args
	if len(args) != 3 {
		t.Fatalf("len(Args) = %d, want 3", len(args))
	}
	if args[0][0] != byte(common.ProposalTypeEVMBatch) {
		t.Errorf("Args[0] = %v, want [%d]", args[0], byte(common.ProposalTypeEVMBatch))
	}
	if !bytes.Equal(args[1], txBytes1) {
		t.Errorf("Args[1] = %x, want %x", args[1], txBytes1)
	}
	if !bytes.Equal(args[2], txBytes2) {
		t.Errorf("Args[2] = %x, want %x", args[2], txBytes2)
	}
}

// WarmBatch+AuthBatch is the split form of ExecuteBatch and must build the same
// invocation and return the same endorsement tuple. WarmBatch builds the
// invocation (so AuthBatch reuses it), and AuthBatch closes the warmed handle.
func TestWarmThenAuthBatchMatchesExecuteBatch(t *testing.T) {
	pResp := &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusOK}}
	stub := &stubEndorser{execResp: pResp}
	c := signingClient(stub)

	tx1 := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0), Nonce: 1})
	tx2 := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0), Nonce: 2})
	txBytes1, _ := tx1.MarshalBinary()
	txBytes2, _ := tx2.MarshalBinary()

	warmed, err := c.WarmBatch(context.Background(), []*types.Transaction{tx1, tx2})
	if err != nil {
		t.Fatalf("WarmBatch: %v", err)
	}
	if warmed == nil {
		t.Fatal("WarmBatch returned a nil bundle")
	}
	// The bundle exposes the batch's txs so the executor can Release them.
	if got := warmed.Txs(); len(got) != 2 || got[0] != tx1 || got[1] != tx2 {
		t.Errorf("warmed.Txs() = %v, want [tx1 tx2]", got)
	}

	end, included, terminal, rws, err := c.AuthBatch(context.Background(), warmed)
	if err != nil {
		t.Fatalf("AuthBatch: %v", err)
	}
	// AuthBatch signs the invocation built at warm time, carrying the whole batch
	// in order (type byte + each tx's marshaled bytes) — identical to ExecuteBatch.
	args := stub.gotInv.Args
	if len(args) != 3 || args[0][0] != byte(common.ProposalTypeEVMBatch) ||
		!bytes.Equal(args[1], txBytes1) || !bytes.Equal(args[2], txBytes2) {
		t.Fatalf("auth invocation Args = %v, want [type tx1 tx2]", args)
	}
	if len(end.Responses) != 1 || end.Responses[0] != pResp {
		t.Errorf("Responses = %v, want [%v]", end.Responses, pResp)
	}
	if end.Proposal == nil {
		t.Error("Proposal = nil, want non-nil")
	}
	// Same fallback as ExecuteBatch: no decodable Payload -> all included.
	if len(included) != 2 || included[0] != tx1 || included[1] != tx2 {
		t.Errorf("included = %v, want [tx1 tx2]", included)
	}
	if len(terminal) != 0 {
		t.Errorf("terminal = %v, want []", terminal)
	}
	if len(rws.Reads) != 0 || len(rws.Writes) != 0 {
		t.Errorf("rws = %v, want empty", rws)
	}
	// AuthBatch closed the warmed handle exactly once.
	if stub.warmClosed != 1 {
		t.Errorf("warmClosed = %d, want 1 (AuthBatch closes the handle)", stub.warmClosed)
	}
}

// AuthBatch surfaces a non-OK auth response as a Go error (the executor backs
// off and re-drains), exactly as ExecuteBatch does.
func TestAuthBatchNonOKStatusErrors(t *testing.T) {
	pResp := &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusServerError, Message: "auth pass: backend gone"}}
	stub := &stubEndorser{execResp: pResp}
	c := signingClient(stub)

	tx := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0)})
	warmed, err := c.WarmBatch(context.Background(), []*types.Transaction{tx})
	if err != nil {
		t.Fatalf("WarmBatch: %v", err)
	}
	if _, _, _, _, err := c.AuthBatch(context.Background(), warmed); err == nil {
		t.Fatal("expected error for non-OK auth status")
	}
	// Even on the error path the handle was closed by the endorser.
	if stub.warmClosed != 1 {
		t.Errorf("warmClosed = %d, want 1", stub.warmClosed)
	}
}

// A nil bundle is a programming error the caller must not make; AuthBatch
// reports it rather than panicking.
func TestAuthBatchNilBundleErrors(t *testing.T) {
	c := signingClient(&stubEndorser{})
	if _, _, _, _, err := c.AuthBatch(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil warmed bundle")
	}
}

// A per-endorser warm failure closes the handles that DID open (no snapshot
// leak) and returns the error. Uses two endorsers so one succeeds and one fails.
func TestWarmBatchClosesOpenedHandlesOnError(t *testing.T) {
	good := &stubEndorser{}
	bad := &stubEndorser{warmErr: errors.New("open snapshot: db unavailable")}
	c := &EndorsementClient{
		endorsers: []api.Service{good, bad},
		signer:    stubSigner{},
		channel:   "ch",
		namespace: "ns",
		nsVersion: "1.0",
	}

	tx := types.NewTx(&types.LegacyTx{Gas: 21000, GasPrice: big.NewInt(0)})
	warmed, err := c.WarmBatch(context.Background(), []*types.Transaction{tx})
	if err == nil {
		t.Fatal("expected error when an endorser's warm fails")
	}
	if warmed != nil {
		t.Errorf("bundle = %v, want nil on error", warmed)
	}
	// The endorser that DID open a handle had it closed to avoid a snapshot leak.
	if good.warmClosed != 1 {
		t.Errorf("good.warmClosed = %d, want 1 (opened handle closed on partial failure)", good.warmClosed)
	}
}

// WarmedBatch.Close is safe on a nil bundle (the executor may Close defensively
// on paths where no bundle was produced).
func TestWarmedBatchCloseNilSafe(t *testing.T) {
	var w *WarmedBatch
	if err := w.Close(); err != nil {
		t.Errorf("nil bundle Close() = %v, want nil", err)
	}
}
