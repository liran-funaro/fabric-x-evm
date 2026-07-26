/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package common

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/notification"
	"google.golang.org/protobuf/proto"
)

// TxNotification contains all data needed to process a transaction notification.
// EthTxBytes and EthTxHash come from the cache; NsRWS and Events come from the AllTxStreamer event.
type TxNotification struct {
	// From notification service
	BlockNum   uint64
	TxNum      uint64
	FabricTxID string
	Status     committerpb.Status

	// From cache
	EthTxBytes []byte
	EthTxHash  common.Hash // pre-computed; handlers that only need the hash skip UnmarshalBinary

	// EvmTxCount is how many EVM transactions this committer (Fabric) tx carries:
	// 1 for a legacy single-tx envelope, N for a merged batch (ProposalTypeEVMBatch).
	// Derived from the invocation Args in the tx metadata, so it is available even
	// though a merged batch emits no per-tx _event_ blind write. Throughput
	// measurement sums it to report EVM tx/s rather than committer tx/s.
	EvmTxCount int

	// From AllTxStreamer (IncludeReadWriteSets must be true)
	NsRWS  []blocks.NsReadWriteSet
	Events []byte
}

var notifLogger = flogging.MustGetLogger("evm.notification")

// TxHandler defines the interface for handlers that process transaction notifications in batches.
// Both gateway and endorser components implement this to receive committed-transaction
// notifications over the AllTxStreamer path (an alternative to the block-synchronizer path).
type TxHandler interface {
	HandleTx(ctx context.Context, notifs []TxNotification) error
}

// AllTxBatchDispatcher implements notification.AllTxHandler. It bridges AllTxStreamer
// (which delivers every committed transaction) to the internal TxHandler chain.
//
// For each committed transaction event it:
//  1. Extracts the Ethereum transaction bytes from the event metadata.
//  2. Converts the event's Namespaces to NsRWS + Events.
//  3. Builds a TxNotification and dispatches it to all registered TxHandlers.
type AllTxBatchDispatcher struct {
	handlers []TxHandler
}

// NewAllTxBatchDispatcher creates a dispatcher that extracts EthTxBytes from event metadata
// and dispatches them as TxNotifications to the handler chain.
func NewAllTxBatchDispatcher(handlers ...TxHandler) *AllTxBatchDispatcher {
	return &AllTxBatchDispatcher{handlers: handlers}
}

// HandleBatch implements notification.AllTxHandler.
func (d *AllTxBatchDispatcher) HandleBatch(ctx context.Context, batch notification.AllTxBatch) error {
	notifLogger.Debugf("[BLOCK] block=%d total_events=%d", batch.BlockNumber, len(batch.Events))

	notifs := make([]TxNotification, 0, len(batch.Events))
	for _, event := range batch.Events {
		// Extract the first Ethereum transaction and the EVM-tx count from metadata.
		ethTxBytes, evmTxCount, err := extractEthTxFromMetadata(event.Metadata)
		if err != nil {
			notifLogger.Debugf("Skipping tx %s: %v", event.TxID, err)
			continue
		}

		var ethTx types.Transaction
		if err := ethTx.UnmarshalBinary(ethTxBytes); err != nil {
			return fmt.Errorf("unmarshal eth tx for %s: %w", event.TxID, err)
		}

		nsrws, events := namespacesToNsRWS(event.Namespaces)

		notifs = append(notifs, TxNotification{
			BlockNum:   event.BlockNum,
			TxNum:      uint64(event.TxNum),
			FabricTxID: event.TxID,
			Status:     event.Status,
			EthTxBytes: ethTxBytes,
			EthTxHash:  ethTx.Hash(),
			EvmTxCount: evmTxCount,
			NsRWS:      nsrws,
			Events:     events,
		})
	}

	if len(notifs) == 0 {
		return nil
	}

	notifLogger.Debugf("[NOTIFY] block=%d dispatching=%d/%d txs", batch.BlockNumber, len(notifs), len(batch.Events))

	for _, h := range d.handlers {
		if err := h.HandleTx(ctx, notifs); err != nil {
			panic(fmt.Errorf("handler failed: %w", err))
		}
	}

	return nil
}

// extractEthTxFromMetadata extracts, from the event metadata, the first Ethereum
// transaction bytes and the count of EVM transactions the committer tx carries.
// Metadata[0] is the marshaled ChaincodeInput: Args[0] is the proposal-type byte
// and Args[1:] are the eth tx bytes (one for a single-tx envelope, N for a merged
// batch), so Args[1] is the first tx and len(Args)-1 is the EVM-tx count.
func extractEthTxFromMetadata(metadata [][]byte) ([]byte, int, error) {
	if len(metadata) == 0 {
		return nil, 0, fmt.Errorf("no metadata")
	}

	var input peer.ChaincodeInput
	if err := proto.Unmarshal(metadata[0], &input); err != nil {
		return nil, 0, fmt.Errorf("unmarshal input: %w", err)
	}

	if len(input.Args) < 2 {
		return nil, 0, fmt.Errorf("insufficient args: %d", len(input.Args))
	}

	return input.Args[1], len(input.Args) - 1, nil
}

// namespacesToNsRWS converts applicationpb.TxNamespace slices (as delivered by
// AllTxStreamer) into the blocks.NsReadWriteSet format used internally.
// It also extracts the special _event_ key as raw event bytes.
func namespacesToNsRWS(namespaces []*applicationpb.TxNamespace) ([]blocks.NsReadWriteSet, []byte) {
	nsrws := make([]blocks.NsReadWriteSet, 0, len(namespaces))
	var events []byte

	for _, ns := range namespaces {
		rws := blocks.ReadWriteSet{
			Reads:  make([]blocks.KVRead, 0),
			Writes: make([]blocks.KVWrite, 0),
		}

		for _, r := range ns.ReadsOnly {
			kvRead := blocks.KVRead{Key: string(r.Key)}
			if r.Version != nil {
				kvRead.Version = &blocks.Version{BlockNum: *r.Version}
			}
			rws.Reads = append(rws.Reads, kvRead)
		}

		for _, rw := range ns.ReadWrites {
			kvRead := blocks.KVRead{Key: string(rw.Key)}
			if rw.Version != nil {
				kvRead.Version = &blocks.Version{BlockNum: *rw.Version}
			}
			rws.Reads = append(rws.Reads, kvRead)
			rws.Writes = append(rws.Writes, blocks.KVWrite{
				Key:   string(rw.Key),
				Value: rw.Value,
			})
		}

		for _, w := range ns.BlindWrites {
			key := string(w.Key)
			switch key {
			case "_event_":
				events = w.Value
			case "_input_":
				// skip
			default:
				rws.Writes = append(rws.Writes, blocks.KVWrite{Key: key, Value: w.Value})
			}
		}

		nsrws = append(nsrws, blocks.NsReadWriteSet{Namespace: ns.NsId, RWS: rws})
	}

	return nsrws, events
}
