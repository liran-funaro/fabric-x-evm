/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package app

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	sdk "github.com/hyperledger/fabric-x-sdk"

	"github.com/hyperledger/fabric-x-committer/utils/connection"
	"github.com/hyperledger/fabric-x-committer/utils/deliverorderer"
	"github.com/hyperledger/fabric-x-committer/utils/ordererdial"

	"github.com/hyperledger/fabric-x-evm/gateway/config"
	"github.com/hyperledger/fabric-x-evm/gateway/core"
)

const (
	// orderedDeliveryQueueSize buffers delivered ordered blocks between the NoFT
	// stream and the drain goroutine. Its depth is logged for queue monitoring.
	orderedDeliveryQueueSize = 256
	// orderedDeliveryBackoff is the pause before reconnecting the NoFT stream after
	// a non-fatal error.
	orderedDeliveryBackoff = 500 * time.Millisecond
)

// buildOrdererDialConfig maps the gateway config into the committer's
// ordererdial.Config for the NoFT ordered-block delivery stream. TLS mode and
// the client cert/key are taken from the first orderer (all gateway.orderers
// share one User client cert/key), while CACertPaths is the UNION across every
// orderer (each orderer org has its own tlsca CA path, so reusing orderers[0]
// alone would fail the TLS handshake against the others). MSP identity is reused
// from gateway.identity with the default BCCSP; the config-block path supplies
// the orderer endpoints and channel ID.
func buildOrdererDialConfig(gw config.Gateway) (*ordererdial.Config, error) {
	if len(gw.Orderers) == 0 {
		return nil, errors.New("ordered-submit: gateway.orderers is empty")
	}
	if gw.OrderedDelivery.ConfigBlockPath == "" {
		return nil, errors.New("ordered-submit: gateway.ordered-delivery.config-block-path is empty")
	}

	seen := make(map[string]struct{})
	var caCertPaths []string
	for _, o := range gw.Orderers {
		for _, p := range o.TLS.CACertPaths {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			caCertPaths = append(caCertPaths, p)
		}
	}

	return &ordererdial.Config{
		FaultToleranceLevel: ordererdial.UnspecifiedFT, // NoFT delivery path; level is irrelevant
		TLS: connection.TLSConfig{
			Mode:        gw.Orderers[0].TLS.Mode,
			CertPath:    gw.Orderers[0].TLS.CertPath,
			KeyPath:     gw.Orderers[0].TLS.KeyPath,
			CACertPaths: caCertPaths,
		},
		LatestKnownConfigBlockPath: gw.OrderedDelivery.ConfigBlockPath,
		Identity: &ordererdial.IdentityConfig{
			MspID:  gw.Identity.MspID,
			MSPDir: gw.Identity.MSPDir,
			BCCSP:  factory.GetDefaultOpts(),
		},
	}, nil
}

// StartOrderedSubmissionGate builds the NoFT ordered-block delivery dial config
// and starts the delivery consumer for gateway.ordered-submit, returning the
// OrderGate it feeds. Returns (nil, nil) when ordered-submit is disabled, so
// callers can install the (possibly nil) gate on the batch submitter
// unconditionally. Shared by the production app path (buildApp) and the perf
// test harness (integration.buildTestHarnessWithExtraHandler); both run against
// a real orderer set and must independently clamp orderer submission to a single
// worker (the gate's single-armed invariant -- SetOrderGate no-ops otherwise).
func StartOrderedSubmissionGate(ctx context.Context, gw config.Gateway, logger sdk.Logger) (*core.OrderGate, error) {
	if !gw.OrderedSubmit {
		return nil, nil
	}
	dialCfg, err := buildOrdererDialConfig(gw)
	if err != nil {
		return nil, fmt.Errorf("ordered-submit: build dial config: %w", err)
	}
	gate := core.NewOrderGate()
	startOrderedDelivery(ctx, dialCfg, gw.OrderedDelivery.NextBlockNum, gate, logger)
	logger.Infof("ordered-submit enabled: gating committer-tx submission on ordered-block delivery")
	return gate, nil
}

// startOrderedDelivery launches the ordered-block delivery consumer that feeds
// the OrderGate. A producer goroutine runs the NoFT delivery stream (reconnecting
// on non-fatal errors), and a consumer goroutine drains delivered blocks and
// Observes each block's committer TxIDs on the gate. Both goroutines exit when
// ctx is done. Delivery order == the assembler's total order, so Observing tx k
// unblocks the submitter to broadcast k+1, which then lands in a strictly later
// block (see core.OrderGate and core.BatchSubmitter.submitOne).
func startOrderedDelivery(ctx context.Context, dialCfg *ordererdial.Config, nextBlock uint64, gate *core.OrderGate, logger sdk.Logger) {
	blockCh := make(chan *common.Block, orderedDeliveryQueueSize)

	// progress holds the NEXT block number to fetch (last delivered + 1); 0 means
	// "nothing delivered yet, use nextBlock". Read by the producer on reconnect so
	// a dropped stream resumes near where it left off instead of replaying from
	// nextBlock. Re-delivery of a few blocks is harmless (Observe is idempotent and
	// stale TxIDs never match a future arm), so a small overlap here is safe.
	var progress atomic.Uint64

	// Producer: run the NoFT delivery stream, reconnecting on non-fatal errors.
	go func() {
		for {
			resume := nextBlock
			if p := progress.Load(); p > 0 {
				resume = p
			}
			err := deliverorderer.ToQueueWithNoFT(ctx, deliverorderer.NoFTParameters{
				ClientConfig: dialCfg,
				NextBlockNum: resume,
				OutputBlock:  blockCh,
			})
			if ctx.Err() != nil {
				logger.Infof("ordered-delivery: stream stopped: %v", ctx.Err())
				return
			}
			logger.Warnf("ordered-delivery: stream ended (%v); reconnecting in %v", err, orderedDeliveryBackoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(orderedDeliveryBackoff):
			}
		}
	}()

	// Consumer: drain delivered blocks into the gate, tracking progress and queue depth.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case b := <-blockCh:
				if depth := len(blockCh); depth > orderedDeliveryQueueSize/2 {
					logger.Warnf("ordered-delivery: drain queue depth=%d/%d (submitter falling behind?)", depth, orderedDeliveryQueueSize)
				}
				gate.Observe(core.OrderedBlockTxIDs(b))
				if b.Header != nil {
					progress.Store(b.Header.Number + 1)
				}
			}
		}
	}()
}
