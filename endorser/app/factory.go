/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package app

import (
	"fmt"

	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/config"
	"github.com/hyperledger/fabric-x-evm/endorser/core"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/endorser/query"
	"github.com/hyperledger/fabric-x-evm/endorser/storage"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	efabx "github.com/hyperledger/fabric-x-sdk/endorsement/fabricx"
)

// NewEndorserCore builds the endorser engine, its read store, and its endorsement
// builder — the construction shared by a production endorser (see NewEndorser) and the
// in-process test harness/testnode, which manage block delivery themselves. It does not
// resolve a signer from MSP; callers own that.
//
// In "query-service" mode the returned store reads committed state on demand from the
// query service and the returned *storage.RevertibleLightKVS is nil. In "memory" mode
// the store reads from an in-process RevertibleLightKVS, which is also returned so the
// caller can register it as a block handler and use its revert API (test RPC).
func NewEndorserCore(
	cfg config.Endorser,
	channel, namespace, protocol string,
	signer sdk.Signer,
	evmConfig execution.EVMConfig,
	testImpl bool,
) (*core.Endorser, execution.KVSSnapshotter, *storage.RevertibleLightKVS, endorsement.Builder, error) {
	var store execution.KVSSnapshotter
	var back *storage.RevertibleLightKVS
	switch cfg.Database.Database {
	case "query-service":
		conn, err := query.Dial(cfg.QueryService)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to dial query service: %w", err)
		}
		store = query.NewStore(query.NewGRPCClient(conn, cfg.ViewTimeout), namespace)
	case "memory":
		back = storage.NewRevertibleLightKVS(storage.NewLightKVS(cfg.Database.HistorySize))
		store = query.NewStore(query.NewMemClient(back), namespace)
	default:
		return nil, nil, nil, nil, fmt.Errorf("invalid endorser database type %s, must be query-service or memory", cfg.Database.Database)
	}

	var builder endorsement.Builder
	var monotonicVersions bool
	switch protocol {
	case "fabric-x":
		builder = efabx.NewEndorsementBuilder(signer)
		monotonicVersions = true
	default: // "fabric" or ""
		builder = efab.NewEndorsementBuilder(signer)
	}

	end, err := core.New(
		execution.NewEVMEngine(namespace, store, evmConfig, monotonicVersions),
		builder,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create endorser: %w", err)
	}

	return end, store, back, builder, nil
}

// NewEndorser creates a single embedded endorser instance and resolves an MSP-based
// signer. This is the canonical way to create a production endorser. Block delivery
// (query-service reads, or feeding the returned RevertibleLightKVS in "memory" mode) is
// owned by the caller. Returns the endorser and, in "memory" mode, the
// RevertibleLightKVS instance for state management (nil in "query-service" mode).
func NewEndorser(
	cfg config.Endorser,
	network common.Network,
	signer sdk.Signer,
	logger sdk.Logger,
	testImpl bool,
) (*core.Endorser, *storage.RevertibleLightKVS, error) {
	evmConfig := execution.EVMConfig{
		ChainConfig: common.BuildChainConfig(network.ChainID),
		MaxTxGas:    network.MaxTxGas,
		DebugLogs:   cfg.DebugLogs,
	}

	end, _, back, _, err := NewEndorserCore(cfg, network.Channel, network.Namespace, network.Protocol, signer, evmConfig, testImpl)
	if err != nil {
		return nil, nil, err
	}

	return end, back, nil
}
