/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package app

import (
	"fmt"
	"os"

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
	"google.golang.org/grpc"
)

// defaultQueryServiceConnections is the endorser->query-service gRPC pool size used
// when config leaves query-service-connections unset. The warm pass fires a whole
// batch's reads concurrently; a single connection serializes them behind one HTTP/2
// transport. A perf sweep (bs=1024, window=20000) measured throughput vs pool size —
// 4212 tx/s at 1 conn, rising to a ~5040 tx/s plateau at 4-16 conns (+19.6% peak at
// 8) and regressing at 32 as per-connection overhead outweighs the added parallelism.
// 8 sits at the peak with margin below the regression point. The query service serves
// a view by id independent of connection, so spreading reads across the pool is safe.
const defaultQueryServiceConnections = 8

// NewEndorserCore builds the endorser engine, its read store, and its endorsement
// builder — the construction shared by a production endorser (see NewEndorser) and the
// in-process test harness/testnode, which manage block delivery themselves. It does not
// resolve a signer from MSP; callers own that.
//
// In "query-service" mode the returned store reads committed state on demand from the
// query service and the returned *storage.RevertibleLightKVS is nil. In "memory" mode
// the store reads from an in-process RevertibleLightKVS, which is also returned so the
// caller can register it as a block handler and use its revert API (test RPC).
//
// cacheWrap, if non-nil, is applied to the store before it is handed to the EVM engine,
// layering the gateway's cross-batch VersionedCache over the read path (see
// gateway/core.NewCachedSnapshotter). nil means identity -- the engine reads the store
// directly. endorser/app cannot import gateway/core (that would invert the dependency),
// so the cache-wrapping closure is built and owned by the caller that wires up both the
// endorsers and the gateway sharing one cache instance.
func NewEndorserCore(
	cfg config.Endorser,
	channel, namespace, protocol string,
	signer sdk.Signer,
	evmConfig execution.EVMConfig,
	testImpl bool,
	cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter,
) (*core.Endorser, execution.KVSSnapshotter, *storage.RevertibleLightKVS, endorsement.Builder, error) {
	var store execution.KVSSnapshotter
	var back *storage.RevertibleLightKVS
	switch cfg.Database.Database {
	case "query-service":
		// 0 (unset) opens the tuned default-sized pool; an explicit positive value
		// overrides it (set 1 to force the pre-pool single shared connection).
		n := cfg.QueryServiceConnections
		if n <= 0 {
			n = defaultQueryServiceConnections
		}
		conns := make([]*grpc.ClientConn, 0, n)
		for i := range n {
			conn, err := query.Dial(cfg.QueryService)
			if err != nil {
				for _, c := range conns {
					_ = c.Close()
				}
				return nil, nil, nil, nil, fmt.Errorf("failed to dial query service (connection %d/%d): %w", i+1, n, err)
			}
			conns = append(conns, conn)
		}
		qStore := query.NewStore(query.NewGRPCClientPool(conns, cfg.ViewTimeout), namespace)
		// EXPERIMENT (default OFF): EVM_QS_NIL_VIEW makes state reads use the query
		// service's non-consistent (nil-view) path -- current committed state per
		// read on a fresh connection -- instead of a BeginView snapshot shared across
		// the service's view-aggregation window. The pinned aggregation snapshot is
		// the source of the pipelined-auth stale read; nil-view removes it, relying on
		// the read-cache + write-cache for intra-pass consistency. See query.Store.nilView.
		if os.Getenv("EVM_QS_NIL_VIEW") != "" {
			qStore.SetNilView(true)
		}
		store = qStore
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

	snap := execution.KVSSnapshotter(store)
	if cacheWrap != nil {
		snap = cacheWrap(snap)
	}

	end, err := core.New(
		execution.NewEVMEngine(namespace, snap, evmConfig, monotonicVersions),
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
//
// cacheWrap is forwarded to NewEndorserCore -- see its doc comment. nil means identity.
func NewEndorser(
	cfg config.Endorser,
	network common.Network,
	signer sdk.Signer,
	logger sdk.Logger,
	testImpl bool,
	cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter,
) (*core.Endorser, *storage.RevertibleLightKVS, error) {
	evmConfig := execution.EVMConfig{
		ChainConfig: common.BuildChainConfig(network.ChainID),
		MaxTxGas:    network.MaxTxGas,
		DebugLogs:   cfg.DebugLogs,
	}

	end, _, back, _, err := NewEndorserCore(cfg, network.Channel, network.Namespace, network.Protocol, signer, evmConfig, testImpl, cacheWrap)
	if err != nil {
		return nil, nil, err
	}

	return end, back, nil
}
