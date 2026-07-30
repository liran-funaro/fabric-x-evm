/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-evm/common"
	eapi "github.com/hyperledger/fabric-x-evm/endorser/api"
	eapp "github.com/hyperledger/fabric-x-evm/endorser/app"
	econf "github.com/hyperledger/fabric-x-evm/endorser/config"
	ecore "github.com/hyperledger/fabric-x-evm/endorser/core"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/endorser/storage"
	gwapi "github.com/hyperledger/fabric-x-evm/gateway/api"
	"github.com/hyperledger/fabric-x-evm/gateway/app"
	"github.com/hyperledger/fabric-x-evm/gateway/config"
	"github.com/hyperledger/fabric-x-evm/gateway/core"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	bfab "github.com/hyperledger/fabric-x-sdk/blocks/fabric"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"github.com/hyperledger/fabric-x-sdk/fabrictest"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/local"
	"github.com/hyperledger/fabric-x-sdk/network"
	nfab "github.com/hyperledger/fabric-x-sdk/network/fabric"
	nfabx "github.com/hyperledger/fabric-x-sdk/network/fabricx"
	"github.com/hyperledger/fabric-x-sdk/notification"
	"google.golang.org/protobuf/proto"
)

// GetERC20BalanceSlot computes the storage slot for a balance in an ERC-20 mapping(address => uint256).
// This uses the Solidity storage layout: keccak256(abi.encodePacked(address, mappingPosition))
func GetERC20BalanceSlot(account ethcommon.Address, mappingPosition uint64) ethcommon.Hash {
	// Concatenate: address (32 bytes) + mapping position (32 bytes)
	data := append(
		ethcommon.LeftPadBytes(account.Bytes(), 32),
		ethcommon.LeftPadBytes(new(big.Int).SetUint64(mappingPosition).Bytes(), 32)...,
	)
	return crypto.Keccak256Hash(data)
}

type localSigner struct{}

func (localSigner) Sign(msg []byte) ([]byte, error) {
	return []byte("signature"), nil
}

func (localSigner) Serialize() ([]byte, error) {
	return proto.Marshal(&msp.SerializedIdentity{Mspid: "test-msp", IdBytes: []byte("serialised identity")})
}

// NewStatePrimer returns a reset StatePrimer ready for a new batch of state operations.
// Can be called at any time during tests.
//
// Example usage:
//
//	primer, err := th.NewStatePrimer()
//	err = primer.SetNonce(addr1, 5).SetCode(addr2, contractCode).Commit(ctx)
func (th *TestHarness) NewStatePrimer() (*StatePrimer, error) {
	return th.Primer.Reset()
}

// PrimeStateFromJSON builds a proposal that contains a RWSet derived from the contents of
// `jsonFilePath` as the chaincode results, creates a ProposalResponses signed by the given
// endorsers and submits them via the submitter. This causes Fabric peers to apply the state
// through normal commit flow.
//
// This is a convenience wrapper around NewStatePrimer().LoadFromJSON().Commit().
func (th *TestHarness) PrimeStateFromJSON(ctx context.Context, jsonFilePath string, wait bool) error {
	// bail if no file is given
	if jsonFilePath == "" {
		return nil
	}

	primer, err := th.NewStatePrimer()
	if err != nil {
		return err
	}
	primer, err = primer.LoadFromJSON(jsonFilePath)
	if err != nil {
		return err
	}
	return primer.Commit(ctx, wait)
}

// buildTestHarness is the shared implementation for all test harness constructors.
// It builds endorsers, a gateway, and primes state.
//
// The gateway signer and identity deserializer are derived from cfg:
//   - cfg.Gateway.SignerMSPDir set → MSP-based signer; empty → local mock
//   - cfg.Endorsers[0].MspDir set → FabricDeserializer; empty → local mock
//
// Sync goroutines are started in the background using ctx. The returned synchronizers
// can be used by callers that need to wait for the initial sync to complete.
//
// If useNotifications is true, uses NotificationDispatcher + MemoryStore instead of
// Synchronizer + Chain. This is intended for fabric-x performance testing.
//
// cache is the VersionedCache shared with endorsers (the SAME instance the caller passed
// as cacheWrap to prepareHarnessConfig/buildEndorsers) -- passed straight through to
// app.BuildGateway so the harness's one gateway and its endorsers agree on one cache.
func buildTestHarness(t *testing.T, logger sdk.Logger, cfg config.Config, evmConfig execution.EVMConfig, primeDBPath string, bypass bool, endorsers []EndorserComponents, useNotifications bool, cache *core.VersionedCache) (*TestHarness, *network.Synchronizer, error) {
	return buildTestHarnessWithExtraHandler(t, logger, cfg, evmConfig, primeDBPath, bypass, endorsers, useNotifications, nil, cache, nil, nil)
}

// NotifierControl is the test's handle on a notifier-mode local harness. The
// gateway is NOT a block handler here; the test resolves in-flight batches by
// calling Handler.Handle (the same path the real notification Processor uses).
type NotifierControl struct {
	Handler notification.TxStatusHandler // gw.SetNotifier(...) result; deliver events through it
	mu      sync.Mutex
	watched []string // fabric TxIDs the gateway Watch()ed, in submission order
}

// Watched returns a copy of the fabric TxIDs the gateway has registered so far,
// in submission order. With SetMaxBatchSize(1), Watched()[k] is the k-th batch.
func (c *NotifierControl) Watched() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.watched...)
}

// buildTestHarnessWithExtraHandler is like buildTestHarness but accepts an optional extra TxHandler
// that will be inserted into the notification handler chain right before the cleanup handler.
//
// notifierCtl, when non-nil (and useNotifications is false), selects notifier
// mode: the gateway is dropped from the block-handler list (endorser DBs + chain
// still feed state and receipts on the block stream) and gw.SetNotifier is wired
// over a test-controlled channel, so the TEST is the sole in-flight resolver via
// notifierCtl.Handler.Handle. nil = existing behavior.
//
// submitterWrap, when non-nil, wraps each freshly-built network/local submitter
// before the gateway's BatchSubmitter is constructed, letting a test intercept
// orderer submission (order/buffering). nil = existing behavior.
func buildTestHarnessWithExtraHandler(t *testing.T, logger sdk.Logger, cfg config.Config, evmConfig execution.EVMConfig, primeDBPath string, bypass bool, endorsers []EndorserComponents, useNotifications bool, extraHandler common.TxHandler, cache *core.VersionedCache, notifierCtl *NotifierControl, submitterWrap func(core.Submitter) core.Submitter) (*TestHarness, *network.Synchronizer, error) {
	dbs := make([]storage.KVS, len(endorsers))
	readStores := make([]execution.KVSSnapshotter, len(endorsers))
	builders := make([]endorsement.Builder, len(endorsers))
	ends := make([]eapi.Service, len(endorsers))
	for i, e := range endorsers {
		dbs[i], readStores[i], builders[i], ends[i] = e.KVS, e.ReadStore, e.Builder, e.Service
	}

	// Build gateway signer.
	var gwSigner sdk.Signer
	if cfg.Gateway.Identity.MSPDir != "" {
		var err error
		gwSigner, err = identity.SignerFromMSP(cfg.Gateway.Identity.MSPDir, cfg.Gateway.Identity.MspID)
		if err != nil {
			return nil, nil, err
		}
	} else {
		gwSigner = localSigner{}
	}

	chain, err := core.NewChain(cfg.Gateway.Database.ConnString, cfg.Gateway.Database.TriePath, false)
	if err != nil {
		return nil, nil, err
	}
	if !useNotifications {
		t.Cleanup(func() { chain.Close() })
	}

	// Build submitters (one per worker for parallel submission)
	orderers := make([]network.OrdererConf, len(cfg.Gateway.Orderers))
	for i, o := range cfg.Gateway.Orderers {
		orderers[i] = o.ToOrdererConf()
	}

	submitterCount := cfg.Gateway.SubmitterCount
	if submitterCount <= 0 {
		submitterCount = core.DefaultNumWorkers
	}

	var submitters []core.Submitter
	var sync *network.Synchronizer

	if bypass {
		// Use local submitters for bypass mode (no network communication)
		submitters = make([]core.Submitter, submitterCount)
		for i := 0; i < submitterCount; i++ {
			submitters[i] = local.NewLocalSubmitter(dbs[0], cfg.Network.Channel, cfg.Network.Namespace, nfab.NewTxPackager(gwSigner), bfab.NewBlockParser(logger), false)
		}
	} else {
		// Create network submitters
		submitters, err = app.NewNetworkSubmitters(t.Context(), cfg.Network.Protocol, orderers, gwSigner, submitterCount, logger)
		if err != nil {
			return nil, nil, err
		}
	}

	if submitterWrap != nil {
		for i := range submitters {
			submitters[i] = submitterWrap(submitters[i])
		}
	}

	// Create gateway before synchronizer so we can register it as a handler
	// Gateway owns the BatchSubmitter and will handle its lifecycle
	// Enable rate limiting only for "synthetic" namespace (10 000 tx/s)
	txPerSec := 0
	if cfg.Network.Namespace == "synthetic" {
		txPerSec = 10000
	}
	gw, err := app.BuildGateway(t.Context(), ends, gwSigner, cfg.Network, chain, submitters, cfg.Gateway.SubmitterCount, cfg.Gateway.EndorsementChanSize, txPerSec, cfg.Gateway.MaxBatchSize, cfg.Gateway.MaxInflight, cfg.Gateway.NotifyTimeout, cfg.Gateway.Pipelined, cache)
	if err != nil {
		return nil, nil, err
	}

	// Create synchronizer with handlers (endorsers, chain, and gateway) - only for non-bypass mode
	if !bypass {
		handlers := make([]blocks.BlockHandler, 0, len(dbs)+2)
		for _, db := range dbs {
			if db != nil {
				handlers = append(handlers, db)
			}
		}
		// Add chain before gateway to ensure blocks are persisted before marking transactions complete
		handlers = append(handlers, chain)
		// Drop gw from the block handlers in notification mode (useNotifications)
		// and in notifier mode (notifierCtl != nil): in both, in-flight batches are
		// resolved per-TxID instead of by the block stream. Receipts still flow
		// through the chain + endorser DB handlers above.
		if !useNotifications && notifierCtl == nil {
			handlers = append(handlers, gw)
		}

		sync, err = app.NewGatewaySynchronizer(cfg.Network.Protocol, chain, cfg.Network.Channel, cfg.Gateway.Committer.ToPeerConf(), gwSigner, logger, handlers...)
		if err != nil {
			return nil, nil, err
		}

		if useNotifications {
			// HYBRID MODE: Use synchronizer to catch up, then switch to notifications
			syncCtx, syncCancel := context.WithCancel(t.Context())
			syncDone := make(chan struct{})
			go func() {
				defer close(syncDone)
				if err := sync.Start(syncCtx); err != nil && syncCtx.Err() == nil {
					logger.Errorf("synchronizer error during catchup: %v", err)
				}
			}()

			logger.Infof("Waiting for synchronizer to catch up...")
			if err := app.WaitUntilSynced(t.Context(), sync, 60*time.Second); err != nil {
				t.Fatal(err)
			}
			logger.Infof("Synchronizer caught up - stopping and switching to notifications")

			syncCancel()
			<-syncDone
			chain.Close()
			logger.Infof("Synchronizer stopped cleanly")

			// The AllTxStreamer keeps feeding the ENDORSER DBs their committed
			// state (in "memory" mode this is how the in-process KVS learns
			// committed state). The gateway is NO LONGER on this broadcast
			// stream: it is now resolved per-TxID by the Notifier wired below,
			// which additionally sees aborts + sidecar timeouts the all-tx stream
			// cannot deliver, plus the query-service fallback for a timeout.
			txHandlers := make([]common.TxHandler, 0, len(dbs)+1)
			for _, db := range dbs {
				if db != nil {
					txHandlers = append(txHandlers, db.(common.TxHandler))
				}
			}
			if extraHandler != nil {
				txHandlers = append(txHandlers, extraHandler)
			}

			dispatcher := common.NewAllTxBatchDispatcher(txHandlers...)

			if cfg.Network.Protocol == "fabric-x" || cfg.Network.Protocol == "" {
				peer, err := nfabx.NewPeer(cfg.Gateway.Committer.ToPeerConf(), cfg.Network.Channel, gwSigner)
				if err != nil {
					return nil, nil, fmt.Errorf("create notification peer: %w", err)
				}
				streamer := notification.NewAllTxStreamer(peer, []notification.AllTxHandler{dispatcher}, logger)
				go func() {
					req := &notification.StreamAllRequest{
						FilterNamespaces:     []string{cfg.Network.Namespace},
						IncludeReadWriteSets: true,
						IncludeMetadata:      true,
					}
					if err := streamer.Stream(t.Context(), req); err != nil && t.Context().Err() == nil {
						logger.Errorf("AllTxStreamer error: %v", err)
					}
				}()
				logger.Infof("AllTxStreamer active (endorser DBs)")

				// Per-TxID Notifier: resolves the gateway's in-flight batches by
				// their real commit/abort/timeout status. The gateway's Watch
				// pushes each submitted TxID onto subscribeCh before submit
				// (register-then-submit); the Subscribe loop below forwards them
				// to the sidecar's notification stream and routes verdicts back
				// through the gateway's txNotifier handler.
				subscribeCh := make(chan []string, 1024)

				// Query-service fallback for a sidecar timeout (STATUS_UNSPECIFIED):
				// read a key's committed version from the endorser's query view.
				// Wired only in query-service mode; left nil in "memory" mode so a
				// timeout stays a conservative rollback (never reads the in-process
				// KVS -- out of scope). The endorser ReadStore is the raw query
				// Store (uncached), so this observes real committed state.
				if len(cfg.Endorsers) > 0 && cfg.Endorsers[0].Database.Database == "query-service" && endorsers[0].ReadStore != nil {
					reader := endorsers[0].ReadStore
					ns := cfg.Network.Namespace
					gw.SetCommittedVersionReader(func(_ context.Context, key string) (uint64, bool, error) {
						view, err := reader.NewSnapshot(0)
						if err != nil {
							return 0, false, err
						}
						defer view.Close()
						rec, err := view.Get(ns, key)
						if err != nil {
							return 0, false, err
						}
						if rec == nil {
							return 0, false, nil
						}
						return rec.Version, true, nil
					})
				}

				handler := gw.SetNotifier(subscribeCh, 0) // 0 -> gateway commitTimeout client backstop
				processor := notification.NewProcessor([]notification.TxStatusHandler{handler}, logger)
				notifier := notification.NewNotifier(peer, processor)
				// Sidecar-side per-request timeout: kept below the client-side
				// backstop so a genuine stall surfaces as STATUS_UNSPECIFIED ->
				// query fallback before the client timer blindly rolls back.
				notifier.SetDefaultTimeout(30 * time.Second)
				go func() {
					if err := notifier.Subscribe(t.Context(), subscribeCh); err != nil && t.Context().Err() == nil {
						logger.Errorf("Notifier error: %v", err)
					}
				}()
				logger.Infof("per-TxID Notifier active (gateway)")
			}

			sync = nil
		} else {
			go func() error { return sync.Start(t.Context()) }()

			// Notifier mode: the gateway is NOT a block handler (dropped above), so
			// the test drives in-flight resolution by calling notifierCtl.Handler.Handle
			// directly -- exactly what the real notification Processor does. Wire the
			// gateway's txNotifier over a test-controlled channel and record every
			// Watch()ed TxID (in submission order) into notifierCtl.
			if notifierCtl != nil {
				subscribeCh := make(chan []string, 1024)
				notifierCtl.Handler = gw.SetNotifier(subscribeCh, cfg.Gateway.NotifyTimeout)
				go func() {
					for {
						select {
						case <-t.Context().Done():
							return
						case ids := <-subscribeCh:
							notifierCtl.mu.Lock()
							notifierCtl.watched = append(notifierCtl.watched, ids...)
							notifierCtl.mu.Unlock()
						}
					}
				}()
			}
		}
	}

	// Start gateway worker pool
	gw.Start(t.Context())
	t.Cleanup(func() { gw.Stop() })

	// Create state primer (use first submitter). Reads go through readStores[0] (always
	// non-nil) rather than dbs[0], which is nil in "query-service" mode.
	primer, err := NewStatePrimer(gw, submitters[0], readStores[0], cfg.Network.Namespace, gwSigner, builders, cfg.Network.Channel, cfg.Network.NsVersion, cfg.Network.Protocol == "fabric-x")
	if err != nil {
		return nil, nil, err
	}

	th := &TestHarness{
		Gateways:       []*core.Gateway{gw},
		endorsers:      ends,
		ethChainConfig: evmConfig.ChainConfig,
		Primer:         primer,
		DBs:            dbs,
	}

	// In notification mode the block store (chain) is closed above, so commit
	// checks via the gateway's SQLite can't work — priming must not wait on it.
	// The caller waits out-of-band (e.g. a short sleep before replay). In
	// block-sync mode the chain is open, so waiting is honored (unless bypass).
	waitForPrimeCommit := !bypass && !useNotifications
	if err := th.PrimeStateFromJSON(t.Context(), primeDBPath, waitForPrimeCommit); err != nil {
		return nil, nil, err
	}

	return th, sync, nil
}

// applyConfigOverrides applies overrides from a map to a config struct using reflection.
// Keys use dot notation like "Gateway.SubmitterCount" to specify nested fields.
func applyConfigOverrides(cfg *config.Config, overrides map[string]any) error {
	for key, value := range overrides {
		parts := strings.Split(key, ".")
		if len(parts) == 0 {
			return fmt.Errorf("invalid config key: %s", key)
		}

		v := reflect.ValueOf(cfg).Elem()
		for i, part := range parts {
			field := v.FieldByName(part)
			if !field.IsValid() {
				return fmt.Errorf("invalid config field: %s", key)
			}
			if i == len(parts)-1 {
				// Last part - set the value
				if !field.CanSet() {
					return fmt.Errorf("cannot set config field: %s", key)
				}
				val := reflect.ValueOf(value)
				if !val.Type().AssignableTo(field.Type()) {
					return fmt.Errorf("type mismatch for %s: expected %s, got %s", key, field.Type(), val.Type())
				}
				field.Set(val)
			} else {
				// Intermediate part - navigate deeper
				if field.Kind() != reflect.Struct {
					return fmt.Errorf("cannot navigate through non-struct field: %s", key)
				}
				v = field
			}
		}
	}
	return nil
}

// EndorserComponents bundles the pieces produced when constructing a single test-harness
// endorser: its ReadStore (what the engine reads through — always non-nil), its KVS (the
// block-handler backing store; non-nil only in "memory" mode, nil in "query-service" mode
// where state comes from the live query service), its endorsement builder (for state
// priming), and the eapi.Service used for endorsement.
type EndorserComponents struct {
	ReadStore execution.KVSSnapshotter
	KVS       storage.KVS
	Builder   endorsement.Builder
	Service   eapi.Service
}

// EndorserFactory is a function that creates an endorser along with its dependencies.
// Service is the eapi.Service interface which both *ecore.Endorser and *testimpl.EndorserWrapper implement.
// cacheWrap, if non-nil, must be applied (directly or via NewEndorser) to any
// execution.KVSSnapshotter the factory builds an EVM engine over, so that engine reads
// through the harness's shared VersionedCache -- see NewEndorser and buildTestHarness.
type EndorserFactory func(t *testing.T, ecfg econf.Endorser, channel, namespace string, evmConfig execution.EVMConfig, protocol string, cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter) EndorserComponents

// buildEndorsers creates endorsers using the provided factory function.
func buildEndorsers(t *testing.T, cfg config.Config, evmConfig execution.EVMConfig, factory EndorserFactory, cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter) []EndorserComponents {
	endorsers := make([]EndorserComponents, len(cfg.Endorsers))
	for i, ecfg := range cfg.Endorsers {
		// LightKVS needs at least one history slot to record the pre-write snapshot on
		// every Update; config files (fablo.yaml, fabx.yaml) don't set history_size.
		if ecfg.Database.HistorySize == 0 {
			ecfg.Database.HistorySize = 1
		}
		endorsers[i] = factory(t, ecfg, cfg.Network.Channel, cfg.Network.Namespace, evmConfig, cfg.Network.Protocol, cacheWrap)
	}
	return endorsers
}

// defaultEndorserFactory creates regular endorsers without wrapping. In-process harnesses
// have no live query service to read from, so they must use the memory-backed client whose
// backing store this package updates directly (see buildTestHarnessWithExtraHandler's handler
// registration) — unless the config already selects query-service, e.g. for perf tests that
// run against a real query service.
func defaultEndorserFactory(t *testing.T, ecfg econf.Endorser, channel, namespace string, evmConfig execution.EVMConfig, protocol string, cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter) EndorserComponents {
	if ecfg.Database.Database != "query-service" {
		ecfg.Database.Database = "memory"
	}
	readStore, backing, builder, end := NewEndorser(t, ecfg, channel, namespace, evmConfig, protocol, cacheWrap)
	return EndorserComponents{ReadStore: readStore, KVS: backing, Builder: builder, Service: end}
}

// prepareHarnessConfig applies configOverrides to cfg, derives evmConfig.ChainConfig from
// cfg.Network.ChainID when not already set, and builds all endorsers via factory. Shared
// tail of every harness constructor below. cacheWrap is forwarded to every endorser built
// (see EndorserFactory) so all of this harness's endorsers share the one VersionedCache
// created alongside the harness's single gateway.
func prepareHarnessConfig(t *testing.T, cfg *config.Config, evmConfig *execution.EVMConfig, configOverrides map[string]any, factory EndorserFactory, cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter) ([]EndorserComponents, error) {
	if err := applyConfigOverrides(cfg, configOverrides); err != nil {
		return nil, err
	}

	if evmConfig.ChainConfig == nil {
		evmConfig.ChainConfig = common.BuildChainConfig(cfg.Network.ChainID)
	}

	return buildEndorsers(t, *cfg, *evmConfig, factory, cacheWrap), nil
}

// NewLocalTestHarness commits updates directly to the DB, bypassing peers and orderers.
func NewLocalTestHarness(t *testing.T, logger sdk.Logger, evmConfig execution.EVMConfig, primeDbPath, networkType string, configOverrides map[string]any) (*TestHarness, error) {
	return NewLocalTestHarnessWithFactory(t, logger, evmConfig, primeDbPath, networkType, configOverrides, defaultEndorserFactory)
}

// NewLocalTestHarnessWithFactory is like NewLocalTestHarness but allows a custom endorser factory.
func NewLocalTestHarnessWithFactory(t *testing.T, logger sdk.Logger, evmConfig execution.EVMConfig, primeDbPath, networkType string, configOverrides map[string]any, factory EndorserFactory) (*TestHarness, error) {
	bypass := networkType == "bypass"

	orderer := &common.Endpoint{Host: "127.0.0.1", Port: 1337}
	peer := &common.Endpoint{Host: "127.0.0.1", Port: 1337}

	// bypass mode uses Fabric block format
	protocol := networkType
	if bypass {
		protocol = "fabric"
	}

	tname := strings.ReplaceAll(strings.ReplaceAll(t.Name(), "/", "_"), ".", "-")
	dir := t.TempDir()
	cfg := config.Config{
		Network: common.Network{
			Protocol:  protocol,
			Channel:   "mychannel",
			Namespace: "basic",
			NsVersion: "1.0",
			ChainID:   4011,
		},
		Gateway: config.Gateway{
			Database: config.DB{
				ConnString: filepath.Join(dir, tname+"gateway.db"),
				TriePath:   filepath.Join(dir, tname+"triedb.db"),
			},
			SyncTimeout: 2 * time.Second,
			Orderers: []common.ClientConfig{
				{Endpoint: orderer},
			},
			Committer: common.ClientConfig{
				Endpoint: peer,
			},
		},
		Endorsers: []econf.Endorser{
			{
				Committer: common.ClientConfig{Endpoint: peer},
				Name:      "endorser1",
				Database: econf.DB{
					Database:    "memory",
					ConnString:  filepath.Join(dir, tname+"endorser1.db"),
					HistorySize: 1,
				},
			},
		},
	}
	// Exactly one VersionedCache for this harness's one gateway, shared by every
	// endorser built below via cacheWrap -- see buildTestHarness's doc comment
	// and the THE CRITICAL INVARIANT note on gateway/core.Gateway.cache.
	cache := core.NewVersionedCache()
	cache.EnableReadOnlyCache(core.DefaultReadOnlyCacheCapacity, core.DefaultReadOnlyAdmitThreshold)
	cacheWrap := func(s execution.KVSSnapshotter) execution.KVSSnapshotter {
		return core.NewCachedSnapshotter(s, cache)
	}

	endorsers, err := prepareHarnessConfig(t, &cfg, &evmConfig, configOverrides, factory, cacheWrap)
	if err != nil {
		return nil, err
	}

	if !bypass {
		// nil RecordGetter: the fabrictest committer validates each block's read
		// versions against its OWN world-state DB, which commit() updates
		// synchronously in block order before the next block is validated. This
		// faithfully models a real fabric-x committer (block N+1 is validated only
		// after block N's writes are applied). Passing endorsers[0].KVS instead
		// would validate against the endorser DB, which the block synchronizer
		// updates ASYNCHRONOUSLY -- so a pipelined dependent tx (nonce k+1 endorsed
		// against nonce k's still-uncommitted cache write) would be validated
		// before its predecessor's block reached the endorser DB, yielding a
		// spurious MVCC version-mismatch conflict under -race. All committed state
		// (including JSON priming) flows through the orderer, so the own DB is a
		// complete, consistent validation source.
		nw, err := fabrictest.Start(t.Context(), cfg.Network.Namespace, networkType, fabrictest.Config{}, nil)
		if err != nil {
			t.Fatalf("fabrictest.Start: %v", err)
		}
		// Don't register cleanup for nw.Stop - fabrictest.Start already registers its own cleanup internally
		orderer.Port = nw.OrdererPort
		peer.Port = nw.PeerPort
	}

	th, _, err := buildTestHarness(t, logger, cfg, evmConfig, primeDbPath, bypass, endorsers, false, cache)
	if err != nil {
		return nil, err
	}

	return th, nil
}

// NewLocalTestHarnessWithNotifier builds a local (in-process fabrictest,
// memory-endorser) harness whose gateway is resolved by a test-driven notifier
// instead of the block stream. The returned NotifierControl is the test's
// control surface. configOverrides may set gateway config (e.g.
// "Gateway.MaxInflight": 4 to bound the in-flight window before Start, or
// "Gateway.NotifyTimeout": <dur> so the client timer fires -- Step 3 uses a
// short value). Mirrors NewLocalTestHarnessWithFactory but drops gw from the
// block handlers and wires gw.SetNotifier (see buildTestHarnessWithExtraHandler).
func NewLocalTestHarnessWithNotifier(t *testing.T, logger sdk.Logger, evmConfig execution.EVMConfig, networkType string, configOverrides map[string]any) (*TestHarness, *NotifierControl, error) {
	bypass := networkType == "bypass"

	orderer := &common.Endpoint{Host: "127.0.0.1", Port: 1337}
	peer := &common.Endpoint{Host: "127.0.0.1", Port: 1337}

	// bypass mode uses Fabric block format
	protocol := networkType
	if bypass {
		protocol = "fabric"
	}

	tname := strings.ReplaceAll(strings.ReplaceAll(t.Name(), "/", "_"), ".", "-")
	dir := t.TempDir()
	cfg := config.Config{
		Network: common.Network{
			Protocol:  protocol,
			Channel:   "mychannel",
			Namespace: "basic",
			NsVersion: "1.0",
			ChainID:   4011,
		},
		Gateway: config.Gateway{
			Database: config.DB{
				ConnString: filepath.Join(dir, tname+"gateway.db"),
				TriePath:   filepath.Join(dir, tname+"triedb.db"),
			},
			SyncTimeout: 2 * time.Second,
			Orderers: []common.ClientConfig{
				{Endpoint: orderer},
			},
			Committer: common.ClientConfig{
				Endpoint: peer,
			},
		},
		Endorsers: []econf.Endorser{
			{
				Committer: common.ClientConfig{Endpoint: peer},
				Name:      "endorser1",
				Database: econf.DB{
					Database:    "memory",
					ConnString:  filepath.Join(dir, tname+"endorser1.db"),
					HistorySize: 1,
				},
			},
		},
	}
	// Exactly one VersionedCache for this harness's one gateway, shared by every
	// endorser built below via cacheWrap -- see buildTestHarness's doc comment.
	cache := core.NewVersionedCache()
	cache.EnableReadOnlyCache(core.DefaultReadOnlyCacheCapacity, core.DefaultReadOnlyAdmitThreshold)
	cacheWrap := func(s execution.KVSSnapshotter) execution.KVSSnapshotter {
		return core.NewCachedSnapshotter(s, cache)
	}

	endorsers, err := prepareHarnessConfig(t, &cfg, &evmConfig, configOverrides, defaultEndorserFactory, cacheWrap)
	if err != nil {
		return nil, nil, err
	}

	if !bypass {
		// nil RecordGetter: validate against the committer's OWN synchronously-
		// committed world state, not the async-synchronized endorser DB. Essential
		// in notifier mode, where pipelined dependent txs commit before their
		// predecessors' blocks reach the endorser DB. See the identical wiring in
		// NewLocalTestHarnessWithFactory for the full rationale.
		nw, err := fabrictest.Start(t.Context(), cfg.Network.Namespace, networkType, fabrictest.Config{}, nil)
		if err != nil {
			t.Fatalf("fabrictest.Start: %v", err)
		}
		// Don't register cleanup for nw.Stop - fabrictest.Start already registers its own cleanup internally
		orderer.Port = nw.OrdererPort
		peer.Port = nw.PeerPort
	}

	ctl := &NotifierControl{}
	th, _, err := buildTestHarnessWithExtraHandler(t, logger, cfg, evmConfig, "", bypass, endorsers, false, nil, cache, ctl, nil)
	if err != nil {
		return nil, nil, err
	}

	return th, ctl, nil
}

// newFileConfigHarness loads configFile (e.g. "fablo.yaml" for Fablo, "fabx.yaml" for
// fabric-x — both connect to a real, already-running network), builds a harness against it,
// and waits for the gateway synchronizer to catch up before returning.
func newFileConfigHarness(t *testing.T, logger sdk.Logger, evmConfig execution.EVMConfig, primeDbPath, configFile string, configOverrides map[string]any) (*TestHarness, error) {
	cfg, err := config.Load(configFile)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	// Exactly one VersionedCache for this harness's one gateway -- see above.
	cache := core.NewVersionedCache()
	cache.EnableReadOnlyCache(core.DefaultReadOnlyCacheCapacity, core.DefaultReadOnlyAdmitThreshold)
	cacheWrap := func(s execution.KVSSnapshotter) execution.KVSSnapshotter {
		return core.NewCachedSnapshotter(s, cache)
	}

	endorsers, err := prepareHarnessConfig(t, &cfg, &evmConfig, configOverrides, defaultEndorserFactory, cacheWrap)
	if err != nil {
		return nil, err
	}

	th, sync, err := buildTestHarness(t, logger, cfg, evmConfig, primeDbPath, false, endorsers, false, cache)
	if err != nil {
		return nil, err
	}

	if err := app.WaitUntilSynced(t.Context(), sync, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	return th, nil
}

// NewFabricXTestHarnessWithNotifications creates a fabric-x test harness with notification-based
// transaction completion tracking instead of block-based synchronization.
// Uses MemoryStore and NotificationDispatcher for better performance in replay scenarios.
// If extraHandler is non-nil, it will be inserted into the handler chain right before the cleanup handler.
func NewFabricXTestHarnessWithNotifications(t *testing.T, logger sdk.Logger, evmConfig execution.EVMConfig, primeDbPath string, configOverrides map[string]any, factory EndorserFactory, extraHandler common.TxHandler, confFile string) (*TestHarness, error) {
	if primeDbPath != "" && !filepath.IsAbs(primeDbPath) {
		if abs, err := filepath.Abs(primeDbPath); err == nil {
			primeDbPath = abs
		}
	}
	cwd, _ := os.Getwd()
	defer os.Chdir(cwd)
	_ = os.Chdir("../")

	cfg, err := config.Load(confFile)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	// Exactly one VersionedCache for this harness's one gateway -- see above.
	cache := core.NewVersionedCache()
	cache.EnableReadOnlyCache(core.DefaultReadOnlyCacheCapacity, core.DefaultReadOnlyAdmitThreshold)
	cacheWrap := func(s execution.KVSSnapshotter) execution.KVSSnapshotter {
		return core.NewCachedSnapshotter(s, cache)
	}

	endorsers, err := prepareHarnessConfig(t, &cfg, &evmConfig, configOverrides, factory, cacheWrap)
	if err != nil {
		return nil, err
	}

	// Use buildTestHarness with useNotifications=true and extraHandler
	th, _, err := buildTestHarnessWithExtraHandler(t, logger, cfg, evmConfig, primeDbPath, false, endorsers, true, extraHandler, cache, nil, nil)
	if err != nil {
		return nil, err
	}

	return th, nil
}

// NewEndorser creates a sync-less endorser with its dependencies, for use under the
// harness's single gateway-level synchronizer topology (see buildTestHarnessWithExtraHandler).
// readStore is always non-nil: it's what the engine reads through. backing is non-nil only
// in "memory" mode (the in-process RevertibleLightKVS to feed with committed blocks); in
// "query-service" mode it's nil since state comes from the live query service.
// cacheWrap is forwarded to NewEndorserCore -- nil means identity (no cache layer).
// Exported for use by custom endorser factories.
func NewEndorser(t *testing.T, cfg econf.Endorser, channel, namespace string, evmConfig execution.EVMConfig, protocol string, cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter) (readStore execution.KVSSnapshotter, backing storage.KVS, builder endorsement.Builder, end *ecore.Endorser) {
	t.Helper()

	var signer sdk.Signer
	if cfg.Identity.MSPDir == "" {
		signer = &localSigner{}
	} else {
		var err error
		signer, err = identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
		if err != nil {
			t.Fatalf("SignerFromMSP: %v", err)
		}
	}

	end, readStore, back, builder, err := eapp.NewEndorserCore(cfg, channel, namespace, protocol, signer, evmConfig, false, cacheWrap)
	if err != nil {
		t.Fatalf("NewEndorserCore: %v", err)
	}
	if back != nil {
		t.Cleanup(func() { back.Close() })
		backing = back
	}

	return readStore, backing, builder, end
}

// TestHarness provides access to gateways and endorsers for testing.
type TestHarness struct {
	DBs            []storage.KVS
	Gateways       []*core.Gateway
	endorsers      []eapi.Service
	ethChainConfig *params.ChainConfig
	Primer         *StatePrimer
}

func (th *TestHarness) Stop() error {
	errs := []error{}
	for _, n := range th.Gateways {
		if err := n.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func processCommon(t *testing.T, gw *core.Gateway, commit bool, tx *types.Transaction) sdk.Endorsement {
	t.Helper()

	env, err := gw.ExecuteEthTx(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}

	if commit {
		if err := gw.SubmitFabricTx(t.Context(), env); err != nil {
			t.Fatal(err)
		}

		ec, err := NewNativeEthClient(gw)
		if err != nil {
			t.Fatal(err)
		}

		waitForCommitT(t, ec, tx)
	}

	return env
}

func getEndorsedTxForSmartContractCall(t *testing.T, client *EthClient, addr ethcommon.Address, gw *core.Gateway, method string, args ...any) sdk.Endorsement {
	t.Helper()
	tx, err := client.TxForCall(t.Context(), gw, &addr, method, args...)
	if err != nil {
		t.Fatal(err)
	}

	return processCommon(t, gw, false, tx)
}

func NewNativeEthClient(gw *core.Gateway) (*ethclient.Client, error) {
	// Create production RPC server (no test accounts needed for integration tests)
	rpcServer, err := gwapi.NewServer(gw)
	if err != nil {
		return nil, err
	}

	client := rpc.DialInProc(rpcServer)
	return ethclient.NewClient(client), nil
}

func deploySmartContract(t *testing.T, gw *core.Gateway, client *EthClient, args ...any) ethcommon.Address {
	t.Helper()

	ec, err := NewNativeEthClient(gw)
	if err != nil {
		t.Fatal(err)
	}

	tx, addr, err := client.txForDeploy(t.Context(), gw, args...)
	if err != nil {
		t.Fatal(err)
	}

	err = ec.SendTransaction(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}

	waitForCommitT(t, ec, tx)

	return addr
}

func callSmartContract(t *testing.T, client *EthClient, addr ethcommon.Address, gw *core.Gateway, method string, args ...any) {
	t.Helper()

	ec, err := NewNativeEthClient(gw)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := client.TxForCall(t.Context(), gw, &addr, method, args...)
	if err != nil {
		t.Fatal(err)
	}

	err = ec.SendTransaction(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}

	waitForCommitT(t, ec, tx)
}

func querySmartContract(t *testing.T, gw *core.Gateway, client *EthClient, addr ethcommon.Address, method string, params ...any) []any {
	t.Helper()

	ec, err := NewNativeEthClient(gw)
	if err != nil {
		t.Fatal(err)
	}

	args, err := client.argsForCall(&addr, method, params...)
	if err != nil {
		t.Fatal(err)
	}

	output, err := ec.CallContract(t.Context(), *args, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) == 0 {
		return []any{}
	}

	res, err := client.getResult(method, output)
	if err != nil {
		t.Fatal(err)
	}

	return res
}

// querySmartContractExpect queries all gateways in the test harness and expects the same result
func querySmartContractExpect(t *testing.T, client *EthClient, addr ethcommon.Address, th *TestHarness, expected any, method string, params ...any) {
	for _, gw := range th.Gateways {
		res := querySmartContract(t, gw, client, addr, method, params...)
		if len(res) == 0 {
			t.Errorf("expected %v, got empty result", expected)
			return
		}

		rBig, rOK := res[0].(*big.Int)
		eBig, eOK := expected.(*big.Int)
		if rOK && eOK {
			if rBig.Cmp(eBig) != 0 {
				t.Errorf("expected %v, got %v", eBig, rBig)
			}
			return
		}

		if !reflect.DeepEqual(res[0], expected) {
			t.Errorf("expected %+v, got %+v", expected, res[0])
		}
	}
}

func submit(t *testing.T, gw *core.Gateway, end sdk.Endorsement) {
	t.Helper()

	if err := gw.SubmitFabricTx(t.Context(), end); err != nil {
		t.Error(err)
	}

	ec, err := NewNativeEthClient(gw)
	if err != nil {
		t.Error(err)
	}

	// Extract the Ethereum transaction from the proposal
	tx, err := extractEthTxFromProposal(end.Proposal)
	if err != nil {
		t.Error(err)
	}

	waitForCommitT(t, ec, tx)
}

// extractEthTxFromProposal extracts the Ethereum transaction from a peer.Proposal
func extractEthTxFromProposal(proposal *peer.Proposal) (*types.Transaction, error) {
	// Unmarshal the proposal payload to get the ChaincodeProposalPayload
	payload, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal proposal payload: %w", err)
	}

	// Unmarshal the ChaincodeInvocationSpec from the input
	cis, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal chaincode invocation spec: %w", err)
	}

	// Get the args - args[0] is the proposal type, args[1] is the serialized eth tx
	args := cis.ChaincodeSpec.Input.Args
	if len(args) < 2 {
		return nil, fmt.Errorf("expected at least 2 args, got %d", len(args))
	}

	// Check that this is an EVM transaction proposal
	if len(args[0]) != 1 || args[0][0] != byte(common.ProposalTypeEVMTx) {
		return nil, fmt.Errorf("not an EVM transaction proposal")
	}

	// Unmarshal the Ethereum transaction
	var tx types.Transaction
	if err := tx.UnmarshalBinary(args[1]); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ethereum transaction: %w", err)
	}

	return &tx, nil
}

func waitForCommitT(t *testing.T, ec *ethclient.Client, tx *types.Transaction) {
	err := waitForCommit(t.Context(), ec, tx)
	if err != nil {
		t.Fatal(err)
	}
}

func waitForCommit(ctx context.Context, ec *ethclient.Client, tx *types.Transaction) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var err error

	backoff := time.Duration(0)
	iter := 0
	step := 100

	for pending := true; pending; {
		_, pending, err = ec.TransactionByHash(ctx, tx.Hash())
		if err != nil {
			if !strings.Contains(err.Error(), "not found") {
				return fmt.Errorf("waiting for tx %s to commit: %w", tx.Hash(), err)
			}
			pending = true
		}

		if pending {
			if backoff == 0 {
				runtime.Gosched()
			} else {
				time.Sleep(backoff)
			}

			iter++
			if iter%step == 0 {
				if backoff == 0 {
					backoff = time.Millisecond
				} else {
					backoff *= 2
				}
			}
		}
	}

	return nil
}

// decodeRawTransactionT decodes a raw Ethereum transaction and
// reports errors via t.Errorf instead of returning them.
func decodeRawTransactionT(t *testing.T, raw []byte) *types.Transaction {
	t.Helper()

	if len(raw) == 0 {
		t.Errorf("DecodeRawTransaction: empty raw transaction")
		return nil
	}

	var tx types.Transaction
	if err := rlp.DecodeBytes(raw, &tx); err != nil {
		t.Errorf("DecodeRawTransaction: failed to decode raw transaction: %v", err)
		return nil
	}

	return &tx
}

// TestLogger is a logger that logs to a testing.T.
type TestLogger struct {
	ID      string
	T       *testing.T
	Disable bool
}

func (tl TestLogger) Debugf(format string, v ...any) {
	tl.T.Helper()
	if !tl.Disable {
		tl.T.Logf(tl.ID+" > [DEBUG] "+format, v...)
	}
}

func (tl TestLogger) Infof(format string, v ...any) {
	tl.T.Helper()
	if !tl.Disable {
		tl.T.Logf(tl.ID+" > [INFO] "+format, v...)
	}
}

func (tl TestLogger) Warnf(format string, v ...any) {
	tl.T.Helper()
	if !tl.Disable {
		tl.T.Logf(tl.ID+" > [WARN] "+format, v...)
	}
}

func (tl TestLogger) Errorf(format string, v ...any) {
	tl.T.Helper()
	if !tl.Disable {
		tl.T.Logf(tl.ID+" > [ERROR] "+format, v...)
	}
}
