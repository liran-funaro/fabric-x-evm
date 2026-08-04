/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	fxcommon "github.com/hyperledger/fabric-x-evm/common"
	econf "github.com/hyperledger/fabric-x-evm/endorser/config"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/endorser/testimpl"
	gwcore "github.com/hyperledger/fabric-x-evm/gateway/core"
	gwtestimpl "github.com/hyperledger/fabric-x-evm/gateway/testimpl"
	"github.com/hyperledger/fabric-x-evm/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v2"
)

var gatewayConfig = flag.String("gateway-config", "../config/gateway/fabx.yaml", "gateway config file for the Fabric-X network (resolved from the integration/ dir the harness chdirs to)")
var metricsAddr = flag.String("metrics-addr", "0.0.0.0:2112", "address for Prometheus metrics endpoint")
var enableMetrics = flag.Bool("enable-metrics", false, "enable Prometheus metrics export")
var namespace = flag.String("namespace", "real", "namespace to commit transactions to")

// dataset selects the replay workload. A bare name ("synthetic", "historic")
// resolves to $EVM_PERF_DATA/USDC_dataset.<name>.json.gz when EVM_PERF_DATA is
// set (the out-of-tree data dir populated by scripts/setup.sh), else to the
// in-tree testdata/USDC_dataset.<name>.json.gz; any value with a file extension
// (e.g. "testdata/foo.json.gz", "/abs/bar.json.gz") is used as a literal path.
// The two named workloads are complementary and BOTH should be measured:
// "synthetic" is conflict-free (raw throughput ceiling), while "historic" is the
// real Jan-2020 USDC trace with an extremely high MVCC-conflict rate that
// stresses the rollback / re-batch path.
var dataset = flag.String("dataset", "synthetic", "replay workload: 'synthetic', 'historic', or a path to a .json.gz dataset")

// submitters sets how many goroutines call the gateway's SendTransaction concurrently.
// The gateway itself is now a single drain-all executor (no per-tx worker pool), so
// there is no "-workers" knob anymore: the executor drains the whole pending pool into
// one merged batch per cycle regardless of how fast txs arrive. There is likewise no
// "-outstanding" flow-control cap: that semaphore predated this redesign and existed to
// mitigate exactly the state-conflict/ordering problem the drain-all merged batch now
// removes, so the harness fires every tx and lets the pending pool absorb the backlog.
var submitters = flag.Int("submitters", 10, "number of goroutines submitting transactions to the gateway")
var orderers = flag.Int("orderers", 64, "number of goroutines submitting transactions to the orderer (BatchSubmitter workers)")

// maxBatchSize bounds how many pending EVM txs the drain-all executor folds into one
// merged committer tx per cycle (see gwcore.Gateway.SetMaxBatchSize). The harness fires
// every tx at once with no completion cap, so without a bound the first drain cycle would
// swallow the whole backlog into one oversized Fabric tx; a positive bound pipelines the
// burst across right-sized batches. Sweep this to find the throughput/latency sweet spot.
// 0 keeps the pure unbounded drain-all behavior (only sane when arrival is paced upstream).
// Default 128 keeps a batch's two-phase execution comfortably inside the query-service
// view lifetime (fabx-full.yaml requests view-timeout 5s): the warm pass issues ~one
// GetRows RPC per distinct key (see query.View.Get), so an oversized batch can't finish
// reading before its pinned view expires (which would otherwise surface as a stale-view
// read error and abort+retry the batch forever). Sweep upward while watching that batches
// still commit; batching the per-key reads would raise this ceiling substantially.
var maxBatchSize = flag.Int("max-batch-size", 128, "max EVM txs per merged committer tx (0 = unbounded drain-all)")

// pipeline selects the warm(N+1) || auth(N) pipelined executor loop over the
// serial one (see gwcore.Gateway.SetPipelined). It overlaps the I/O-bound
// concurrent warm pass of the next batch with the CPU-bound serial
// authoritative pass of the current one. Run the same dataset with -pipeline
// off (control) then on to measure the speedup. Threaded through the config
// override (Gateway.Pipelined) so BuildGateway sets it before the gateway
// Start reads it once.
var pipeline = flag.Bool("pipeline", false, "enable the warm(N+1)||auth(N) pipelined executor loop")

// maxOutstanding bounds submitted-but-not-yet-committed EVM txs (see inflightLimiter).
// 0 (the default) keeps the historical fire-everything feeder, so every measured
// config in findings.md is unaffected. A positive value turns the replay into a
// closed loop that self-paces to the sustainable rate, which is what makes a
// multi-hour run possible: the fire-everything feeder outruns the drain rate
// without bound and would exhaust the host's memory hours in.
var maxOutstanding = flag.Int("max-outstanding", 0, "max submitted-but-uncommitted EVM txs (0 = unbounded, historical behavior)")

// targetTPS paces submission to a fixed rate (0 = unpaced, full speed). This is
// additive to -max-outstanding, not an alternative: the in-flight bound still
// applies as a safety net so a dip below the target cannot grow the backlog
// without limit.
//
// It exists because DISK, not time, bounds a run on the experiment host (~6.3 KB
// per committed EVM tx across 9 replicated ledger copies => a fixed ~12M-tx
// budget), so a multi-hour run must submit below the system's ceiling to fit.
// Default off, so ordinary runs still measure the true ceiling and every number
// in findings.md stays comparable.
var targetTPS = flag.Float64("target-tps", 0, "pace submission to this many EVM txs/s (0 = unpaced, full speed)")

// TxCompletionTracker forwards all transaction completion notifications to a single channel.
// It implements common.TxHandler to receive notifications from the notification system.
type TxCompletionTracker struct {
	mu           sync.Mutex
	completionCh chan fxcommon.TxNotification
	stopped      bool
}

// NewTxCompletionTracker creates a new tracker with a completion channel.
func NewTxCompletionTracker(completionCh chan fxcommon.TxNotification) *TxCompletionTracker {
	return &TxCompletionTracker{
		completionCh: completionCh,
	}
}

// Stop prevents any further sends to the completion channel. Must be called before
// closing the channel to avoid panics from in-flight notification goroutines.
func (t *TxCompletionTracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

// HandleTx implements common.TxHandler. It receives notifications about completed transactions
// and forwards them to the completion channel.
func (t *TxCompletionTracker) HandleTx(ctx context.Context, notifs []fxcommon.TxNotification) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil
	}
	for _, notif := range notifs {
		select {
		case t.completionCh <- notif:
		default:
			// Channel full - this shouldn't happen with proper sizing
			return fmt.Errorf("completion channel full, dropping notification for tx %s", notif.EthTxHash.Hex())
		}
	}
	return nil
}

// balancePrimingEndorserFactory creates endorsers with balance priming support for testing.
func balancePrimingEndorserFactory(balancePriming *testimpl.BalancePrimingConfig) integration.EndorserFactory {
	return func(t *testing.T, ecfg econf.Endorser, channel, namespace string, evmConfig execution.EVMConfig, protocol string, cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter) integration.EndorserComponents {
		// PERF_QS_CONNS overrides the number of gRPC connections the endorser opens
		// to the query service (econf.Endorser.QueryServiceConnections) for sweeps.
		// Unset/0 keeps the production default of a single shared connection. Because
		// the warm pass fires a whole batch's reads concurrently, a pool lets those
		// reads use independent HTTP/2 transports instead of serializing behind one.
		// The view is a server-side handle, so correctness is unaffected: reads for a
		// view may be issued over any connection in the pool.
		if v := os.Getenv("PERF_QS_CONNS"); v != "" {
			var nc int
			_, err := fmt.Sscanf(v, "%d", &nc)
			require.NoError(t, err, "PERF_QS_CONNS must be a valid integer")
			require.GreaterOrEqual(t, nc, 1, "PERF_QS_CONNS must be >= 1")
			ecfg.QueryServiceConnections = nc
		}

		// Create the base endorser components
		readStore, backing, builder, baseEndorser := integration.NewEndorser(t, ecfg, channel, namespace, evmConfig, protocol, cacheWrap)

		// Extract the base EVMEngine
		baseEngine, ok := baseEndorser.Engine.(*execution.EVMEngine)
		if !ok {
			t.Fatalf("Expected *execution.EVMEngine, got %T", baseEndorser.Engine)
		}

		snap := execution.KVSSnapshotter(readStore)
		if cacheWrap != nil {
			snap = cacheWrap(snap)
		}

		// Wrap the engine with balance priming support
		wrappedEngine := testimpl.NewEVMEngineWrapper(
			namespace,
			snap,
			evmConfig,
			protocol == "fabric-x", // monotonicVersions
			baseEngine,
		)
		wrappedEngine.SetBalancePriming(balancePriming)

		// Replace the engine in the endorser
		baseEndorser.Engine = wrappedEngine

		return integration.EndorserComponents{ReadStore: readStore, KVS: backing, Builder: builder, Service: baseEndorser}
	}
}

type replayConfig struct {
	// windowSize is the number of transfers to use from the dataset.
	// 0 means use the entire dataset.
	windowSize int

	// wrapAround, when true, restarts the feed from the beginning of the
	// window after every pass. The feed continues until totalDispatches
	// transfers have been sent to workChan. Ignored when false.
	wrapAround bool

	// wrapCount is the raw wrap count requested by configuration.
	// totalDispatches is computed later, after the effective window size is known.
	wrapCount int64

	// totalDispatches is the total number of transfers to dispatch when
	// wrapAround is true. Ignored when wrapAround is false.
	totalDispatches int64

	// duration, when > 0, stops the feeder after this much wall-clock time
	// regardless of how many transfers remain. This is what makes an overnight
	// run reliable: tx/s cannot be predicted closely enough to pick a wrap count
	// that lands on a target time, but a stop time is exact. Used with
	// windowSize 0 (whole dataset) and a large wrapCount, so the duration is
	// what actually ends the run.
	duration time.Duration
}

// effectiveTotal is the denominator for progress reporting and for deciding when
// a run is complete. fedTotal is -1 while the feeder is still running and the
// count of transfers actually fed once it stops.
//
// Under a duration stop the configured totalToSubmit (window x wrapCount) is
// deliberately far larger than what will be fed, so both reporting and the
// completion condition must switch to the fed count as soon as it is known --
// otherwise a successful run never satisfies its own done condition and hangs.
func effectiveTotal(fedTotal, totalToSubmit int64) int64 {
	if fedTotal >= 0 {
		return fedTotal
	}
	return totalToSubmit
}

func loadReplayConfigFromEnv(t *testing.T) replayConfig {
	cfg := replayConfig{windowSize: 3000, wrapAround: false}

	windowSizeStr := os.Getenv("PERF_REPLAY_WINDOW_SIZE")
	if windowSizeStr != "" {
		var parsedWindowSize int
		_, err := fmt.Sscanf(windowSizeStr, "%d", &parsedWindowSize)
		assert.NoError(t, err, "PERF_REPLAY_WINDOW_SIZE must be a valid integer")
		assert.True(t, parsedWindowSize >= 0, "PERF_REPLAY_WINDOW_SIZE must be >= 0")
		cfg.windowSize = parsedWindowSize
	}

	if cfg.windowSize == 0 {
		t.Log("WARNING: full dataset mode selected — this is intended for distributed infra, not local runs")
	}

	wrapCountStr := os.Getenv("PERF_REPLAY_WRAP_COUNT")
	if wrapCountStr != "" {
		var wrapCount int64
		_, err := fmt.Sscanf(wrapCountStr, "%d", &wrapCount)
		assert.NoError(t, err, "PERF_REPLAY_WRAP_COUNT must be a valid integer")
		assert.True(t, wrapCount >= 1, "PERF_REPLAY_WRAP_COUNT must be >= 1")
		cfg.wrapCount = wrapCount
		if wrapCount > 1 {
			cfg.wrapAround = true
		}
	}

	if v := os.Getenv("PERF_REPLAY_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		assert.NoError(t, err, "PERF_REPLAY_DURATION must be a Go duration (e.g. 8h, 20m)")
		assert.True(t, d > 0, "PERF_REPLAY_DURATION must be > 0")
		cfg.duration = d
	}

	return cfg
}

//lint:ignore U1000 kept for future tests / debugging
func logMem(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] Alloc = %d MB | TotalAlloc = %d MB | Sys = %d MB | NumGC = %d\n",
		tag,
		m.Alloc/1024/1024,
		m.TotalAlloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC,
	)
}

//lint:ignore U1000 kept for future tests / debugging
func writeHeapProfile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	runtime.GC() // normalize heap before snapshot
	if err := pprof.WriteHeapProfile(f); err != nil {
		panic(err)
	}
}

// startProfiling arms CPU, mutex, and block profiling for the measurement
// window when PERF_PROFILE_DIR is set, and returns a stop function that writes
// cpu.prof / mutex.prof / block.prof into that directory. When the env var is
// unset it is a no-op returning an empty stop function, so callers can always
// defer the result unconditionally. This is a measurement-only hook: it is used
// to quantify time spent in the per-read cache locks (VersionedCache.Read,
// ReadOnlyCache.get) and the serial-auth overlayReader lock before deciding
// whether those locks can be removed. Fraction/rate of 1 samples every event
// -- this is a bench run, not production, so we want the full contention graph.
func startProfiling() func() {
	dir := os.Getenv("PERF_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}

	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	cpuFile, err := os.Create(filepath.Join(dir, "cpu.prof"))
	if err != nil {
		panic(err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		panic(err)
	}

	return func() {
		pprof.StopCPUProfile()
		cpuFile.Close()

		writeLookupProfile(filepath.Join(dir, "mutex.prof"), "mutex")
		writeLookupProfile(filepath.Join(dir, "block.prof"), "block")

		// Reset so any later run in the same process starts clean.
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	}
}

// writeLookupProfile snapshots a named runtime profile (e.g. "mutex", "block")
// to filename in the default (proto) format go tool pprof reads.
func writeLookupProfile(filename, name string) {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
		panic(err)
	}
}

// runReplayTest fires every transfer in the (optionally wrapped) window at the
// gateway as fast as submittingWorkerCount goroutines can, with no outstanding-tx
// cap, then measures how fast the drain-all executor commits them. Throughput is
// reported in EVM tx/s (individual transfers), NOT committer tx/s: one committer
// (Fabric) tx now merges many EVM txs, so each committed-batch notification is
// credited with its EVM-tx count (common.TxNotification.EvmTxCount).
// Returns: (evmThroughput, failedEVMTxCount, totalEVMTxCount).
func runReplayTest(
	t *testing.T,
	submittingWorkerCount int,
	ordererSubmitterCount int,
	cfg replayConfig,
	gwConfig string,
) (float64, int64, int64) {
	// Silence GRPC logging
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, os.Stderr, os.Stderr))

	// Initialize Prometheus metrics if enabled
	var metrics *LoadgenMetrics
	if *enableMetrics {
		metrics = NewLoadgenMetrics()
		if err := metrics.StartServer(*metricsAddr); err != nil {
			t.Logf("Failed to start metrics server: %v", err)
		} else {
			t.Logf("Prometheus metrics available at http://localhost%s/metrics", *metricsAddr)
			defer metrics.StopServer()
		}

		// Wire up queue size metrics callbacks
		gwcore.SetBatchSubmitterQueueSizeMetric = metrics.SetBatchSubmitterInputQueueSize

		// Wire up the two-phase executor phase/abort/commit hooks (nil-guarded in
		// core; zero overhead when metrics are disabled). The rollback-batch count
		// is fed from the completion goroutine below (it already discriminates
		// COMMITTED vs invalidated notifications), not via a core hook.
		gwcore.RecordWarmPhaseDuration = metrics.RecordWarmPhase
		gwcore.RecordAuthPhaseDuration = metrics.RecordAuthPhase
		gwcore.RecordEndorsePhaseDuration = metrics.RecordEndorsePhase
		gwcore.RecordCommitLatency = metrics.RecordCommitLatency
		gwcore.RecordSpecAbort = metrics.RecordSpecAbort
		defer func() {
			gwcore.RecordWarmPhaseDuration = nil
			gwcore.RecordAuthPhaseDuration = nil
			gwcore.RecordEndorsePhaseDuration = nil
			gwcore.RecordCommitLatency = nil
			gwcore.RecordSpecAbort = nil
		}()
	}

	// USDC contract address
	USDCAddr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	// Configure balance priming for USDC transfers
	balancePriming := &testimpl.BalancePrimingConfig{
		Enabled:         true,
		ContractAddress: USDCAddr,
		MappingPosition: 9, // USDC balance mapping is at slot 9
	}
	evmConfig := execution.EVMConfig{}

	// PERF_WARM_WORKERS caps ExecuteBatch's warm-pass concurrency
	// (execution.EVMConfig.WarmWorkers) for sweeps. Unset/0 keeps the production
	// default of one warm worker per tx. A positive value forces each warm worker
	// to reuse its primed EVM across several txs, restoring cross-tx reuse of the
	// EVM/stack-arena/JUMPDEST cache -- used to measure the alloc/GC cost of the
	// len(txs)-workers regime against warm-pass I/O saturation. Correctness is
	// unaffected: the warm pass only primes caches and always completes (wg.Wait)
	// before the serial authoritative pass reads, so a lower worker count changes
	// warm wall-time, never the auth pass's cache-hit rate or MVCC outcome.
	if v := os.Getenv("PERF_WARM_WORKERS"); v != "" {
		var ww int
		_, err := fmt.Sscanf(v, "%d", &ww)
		require.NoError(t, err, "PERF_WARM_WORKERS must be a valid integer")
		require.GreaterOrEqual(t, ww, 0, "PERF_WARM_WORKERS must be >= 0")
		evmConfig.WarmWorkers = ww
	}

	// Setup test harness with USDC contract and balance priming enabled
	factory := balancePrimingEndorserFactory(balancePriming)

	// Create completion channel for committed-batch notifications. One
	// notification arrives per committer (Fabric) tx, i.e. per merged batch --
	// far fewer than the number of EVM txs -- so a modest buffer absorbs bursts
	// without the tracker ever blocking the notification-streaming goroutine.
	completionCh := make(chan fxcommon.TxNotification, 8192)

	// Create completion tracker for async transaction monitoring
	tracker := NewTxCompletionTracker(completionCh)

	// Choose test harness based on backend:
	// - Local: Traditional block-based synchronization
	// - Fabric: Traditional block-based synchronization
	// - Fabric-X: Notification-based (MemoryStore + NotificationDispatcher)
	fmt.Printf("using namespace %s", *namespace)
	// The USDC contract (prime DB) lives alongside the datasets: in EVM_PERF_DATA
	// when set, else the in-tree testdata/ copy. The harness makes a relative path
	// absolute against integration/perf/ (its CWD before it chdirs to integration/),
	// so both the testdata/ default and an absolute EVM_PERF_DATA path resolve
	// correctly.
	contractPath := "testdata/USDC_contract.json"
	if dir := os.Getenv("EVM_PERF_DATA"); dir != "" {
		contractPath = filepath.Join(dir, "USDC_contract.json")
	}
	th, err := integration.NewFabricXTestHarnessWithNotifications(
		t,
		integration.TestLogger{T: t, Disable: true}, // Disable test harness logging to avoid overwhelming output
		evmConfig,
		contractPath,
		map[string]any{
			"Gateway.SubmitterCount": ordererSubmitterCount,
			"Network.Namespace":      *namespace,
			// Set via the config override (consumed by BuildGateway before the
			// gateway Start reads the flag once), NOT a post-construction setter
			// like SetMaxBatchSize below -- SetPipelined must precede Start.
			"Gateway.Pipelined": *pipeline,
		},
		factory,
		tracker,
		gwConfig,
	)
	require.NoError(t, err) // harness setup must succeed before we deref th below

	// Bound the merged-batch size. The harness fires every tx with no
	// outstanding-completion cap, so the first drain cycle would otherwise fold
	// the whole backlog into a single oversized Fabric tx; a positive bound
	// pipelines the burst across right-sized batches (sweep -max-batch-size).
	th.Gateways[0].SetMaxBatchSize(*maxBatchSize)

	// wait for the priming tx to be committed: we can no longer
	// rely on commit checks because we have disabled the block store
	time.Sleep(time.Second)

	// Wrap the gateway with NonceBypassGateway to skip nonce validation
	// This is necessary for wrap-around replay where the same transactions are replayed
	wrappedGateway := gwtestimpl.NewNonceBypassGateway(th.Gateways[0])

	// Load the JSON dataset
	// The dataset path can be:
	// 1. An absolute path
	// 2. A relative path from the current working directory
	// 3. A relative path from the repo root (../../ from this test file)
	//
	// When running `go test ./integration/perf/...` from repo root, the test's
	// working directory becomes integration/perf/, so we try both cwd and repo root.
	datasetPath := *dataset
	// A bare workload name (no file extension) selects one of the workloads
	// scripts/setup.sh downloads; a value with an extension is treated as a
	// literal path. When EVM_PERF_DATA is set (the out-of-tree data dir), bare
	// names resolve there; otherwise they fall back to the in-tree testdata/.
	if filepath.Ext(datasetPath) == "" {
		name := fmt.Sprintf("USDC_dataset.%s.json.gz", datasetPath)
		if dir := os.Getenv("EVM_PERF_DATA"); dir != "" {
			datasetPath = filepath.Join(dir, name)
		} else {
			datasetPath = filepath.Join("testdata", name)
		}
	}

	var file *os.File
	var fileErr error

	if filepath.IsAbs(datasetPath) {
		// Absolute path - use as-is
		file, fileErr = os.Open(datasetPath)
		if fileErr != nil {
			t.Fatalf("Failed to open dataset file %s: %v", datasetPath, fileErr)
		}
	} else {
		// Relative path - try from cwd first, then from repo root
		file, fileErr = os.Open(datasetPath)
		if fileErr != nil {
			// Try from repo root (../../ from integration/perf/)
			repoRootPath := filepath.Join("..", "..", datasetPath)
			file, fileErr = os.Open(repoRootPath)
			if fileErr != nil {
				t.Fatalf("Failed to open dataset file. Tried:\n  1. %s\n  2. %s\nError: %v",
					datasetPath, repoRootPath, fileErr)
			}
			datasetPath = repoRootPath
		}
	}
	defer file.Close()

	t.Logf("Loading dataset from %s", datasetPath)

	gzReader, err := gzip.NewReader(file)
	assert.NoError(t, err)
	defer gzReader.Close()

	var allTransfers []TokenTransfer
	decoder := json.NewDecoder(gzReader)
	err = decoder.Decode(&allTransfers)
	assert.NoError(t, err)
	assert.NotEmpty(t, allTransfers, "dataset should contain transfers")

	t.Logf("Loaded %d transfers from dataset", len(allTransfers))

	window := allTransfers
	if cfg.windowSize > 0 && cfg.windowSize < len(allTransfers) {
		window = allTransfers[:cfg.windowSize]
	}

	if cfg.wrapAround && cfg.wrapCount > 0 {
		cfg.totalDispatches = int64(len(window)) * cfg.wrapCount
	}

	// totalToSubmit is the number of EVM txs the harness will fire: the whole
	// window once, or the window repeated wrapCount times (wrap-around replay).
	totalToSubmit := int64(len(window))
	if cfg.wrapAround {
		totalToSubmit = cfg.totalDispatches
	}

	// Counters (all atomic; read from the logging/completion goroutines):
	//   submitted        -- EVM txs successfully handed to the gateway
	//   submitFailed     -- EVM txs SendTransaction rejected outright (should be ~0)
	//   committedEVM     -- EVM txs that actually committed (summed across batches)
	//   committedBatches -- committer (Fabric) txs that committed (== merged batches)
	//   rolledBackBatches-- committer txs that came back non-COMMITTED; their EVM
	//                       txs stay pending and are re-batched, so they are NOT
	//                       counted as committed until they land in a later batch.
	var submitted, submitFailed, committedEVM, committedBatches, rolledBackBatches int64

	// Drain notifications buffered before the measurement window (e.g. the
	// balance-priming tx that committed during the sleep above) so they do not
	// skew the committed-EVM count.
	for len(completionCh) > 0 {
		<-completionCh
	}

	runtime.GC()

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Hour)
	defer cancel()

	// Profile only the measurement window (submit + drain), never the gzip
	// dataset load above -- otherwise decode noise pollutes the CPU profile.
	// No-op unless PERF_PROFILE_DIR is set. Stopped after the drain completes,
	// below, before the final stats are computed.
	stopProfiling := startProfiling()

	startTime := time.Now()
	if metrics != nil {
		metrics.SetRunStart(startTime)
	}

	// Work channel: a bounded buffer that lets the feeder run ahead of the
	// submitters without materializing every tx at once. By default there is no
	// outstanding-completion cap -- the feeder pushes all totalToSubmit items and
	// the gateway's pending pool absorbs whatever the executor hasn't yet drained
	// (see the -outstanding removal note on the flags above). -max-outstanding
	// re-introduces a cap for multi-hour runs only, where an unbounded pending
	// pool would exhaust the host; see inflightLimiter.
	type workItem struct {
		index    int64
		transfer TokenTransfer
	}
	workChan := make(chan workItem, 4096)

	limiter := newInflightLimiter(*maxOutstanding)
	if *maxOutstanding > 0 {
		t.Logf("Closed-loop flow control: at most %d outstanding EVM txs", *maxOutstanding)
	}
	if *targetTPS > 0 {
		t.Logf("Paced submission: %.0f EVM tx/s target (NOT the system ceiling -- this run is "+
			"deliberately throttled to fit a disk budget)", *targetTPS)
	}

	// fedTotal is -1 until the feeder stops, then the number of transfers it fed.
	// Always read through effectiveTotal: under a duration stop, totalToSubmit is
	// an unreachable ceiling and only the fed count is a real total.
	fedTotal := int64(-1)
	if cfg.duration > 0 {
		t.Logf("Duration-bounded run: feeding for %s (wrap target %d is a ceiling, not a goal)",
			cfg.duration, totalToSubmit)
	}

	// doneCh is closed once every fired tx has been accounted for (committed, or
	// failed to even submit). Guarded by sync.Once so the completion goroutine,
	// the feeder's final check, and the stall path can all request it safely.
	// Declared before the feeder because the feeder signals it too (see below).
	doneCh := make(chan struct{})
	var doneOnce sync.Once
	signalDone := func() { doneOnce.Do(func() { close(doneCh) }) }

	// Submitters: fire every tx, never waiting for a completion.
	var wg sync.WaitGroup
	for range submittingWorkerCount {
		wg.Go(func() {
			for item := range workChan {
				if ctx.Err() != nil {
					return
				}
				tx := new(types.Transaction)
				if err := tx.UnmarshalBinary(item.transfer.Transaction); err != nil {
					t.Logf("Transfer %d: failed to unmarshal transaction: %v", item.index, err)
					panic(err)
				}
				// Use the wrapped gateway to bypass nonce validation (wrap-around
				// replay resubmits the same signed txs).
				if err := wrappedGateway.SendTransaction(ctx, tx); err != nil {
					t.Logf("Transfer %d: SendTransaction error: %v", item.index, err)
					atomic.AddInt64(&submitFailed, 1)
					// Rejected outright, so it will never commit and never be
					// credited -- return its slot or the closed loop leaks
					// headroom and eventually wedges the feeder.
					limiter.Release(1)
					continue
				}
				atomic.AddInt64(&submitted, 1)
				if metrics != nil {
					metrics.RecordTransactionSent()
				}
			}
		})
	}

	// Feeder: push items (wrapping over the window as needed), then close
	// workChan. Stops early on the configured duration, and -- when flow control
	// is on -- waits for commit headroom before each push rather than
	// refilling on completion.
	var feederWg sync.WaitGroup
	feederWg.Go(func() {
		defer close(workChan)
		cursor := 0
		var fed int64
		defer func() {
			atomic.StoreInt64(&fedTotal, fed)
			// The final completion may already have been processed before
			// fedTotal became known, in which case nothing will re-evaluate the
			// done condition -- so evaluate it here too, or a finished run hangs
			// until the 60s stall detector rescues it with a misleading message.
			if atomic.LoadInt64(&committedEVM)+atomic.LoadInt64(&submitFailed) >= fed {
				signalDone()
			}
		}()

		// feedCtx ends at the duration deadline, so a feeder blocked waiting for
		// commit headroom still stops on time. Without this the deadline is only
		// checked at the top of the loop, and a feeder parked in Acquire would
		// never close workChan -- which wg.Wait() below is waiting on, so the
		// drain and stall-detection path would never even be reached.
		feedCtx := ctx
		if cfg.duration > 0 {
			var cancelFeed context.CancelFunc
			feedCtx, cancelFeed = context.WithDeadline(ctx, startTime.Add(cfg.duration))
			defer cancelFeed()
		}

		for i := int64(0); i < totalToSubmit; i++ {
			if feedCtx.Err() != nil {
				if cfg.duration > 0 {
					t.Logf("Duration %s reached; feeder stopping after %d transfers", cfg.duration, fed)
				}
				return
			}

			// Open-loop pacing, when a target rate is set: hold this tx until its
			// slot in the schedule. Absolute deadlines from startTime, so the
			// schedule cannot drift over a multi-hour run.
			if *targetTPS > 0 {
				if d := time.Until(paceDeadline(startTime, fed, *targetTPS)); d > 0 {
					timer := time.NewTimer(d)
					select {
					case <-timer.C:
					case <-feedCtx.Done():
						timer.Stop()
						return
					}
				}
			}

			// Closed loop: block until an earlier tx commits and frees headroom.
			// No-op when flow control is off (nil limiter).
			//
			// Bounded, because a permanently wedged stack frees no headroom and an
			// unbounded wait here would hang the whole test (see feedCtx above)
			// rather than letting the drain/stall path report the failure. The
			// bound is far above any legitimate commit gap -- commit latency is
			// sub-second -- so reaching it always means something is broken.
			const feederAcquireTimeout = 5 * time.Minute
			acqCtx, cancelAcq := context.WithTimeout(feedCtx, feederAcquireTimeout)
			err := limiter.Acquire(acqCtx)
			cancelAcq()
			if err != nil {
				switch {
				case ctx.Err() != nil:
					// Whole test is shutting down.
				case feedCtx.Err() != nil:
					t.Logf("Duration %s reached while waiting for commit headroom; feeder stopping after %d transfers",
						cfg.duration, fed)
				default:
					t.Logf("Feeder: no commit headroom for %s after %d transfers; stopping so the drain/stall path can report",
						feederAcquireTimeout, fed)
				}
				return
			}

			select {
			case <-feedCtx.Done():
				return
			case workChan <- workItem{index: i, transfer: window[cursor]}:
				fed++
			}
			cursor++
			if cursor >= len(window) {
				cursor = 0 // only reached when totalToSubmit > len(window) (wrap-around)
			}
		}
	})

	// Completion goroutine: credit each committed batch with its committed
	// sub-tx count so throughput is measured in EVM txs, not committer txs.
	var completionWg sync.WaitGroup
	completionWg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notif, ok := <-completionCh:
				if !ok {
					return
				}
				if notif.Status == committerpb.Status_COMMITTED {
					// One committer tx merges many EVM txs; credit them all so
					// throughput is EVM tx/s, not committer tx/s. A merged batch
					// emits no per-tx event blob, so count from EvmTxCount (the
					// invocation Args count) rather than the empty Events.
					n := notif.EvmTxCount
					if n <= 0 {
						n = 1
					}
					newTotal := atomic.AddInt64(&committedEVM, int64(n))
					atomic.AddInt64(&committedBatches, 1)
					if metrics != nil {
						metrics.RecordBatchCommitted() // one committer tx per merged batch
						for range n {
							metrics.RecordTransactionCommitted()
						}
					}
					// These txs are done, so free their headroom for new work.
					// The rollback branch below deliberately releases nothing:
					// those EVM txs are still outstanding and get credited when a
					// later batch commits them.
					limiter.Release(n)
					// Done when every fired tx is accounted for: committed, or
					// failed to submit (those never commit).
					if newTotal+atomic.LoadInt64(&submitFailed) >= effectiveTotal(atomic.LoadInt64(&fedTotal), totalToSubmit) {
						signalDone()
					}
				} else {
					// The whole committer tx (merged batch) was invalidated; its
					// EVM txs roll back and are re-batched, so they are not
					// counted here -- they will be credited when a later batch
					// commits them.
					atomic.AddInt64(&rolledBackBatches, 1)
					if metrics != nil {
						metrics.RecordTransactionAborted()
						metrics.RecordBatchRolledBack(1)
					}
				}
			}
		}
	})

	// Progress logging goroutine: reports EVM tx/s.
	stopLogging := make(chan struct{})
	var loggingWg sync.WaitGroup
	loggingWg.Go(func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		p := message.NewPrinter(language.English)
		lastTime := startTime
		var lastCommitted int64

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				elapsed := now.Sub(lastTime).Seconds()
				committed := atomic.LoadInt64(&committedEVM)
				sub := atomic.LoadInt64(&submitted)
				batches := atomic.LoadInt64(&committedBatches)
				rb := atomic.LoadInt64(&rolledBackBatches)

				recent := float64(committed-lastCommitted) / elapsed
				overall := float64(committed) / now.Sub(startTime).Seconds()
				inFlight := sub - committed
				avgBatch := 0.0
				if batches > 0 {
					avgBatch = float64(committed) / float64(batches)
				}

				t.Log(p.Sprintf("Progress: %d/%d EVM txs committed | submitted %d, in-flight %d | %d batches (avg %.1f EVM/batch), %d rolled back | %.0f EVM tx/s (recent), %.0f EVM tx/s (overall)",
					committed, effectiveTotal(atomic.LoadInt64(&fedTotal), totalToSubmit),
					sub, inFlight, batches, avgBatch, rb, recent, overall))

				if metrics != nil {
					metrics.SetOutstandingTransactions(inFlight)
					metrics.SetThroughput(overall)
				}

				lastTime = now
				lastCommitted = committed
			case <-ctx.Done():
				return
			case <-stopLogging:
				return
			}
		}
	})

	// Wait until all txs are fired (submission is local and fast under nonce
	// bypass; the pending pool holds the backlog).
	wg.Wait()
	// The feeder has finished by now (workChan is closed and drained), so
	// fedTotal is authoritative from here on.
	targetTotal := effectiveTotal(atomic.LoadInt64(&fedTotal), totalToSubmit)
	t.Logf("Submission complete: %d submitted, %d failed to submit (of %d)",
		atomic.LoadInt64(&submitted), atomic.LoadInt64(&submitFailed), targetTotal)

	// If every fired tx already failed to submit, there is nothing to wait for.
	if atomic.LoadInt64(&submitFailed) >= targetTotal {
		signalDone()
	}

	// Wait for all fired txs to commit, or for progress to stall (some tx never
	// commits -- e.g. a permanent exclusion), or for the overall context to end.
	const stallTimeout = 60 * time.Second
	waitTicker := time.NewTicker(time.Second)
	lastCommitted := atomic.LoadInt64(&committedEVM)
	lastProgress := time.Now()
wait:
	for {
		select {
		case <-doneCh:
			break wait
		case <-ctx.Done():
			t.Logf("Context ended before all txs committed")
			break wait
		case <-waitTicker.C:
			cur := atomic.LoadInt64(&committedEVM)
			if cur > lastCommitted {
				lastCommitted = cur
				lastProgress = time.Now()
			} else if time.Since(lastProgress) > stallTimeout {
				t.Logf("Stalled: no commit progress for %s (%d/%d EVM txs committed); giving up",
					stallTimeout, cur, targetTotal)
				break wait
			}
		}
	}
	waitTicker.Stop()
	finishTime := time.Now()

	// Teardown. cancel() unblocks the feeder/submitters if we broke out early.
	cancel()
	// Stop the tracker before closing completionCh -- the notification-streaming
	// goroutine (owned by the harness) outlives this function and would panic
	// sending on a closed channel.
	tracker.Stop()
	close(completionCh)
	completionWg.Wait()
	feederWg.Wait()
	close(stopLogging)
	loggingWg.Wait()

	// Drain finished: write the CPU/mutex/block profiles for the window.
	stopProfiling()

	finalCommitted := atomic.LoadInt64(&committedEVM)
	finalSubmitFailed := atomic.LoadInt64(&submitFailed)
	finalBatches := atomic.LoadInt64(&committedBatches)
	finalRolledBack := atomic.LoadInt64(&rolledBackBatches)

	elapsed := finishTime.Sub(startTime).Seconds()
	evmThroughput := float64(finalCommitted) / elapsed
	avgBatch := 0.0
	if finalBatches > 0 {
		avgBatch = float64(finalCommitted) / float64(finalBatches)
	}

	t.Logf("Replay complete: %d/%d EVM txs committed in %.1fs across %d committer txs (avg %.1f EVM/batch); %d rolled-back batches, %d submit failures | %.0f EVM tx/s",
		finalCommitted, targetTotal, elapsed, finalBatches, avgBatch, finalRolledBack, finalSubmitFailed, evmThroughput)

	// Commit-path timing: submit->commit-notification latency and how full the
	// in-flight window got. If peak in-flight stays below the cap, the commit path
	// was hidden behind execution (never on the critical path); if it pins at the
	// cap, commit latency is throttling the executor. This is the piece the
	// endorser's ENDORSE-TIMING execution split cannot measure.
	if n, avgCommit, maxCommit := th.Gateways[0].CommitLatencyStats(); n > 0 {
		t.Logf("Commit-path timing: %d committer txs | submit->commit avg %s max %s | peak in-flight %d/%d",
			n, avgCommit.Round(time.Millisecond), maxCommit.Round(time.Millisecond),
			th.Gateways[0].MaxInflightObserved(), th.Gateways[0].MaxInflight())
	}

	// Return (EVM tx/s, EVM txs that never committed, total EVM txs targeted).
	// Under a duration stop the target is what was actually fed, not the wrap
	// ceiling -- otherwise the caller computes a ~100% failure rate on a
	// perfectly healthy run.
	return evmThroughput, targetTotal - finalCommitted, targetTotal
}

// TestReplayJSONDataset loads the USDC_dataset.json.gz file with pre-generated transactions
// and replays them with batched priming of sender balances.
func TestReplayJSONDataset(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	submittingWorkerCount := *submitters // goroutines calling the gateway's SendTransaction
	ordererSubmitterCount := *orderers   // BatchSubmitter workers submitting to the orderer

	// Fire a bounded single pass by default. The old infinite wrap (windowSize
	// and wrapCount 1_000_000) relied on the -outstanding cap to keep only ~10k
	// txs in flight; with fire-all + no cap that would balloon the pending pool,
	// so we submit a fixed window once and measure steady-state EVM tx/s. Scale
	// via PERF_REPLAY_WINDOW_SIZE (0 = whole dataset) and PERF_REPLAY_WRAP_COUNT.
	cfg := loadReplayConfigFromEnv(t)
	if os.Getenv("PERF_REPLAY_WINDOW_SIZE") == "" {
		cfg.windowSize = 50_000
	}

	throughput, uncommitted, total := runReplayTest(
		t, submittingWorkerCount, ordererSubmitterCount, cfg, *gatewayConfig,
	)
	t.Logf("TestReplayJSONDataset: %.0f EVM tx/s (%d/%d committed, batch cap %d)",
		throughput, total-uncommitted, total, *maxBatchSize)
}

type performanceResult struct {
	submittingWorkers  int
	ordererSubmitters  int
	throughput         float64 // EVM tx/s
	failedTransactions int64   // EVM txs that never committed
	totalTransactions  int64   // EVM txs targeted
	failureRate        float64
}

// TestReplayJSONDatasetPerformance sweeps the two knobs that still matter under
// the drain-all executor -- the number of goroutines submitting to the gateway
// and the number of BatchSubmitter workers submitting to the orderer -- and
// records EVM tx/s for each. The old per-tx gateway-worker dimension is gone:
// the executor drains the whole pending pool into one merged batch per cycle
// regardless of arrival concurrency.
func TestReplayJSONDatasetPerformance(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	submittingWorkerCounts := []int{4, 8, 16, 24}
	ordererSubmitterCounts := []int{16} // BatchSubmitter workers

	// Store results
	var results []performanceResult

	t.Logf("Starting performance sweep (EVM tx/s) over submitter / orderer-submitter counts...")

	for _, submittingWorkers := range submittingWorkerCounts {
		for _, ordererSubmitters := range ordererSubmitterCounts {
			t.Logf("\n=== Testing with submittingWorkers=%d, ordererSubmitters=%d ===",
				submittingWorkers, ordererSubmitters)

			throughput, failedTxs, totalTxs := runReplayTest(t, submittingWorkers, ordererSubmitters, loadReplayConfigFromEnv(t), *gatewayConfig)
			failureRate := float64(failedTxs) / float64(totalTxs)

			results = append(results, performanceResult{
				submittingWorkers:  submittingWorkers,
				ordererSubmitters:  ordererSubmitters,
				throughput:         throughput,
				failedTransactions: failedTxs,
				totalTransactions:  totalTxs,
				failureRate:        failureRate,
			})

			t.Logf("Result: Throughput=%.2f EVM tx/s, Uncommitted=%d/%d (%.2f%%)",
				throughput, failedTxs, totalTxs, failureRate*100)
		}
	}

	// Write results to CSV file
	csvPath := "performance_results.csv"
	file, err := os.Create(csvPath)
	assert.NoError(t, err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	err = writer.Write([]string{
		"submitting_workers",
		"orderer_submitters",
		"throughput_evm_tx_per_s",
		"uncommitted_transactions",
		"total_transactions",
		"failure_rate",
	})
	assert.NoError(t, err)

	// Write data rows
	for _, result := range results {
		err = writer.Write([]string{
			fmt.Sprintf("%d", result.submittingWorkers),
			fmt.Sprintf("%d", result.ordererSubmitters),
			fmt.Sprintf("%.2f", result.throughput),
			fmt.Sprintf("%d", result.failedTransactions),
			fmt.Sprintf("%d", result.totalTransactions),
			fmt.Sprintf("%.4f", result.failureRate),
		})
		assert.NoError(t, err)
	}

	t.Logf("Performance results written to %s", csvPath)
}

func TestShared(t *testing.T) {
	sharedConfigBytes, err := os.ReadFile("../../testdata/crypto/shared_config.binpb")
	require.NoError(t, err)

	// Unmarshal the protobuf
	sharedConfig := &ordererpb.SharedConfig{}
	err = proto.Unmarshal(sharedConfigBytes, sharedConfig)
	require.NoError(t, err)
	s, err := yaml.Marshal(sharedConfig)
	require.NoError(t, err)
	fmt.Printf("%s\n", s)
}
