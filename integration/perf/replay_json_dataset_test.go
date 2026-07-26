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

var gatewayConfig = flag.String("gateway-config", "fabx.yaml", "gateway config file for the Fabric-X network")
var metricsAddr = flag.String("metrics-addr", "0.0.0.0:2112", "address for Prometheus metrics endpoint")
var enableMetrics = flag.Bool("enable-metrics", false, "enable Prometheus metrics export")
var namespace = flag.String("namespace", "real", "namespace to commit transactions to")
var dataset = flag.String("dataset", "testdata/USDC_dataset.json.gz", "dataset to use")

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
	return func(t *testing.T, ecfg econf.Endorser, channel, namespace string, evmConfig execution.EVMConfig, protocol string) integration.EndorserComponents {
		// Create the base endorser components
		readStore, backing, builder, baseEndorser := integration.NewEndorser(t, ecfg, channel, namespace, evmConfig, protocol)

		// Extract the base EVMEngine
		baseEngine, ok := baseEndorser.Engine.(*execution.EVMEngine)
		if !ok {
			t.Fatalf("Expected *execution.EVMEngine, got %T", baseEndorser.Engine)
		}

		// Wrap the engine with balance priming support
		wrappedEngine := testimpl.NewEVMEngineWrapper(
			namespace,
			readStore,
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
	th, err := integration.NewFabricXTestHarnessWithNotifications(
		t,
		integration.TestLogger{T: t, Disable: true}, // Disable test harness logging to avoid overwhelming output
		evmConfig,
		"testdata/USDC_contract.json",
		map[string]any{
			"Gateway.SubmitterCount": ordererSubmitterCount,
			"Network.Namespace":      *namespace,
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

	startTime := time.Now()

	// Work channel: a bounded buffer that lets the feeder run ahead of the
	// submitters without materializing every tx at once. There is deliberately
	// no outstanding-completion cap -- the feeder pushes all totalToSubmit items
	// and the gateway's pending pool absorbs whatever the executor hasn't yet
	// drained (see the -outstanding removal note on the flags above).
	type workItem struct {
		index    int64
		transfer TokenTransfer
	}
	workChan := make(chan workItem, 4096)

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
					continue
				}
				atomic.AddInt64(&submitted, 1)
				if metrics != nil {
					metrics.RecordTransactionSent()
				}
			}
		})
	}

	// Feeder: push all totalToSubmit items (wrapping over the window as needed),
	// then close workChan. No refill-on-completion.
	var feederWg sync.WaitGroup
	feederWg.Go(func() {
		defer close(workChan)
		cursor := 0
		for i := int64(0); i < totalToSubmit; i++ {
			select {
			case <-ctx.Done():
				return
			case workChan <- workItem{index: i, transfer: window[cursor]}:
			}
			cursor++
			if cursor >= len(window) {
				cursor = 0 // only reached when totalToSubmit > len(window) (wrap-around)
			}
		}
	})

	// doneCh is closed once every fired tx has been accounted for (committed, or
	// failed to even submit). Guarded by sync.Once so both the completion
	// goroutine and the stall path can request it safely.
	doneCh := make(chan struct{})
	var doneOnce sync.Once
	signalDone := func() { doneOnce.Do(func() { close(doneCh) }) }

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
						for range n {
							metrics.RecordTransactionCommitted()
						}
					}
					// Done when every fired tx is accounted for: committed, or
					// failed to submit (those never commit).
					if newTotal+atomic.LoadInt64(&submitFailed) >= totalToSubmit {
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
					committed, totalToSubmit, sub, inFlight, batches, avgBatch, rb, recent, overall))

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
	t.Logf("Submission complete: %d submitted, %d failed to submit (of %d)",
		atomic.LoadInt64(&submitted), atomic.LoadInt64(&submitFailed), totalToSubmit)

	// If every fired tx already failed to submit, there is nothing to wait for.
	if atomic.LoadInt64(&submitFailed) >= totalToSubmit {
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
					stallTimeout, cur, totalToSubmit)
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
		finalCommitted, totalToSubmit, elapsed, finalBatches, avgBatch, finalRolledBack, finalSubmitFailed, evmThroughput)

	// Return (EVM tx/s, EVM txs that never committed, total EVM txs targeted).
	return evmThroughput, totalToSubmit - finalCommitted, totalToSubmit
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
