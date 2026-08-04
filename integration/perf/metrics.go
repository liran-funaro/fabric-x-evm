/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// LoadgenMetrics holds all Prometheus metrics for the loadgen test
type LoadgenMetrics struct {
	// Transaction counters
	transactionSent      prometheus.Counter
	transactionCommitted prometheus.Counter
	transactionAborted   prometheus.Counter

	// Committer (Fabric) tx counter: one merged batch == one committer tx, so
	// rate(batchCommitted) is committer tx/s alongside the EVM tx/s derived from
	// transactionCommitted (which credits every EVM sub-tx of the batch).
	batchCommitted prometheus.Counter

	// Latency breakdown histograms
	totalLatency      prometheus.Histogram // T4 - T1: end-to-end latency
	queueLatency      prometheus.Histogram // T2 - T1: queueing time
	processingLatency prometheus.Histogram // T3 - T2: processing time by the app
	backendLatency    prometheus.Histogram // T4 - T3: processing time by the backend

	// Block counters (for compatibility with existing dashboard)
	blockSent     prometheus.Counter
	blockReceived prometheus.Counter

	// Gauges for current state
	outstandingTxGauge prometheus.Gauge
	throughputGauge    prometheus.Gauge

	// Queue size gauges
	batchSubmitterInputQueueSize prometheus.Gauge
	txQueueReadyListSize         prometheus.Gauge
	txQueueWaitingListSize       prometheus.Gauge

	// Gateway two-phase executor metrics (fed by the nil-guarded core hooks:
	// RecordWarmPhaseDuration / RecordAuthPhaseDuration / RecordEndorsePhaseDuration
	// / RecordSpecAbort, plus the commit-path timing + rollback count surfaced from
	// the replay test).
	warmPhaseLatency    prometheus.Histogram   // concurrent WARM pass wall-clock (pipelined only)
	authPhaseLatency    prometheus.Histogram   // AUTHORITATIVE pass wall-clock (pipelined AuthBatch only)
	endorsePhaseLatency prometheus.Histogram   // fused ExecuteBatch wall-clock, warm+auth combined (serial only)
	commitLatency       prometheus.Histogram   // committer round-trip per batch
	batchesRolledBack   prometheus.Counter     // MVCC rollback cascades (batches rolled back + re-batched)
	specAbort           *prometheus.CounterVec // spec-version aborts by dominant stale-read class

	registry *prometheus.Registry
	server   *http.Server
	mu       sync.Mutex
}

// NewLoadgenMetrics creates and registers all Prometheus metrics
func NewLoadgenMetrics() *LoadgenMetrics {
	registry := prometheus.NewRegistry()

	m := &LoadgenMetrics{
		transactionSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_transaction_sent_total",
			Help: "Total number of transactions sent to the gateway",
		}),
		transactionCommitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_transaction_committed_total",
			Help: "Total number of transactions committed successfully",
		}),
		transactionAborted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_transaction_aborted_total",
			Help: "Total number of transactions aborted or failed",
		}),
		batchCommitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_batch_committed_total",
			Help: "Total number of committer (Fabric) txs committed; one merged batch == one committer tx (rate = committer tx/s)",
		}),
		totalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "loadgen_total_latency_seconds",
			Help:    "End-to-end latency (T4-T1: test submit to notification) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~524s
		}),
		queueLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "loadgen_queue_latency_seconds",
			Help:    "Queueing latency (T2-T1: test submit to dequeue) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms to ~52s
		}),
		processingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "loadgen_processing_latency_seconds",
			Help:    "Processing latency (T3-T2: dequeue to batch submit) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms to ~52s
		}),
		backendLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "loadgen_backend_latency_seconds",
			Help:    "Backend latency (T4-T3: batch submit to notification) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~524s
		}),
		blockSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_block_sent_total",
			Help: "Total number of blocks sent (for dashboard compatibility)",
		}),
		blockReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_block_received_total",
			Help: "Total number of blocks received (for dashboard compatibility)",
		}),
		outstandingTxGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "loadgen_outstanding_transactions",
			Help: "Current number of outstanding transactions",
		}),
		throughputGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "loadgen_throughput_tx_per_second",
			Help: "Current throughput in transactions per second",
		}),
		batchSubmitterInputQueueSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_batch_submitter_input_queue_size",
			Help: "Current size of the batch submitter input channel queue",
		}),
		txQueueReadyListSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_txqueue_ready_list_size",
			Help: "Current size of the transaction queue ready list",
		}),
		txQueueWaitingListSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_txqueue_waiting_list_size",
			Help: "Current size of the transaction queue waiting list",
		}),
		warmPhaseLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_warm_phase_seconds",
			Help:    "Duration of the concurrent WARM pass per batch (pipelined executor only) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~524s
		}),
		authPhaseLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_auth_phase_seconds",
			Help:    "Duration of the AUTHORITATIVE pass per batch (pipelined AuthBatch only; serial uses gateway_endorse_phase_seconds) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~524s
		}),
		endorsePhaseLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_endorse_phase_seconds",
			Help:    "Duration of the serial executor's fused ExecuteBatch per batch (warm+auth combined; serial only) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~524s
		}),
		commitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_commit_latency_seconds",
			Help:    "Committer round-trip latency per batch (submit to commit notification) in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~524s
		}),
		batchesRolledBack: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_batches_rolled_back_total",
			Help: "Total number of batches rolled back and re-batched due to MVCC spec-version aborts",
		}),
		specAbort: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_spec_abort_total",
			Help: "Spec-version abort cascades by dominant stale-read class (H1-qs-lag / H2-stale-clone / H3-write-cache / over-read / unclassified)",
		}, []string{"class"}),
		registry: registry,
	}

	// Register all metrics
	registry.MustRegister(
		m.transactionSent,
		m.transactionCommitted,
		m.transactionAborted,
		m.batchCommitted,
		m.totalLatency,
		m.queueLatency,
		m.processingLatency,
		m.backendLatency,
		m.blockSent,
		m.blockReceived,
		m.outstandingTxGauge,
		m.throughputGauge,
		m.batchSubmitterInputQueueSize,
		m.txQueueReadyListSize,
		m.txQueueWaitingListSize,
		m.warmPhaseLatency,
		m.authPhaseLatency,
		m.endorsePhaseLatency,
		m.commitLatency,
		m.batchesRolledBack,
		m.specAbort,
	)

	return m
}

// StartServer starts the Prometheus metrics HTTP server on the specified address
func (m *LoadgenMetrics) StartServer(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server != nil {
		return nil // Already started
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

	m.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error but don't crash the test
			println("Metrics server error:", err.Error())
		}
	}()

	return nil
}

// StopServer gracefully stops the metrics HTTP server
func (m *LoadgenMetrics) StopServer() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server == nil {
		return nil
	}

	return m.server.Close()
}

// RecordTransactionSent increments the sent transaction counter
func (m *LoadgenMetrics) RecordTransactionSent() {
	m.transactionSent.Inc()
}

// RecordTransactionCommitted increments the committed transaction counter
func (m *LoadgenMetrics) RecordTransactionCommitted() {
	m.transactionCommitted.Inc()
}

// RecordTransactionAborted increments the aborted transaction counter
func (m *LoadgenMetrics) RecordTransactionAborted() {
	m.transactionAborted.Inc()
}

// RecordBatchCommitted increments the committer-tx counter (one per merged batch
// that commits); rate(loadgen_batch_committed_total) is committer tx/s.
func (m *LoadgenMetrics) RecordBatchCommitted() {
	m.batchCommitted.Inc()
}

// RecordLatencies records the four latency measurements
func (m *LoadgenMetrics) RecordLatencies(total, queue, processing, backend time.Duration) {
	m.totalLatency.Observe(total.Seconds())
	m.queueLatency.Observe(queue.Seconds())
	m.processingLatency.Observe(processing.Seconds())
	m.backendLatency.Observe(backend.Seconds())
}

// RecordBlockSent increments the block sent counter (for dashboard compatibility)
func (m *LoadgenMetrics) RecordBlockSent() {
	m.blockSent.Inc()
}

// RecordBlockReceived increments the block received counter (for dashboard compatibility)
func (m *LoadgenMetrics) RecordBlockReceived() {
	m.blockReceived.Inc()
}

// SetOutstandingTransactions sets the current number of outstanding transactions
func (m *LoadgenMetrics) SetOutstandingTransactions(count int64) {
	m.outstandingTxGauge.Set(float64(count))
}

// SetThroughput sets the current throughput in tx/s
func (m *LoadgenMetrics) SetThroughput(txPerSecond float64) {
	m.throughputGauge.Set(txPerSecond)
}

// SetBatchSubmitterInputQueueSize sets the current size of the batch submitter input queue
func (m *LoadgenMetrics) SetBatchSubmitterInputQueueSize(size int) {
	m.batchSubmitterInputQueueSize.Set(float64(size))
}

// SetTxQueueReadyListSize sets the current size of the transaction queue ready list
func (m *LoadgenMetrics) SetTxQueueReadyListSize(size int) {
	m.txQueueReadyListSize.Set(float64(size))
}

// SetTxQueueWaitingListSize sets the current size of the transaction queue waiting list
func (m *LoadgenMetrics) SetTxQueueWaitingListSize(size int) {
	m.txQueueWaitingListSize.Set(float64(size))
}

// RecordWarmPhase observes the duration of one concurrent WARM pass. Assigned to
// core.RecordWarmPhaseDuration; fires only on the pipelined executor.
func (m *LoadgenMetrics) RecordWarmPhase(d time.Duration) {
	m.warmPhaseLatency.Observe(d.Seconds())
}

// RecordAuthPhase observes the duration of one AUTHORITATIVE pass. Assigned to
// core.RecordAuthPhaseDuration; fires only on the pipelined executor (AuthBatch).
// The serial executor's fused pass is recorded via RecordEndorsePhase instead.
func (m *LoadgenMetrics) RecordAuthPhase(d time.Duration) {
	m.authPhaseLatency.Observe(d.Seconds())
}

// RecordEndorsePhase observes the duration of the serial executor's fused
// ExecuteBatch (warm+auth combined). Assigned to core.RecordEndorsePhaseDuration;
// fires only on the serial (default) executor.
func (m *LoadgenMetrics) RecordEndorsePhase(d time.Duration) {
	m.endorsePhaseLatency.Observe(d.Seconds())
}

// RecordCommitLatency observes the committer round-trip latency for one batch.
// Fed from the replay test's commit-path timing (no core hook needed).
func (m *LoadgenMetrics) RecordCommitLatency(d time.Duration) {
	m.commitLatency.Observe(d.Seconds())
}

// RecordBatchRolledBack increments the rolled-back-batches counter by n. Fed from
// the replay test's rollback tracking (no core hook needed).
func (m *LoadgenMetrics) RecordBatchRolledBack(n int) {
	m.batchesRolledBack.Add(float64(n))
}

// RecordSpecAbort increments the spec-abort counter for the given stale-read
// class. Assigned to core.RecordSpecAbort; fires once per MVCC rollback cascade.
func (m *LoadgenMetrics) RecordSpecAbort(class string) {
	m.specAbort.WithLabelValues(class).Inc()
}
