/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"golang.org/x/time/rate"
)

var batchLogger = flogging.MustGetLogger("gateway.core.batch_submitter")

// SubmissionTimestamps is an optional map for tracking submission timestamps.
// If non-nil, timestamps are recorded when transactions are submitted to the orderer.
// Key: Ethereum transaction hash, Value: T2 timestamp (when submitted to orderer)
var SubmissionTimestamps map[common.Hash]time.Time

// SubmissionTimestampsMu protects access to SubmissionTimestamps
var SubmissionTimestampsMu sync.Mutex

// SetBatchSubmitterQueueSizeMetric is an optional callback for reporting the batch submitter input queue size.
// If non-nil, it will be called to report the current queue size.
var SetBatchSubmitterQueueSizeMetric func(size int)

// BatchSubmitter reads endorsements from a channel, optionally records them in the
// pending-tx cache (when cache != nil), then submits each one to the orderer.
// The cache is used by AllTxBatchDispatcher to correlate commit events with the
// originating Ethereum transaction.
// Multiple worker goroutines read from inputChan and submit in parallel.
// Each worker has its own submitter instance for better performance.
// Rate limiting is optionally applied across all workers to ensure aggregate submission rate
// does not exceed the configured limit.
//
// Ordering invariant: when the producer feeding inputChan emits order-dependent
// committer txs -- the pipelined gateway does, since it endorses batch k+1
// against batch k's still-uncommitted cache writes and therefore requires the
// committer to see them in submission order -- numWorkers MUST be 1. With more
// than one worker, parallel delivery to the orderer can reorder those dependent
// txs and break cross-batch MVCC validation (see
// gateway/app.orderedOrdererSubmitterCount, which enforces this on the
// production path). Non-production callers with independent (order-insensitive)
// workloads may still pass numWorkers > 1.
type BatchSubmitter struct {
	submitters  []Submitter // One submitter per worker for parallel submission
	inputChan   chan sdk.Endorsement
	stopChan    chan struct{}
	doneChan    chan struct{}
	numWorkers  int
	rateLimiter *rate.Limiter // Shared rate limiter across all workers (nil if disabled)
}

const DefaultNumWorkers = 16

// NewBatchSubmitter creates a new BatchSubmitter.
// numWorkers specifies the number of parallel submission goroutines (default: 16).
// Creates one submitter instance per worker for optimal parallel performance.
// txPerSec sets the aggregate submission rate limit across all workers; when zero,
// rate limiting is disabled.
func NewBatchSubmitter(
	submitters []Submitter,
	inputChan chan sdk.Endorsement,
	numWorkers int,
	txPerSec int,
) *BatchSubmitter {
	if numWorkers <= 0 {
		numWorkers = DefaultNumWorkers
	}

	// Ensure we have enough submitters for the workers
	if len(submitters) < numWorkers {
		panic(fmt.Sprintf("Only %d submitters provided for %d workers.", len(submitters), numWorkers))
	}

	var rateLimiter *rate.Limiter
	if txPerSec > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(txPerSec), txPerSec)
		batchLogger.Infof("Rate limiting enabled: %d tx/s", txPerSec)
	}

	return &BatchSubmitter{
		submitters:  submitters,
		inputChan:   inputChan,
		stopChan:    make(chan struct{}),
		doneChan:    make(chan struct{}),
		numWorkers:  numWorkers,
		rateLimiter: rateLimiter,
	}
}

// Start begins the submission loop with multiple worker goroutines.
func (bs *BatchSubmitter) Start(ctx context.Context) {
	go bs.run(ctx)
}

// Stop signals the submitter to stop and waits for all workers to finish.
func (bs *BatchSubmitter) Stop() {
	close(bs.stopChan)
	<-bs.doneChan
}

// Close closes all submitter connections.
func (bs *BatchSubmitter) Close() error {
	var firstErr error
	for i, submitter := range bs.submitters {
		if err := submitter.Close(); err != nil {
			batchLogger.Errorf("Failed to close submitter %d: %v", i, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (bs *BatchSubmitter) run(ctx context.Context) {
	defer close(bs.doneChan)

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := range bs.numWorkers {
		wg.Go(func() {
			bs.worker(ctx, i)
		})
	}

	// Wait for all workers to complete
	wg.Wait()
}

func (bs *BatchSubmitter) worker(ctx context.Context, workerID int) {
	batchLogger.Debugf("Worker %d started", workerID)
	defer batchLogger.Debugf("Worker %d stopped", workerID)

	for {
		// Report queue size metric if callback is set
		if SetBatchSubmitterQueueSizeMetric != nil {
			SetBatchSubmitterQueueSizeMetric(len(bs.inputChan))
		}

		select {
		case <-bs.stopChan:
			return

		case end, ok := <-bs.inputChan:
			if !ok {
				return
			}
			if err := bs.submitOne(ctx, workerID, end); err != nil {
				batchLogger.Errorf("Worker %d: submit failed: %v", workerID, err)
			}
		}
	}
}

func (bs *BatchSubmitter) submitOne(ctx context.Context, workerID int, end sdk.Endorsement) error {
	// Wait for rate limiter to allow this submission (if enabled)
	if bs.rateLimiter != nil {
		if err := bs.rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limiter wait failed: %w", err)
		}
	}

	var txid string
	t0 := time.Now()
	err := bs.submitters[workerID].Submit(ctx, end)
	batchLogger.Debugf("[SUBMIT] worker=%d txid=%s submit_took=%v", workerID, txid, time.Since(t0))
	return err
}
