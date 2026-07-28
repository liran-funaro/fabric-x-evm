/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-evm/common"
	econf "github.com/hyperledger/fabric-x-evm/endorser/config"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/gateway/config"
	"github.com/hyperledger/fabric-x-evm/gateway/core"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/fabrictest"
)

// SubmitterControl is a test handle over a controllableSubmitter that gives a test
// deterministic control of orderer submission ORDER. The first holdN Submit calls are
// BUFFERED (Submit returns nil immediately, so the BatchSubmitter worker and the
// pipelined executor proceed as if the tx were submitted to the orderer); the test then
// forwards the buffered endorsements to the real inner submitter in a chosen order.
// Submit calls beyond holdN pass straight through to the inner submitter, so a cascade
// re-execution's re-submitted batch reaches the orderer without further test action.
//
// The harness that installs this forces SubmitterCount == 1, so exactly one submitter is
// wrapped and Submit calls arrive serially in the executor's FIFO submission order.
//
// Buffering only takes effect after Arm is called. This matters because the SAME
// wrapped submitter also carries the harness's own state-priming traffic
// (StatePrimer.Commit submits through submitters[0] too -- see
// buildTestHarnessWithExtraHandler / NewStatePrimer). If priming were captured by the
// hold-buffer, a wait=true priming Commit would block forever waiting for a "commit"
// that the test cannot release until after that same blocking call returns (deadlock).
// Before Arm, every Submit call passes straight through to the inner submitter --
// identical to the unwrapped behavior every other harness constructor gets -- so
// priming (and anything else run before the test proper begins) is unaffected.
type SubmitterControl struct {
	mu       sync.Mutex
	inner    core.Submitter    // the real submitter (set when the single submitter is wrapped)
	holdN    int               // number of initial (post-Arm) Submit calls to buffer
	armed    bool              // false until Arm is called; unarmed Submit calls pass straight through
	seen     int               // Submit calls observed since Arm
	buffered []sdk.Endorsement // the first holdN post-Arm endorsements, in Submit-call order
}

type controllableSubmitter struct{ ctl *SubmitterControl }

func (c *controllableSubmitter) Submit(ctx context.Context, end sdk.Endorsement) error {
	return c.ctl.onSubmit(ctx, end)
}
func (c *controllableSubmitter) Close() error { return c.ctl.inner.Close() }

// Arm switches the control from pass-through to hold-and-buffer mode. Call it once
// setup (e.g. state priming) that must reach the orderer immediately is complete.
func (c *SubmitterControl) Arm() {
	c.mu.Lock()
	c.armed = true
	c.mu.Unlock()
}

func (c *SubmitterControl) onSubmit(ctx context.Context, end sdk.Endorsement) error {
	c.mu.Lock()
	if !c.armed {
		inner := c.inner
		c.mu.Unlock()
		return inner.Submit(ctx, end) // not armed yet: pass straight through (e.g. state priming)
	}
	i := c.seen
	c.seen++
	if i < c.holdN {
		c.buffered = append(c.buffered, end)
		c.mu.Unlock()
		return nil // hold it; the test releases it later via ReleaseReversed
	}
	c.mu.Unlock()
	return c.inner.Submit(ctx, end) // passthrough (e.g. cascade re-execution)
}

// BufferedLen reports how many endorsements are currently held.
func (c *SubmitterControl) BufferedLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buffered)
}

// ReleaseReversed forwards every buffered endorsement to the inner submitter in REVERSE
// Submit-call order (last-buffered first) and clears the buffer. With two buffered
// batches [k, k+1] this submits k+1 before k -- the out-of-order submission that makes
// the committer MVCC-abort the dependent batch k+1.
func (c *SubmitterControl) ReleaseReversed(ctx context.Context) error {
	c.mu.Lock()
	buf := c.buffered
	c.buffered = nil
	inner := c.inner
	c.mu.Unlock()
	for i := len(buf) - 1; i >= 0; i-- {
		if err := inner.Submit(ctx, buf[i]); err != nil {
			return err
		}
	}
	return nil
}

// NewLocalTestHarnessWithSubmitterControl builds a block-sync local harness (in-process
// fabrictest, memory endorser -- the gateway IS a block handler and is resolved by the
// real ledger's per-tx validity) whose single orderer submitter is wrapped by a
// controllableSubmitter. SubmitterCount is forced to 1 so submission is a serial FIFO
// stream the returned SubmitterControl can reorder deterministically. holdN is the number
// of initial batch submissions to buffer for reordering.
func NewLocalTestHarnessWithSubmitterControl(t *testing.T, logger sdk.Logger, evmConfig execution.EVMConfig, networkType string, holdN int, configOverrides map[string]any) (*TestHarness, *SubmitterControl, error) {
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
	// SubmitterCount==1: one submitter, one BatchSubmitter worker, so Submit calls
	// arrive serially in the executor's FIFO submission order -- essential for the
	// SubmitterControl to deterministically reorder them.
	cfg.Gateway.SubmitterCount = 1

	// Exactly one VersionedCache for this harness's one gateway, shared by every
	// endorser built below via cacheWrap -- see buildTestHarness's doc comment.
	cache := core.NewVersionedCache()
	cacheWrap := func(s execution.KVSSnapshotter) execution.KVSSnapshotter {
		return core.NewCachedSnapshotter(s, cache)
	}

	endorsers, err := prepareHarnessConfig(t, &cfg, &evmConfig, configOverrides, defaultEndorserFactory, cacheWrap)
	if err != nil {
		return nil, nil, err
	}

	if !bypass {
		// nil RecordGetter: the fabrictest committer validates each block's read
		// versions against its OWN world-state DB -- essential here: the committer
		// must validate against its own synchronously-committed world state so k+1's
		// out-of-order arrival genuinely aborts. See the identical wiring in
		// NewLocalTestHarnessWithFactory for the full rationale.
		nw, err := fabrictest.Start(t.Context(), cfg.Network.Namespace, networkType, fabrictest.Config{}, nil)
		if err != nil {
			t.Fatalf("fabrictest.Start: %v", err)
		}
		// Don't register cleanup for nw.Stop - fabrictest.Start already registers its own cleanup internally
		orderer.Port = nw.OrdererPort
		peer.Port = nw.PeerPort
	}

	ctl := &SubmitterControl{holdN: holdN}
	// INVARIANT: this closure assumes exactly one submitter (cfg.Gateway.SubmitterCount
	// forced to 1 above). It records each wrapped submitter as the SINGLE ctl.inner, so
	// with SubmitterCount > 1 every controllableSubmitter would share one ctl and only the
	// last-wrapped submitter's inner would be reachable -- ReleaseReversed/passthrough would
	// then forward to the wrong submitter. Do not relax the SubmitterCount==1 clamp without
	// making ctl hold a per-submitter inner.
	wrap := func(s core.Submitter) core.Submitter {
		ctl.inner = s
		return &controllableSubmitter{ctl: ctl}
	}
	th, _, err := buildTestHarnessWithExtraHandler(t, logger, cfg, evmConfig, "", bypass, endorsers, false, nil, cache, nil, wrap)
	if err != nil {
		return nil, nil, err
	}
	return th, ctl, nil
}
