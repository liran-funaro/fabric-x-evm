/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/stretchr/testify/require"
)

func rec(key string, version uint64, val string) *blocks.WriteRecord {
	return &blocks.WriteRecord{Key: key, Version: version, BlockNum: version, Value: []byte(val)}
}

// stageN reads key n times this batch (simulating n txs reading it in the warm
// pass), so maintain sees a candidate with seen == n.
func stageN(c *ReadOnlyCache, key string, version uint64, val string, n int) {
	r := rec(key, version, val)
	for i := 0; i < n; i++ {
		c.stage(key, r)
	}
}

func TestReadOnlyCache_MissBeforeAdmission(t *testing.T) {
	c := newReadOnlyCache(8, 4)
	stageN(c, "hot", 1, "v", 10) // staged but not yet admitted (no maintain)
	_, ok := c.get("hot")
	require.False(t, ok, "candidate must not be readable before a boundary admits it")
}

func TestReadOnlyCache_AdmitAtThreshold(t *testing.T) {
	c := newReadOnlyCache(8, 4)

	stageN(c, "below", 1, "b", 3) // 3 < threshold 4 -> not admitted
	stageN(c, "at", 2, "a", 4)    // 4 == threshold   -> admitted
	stageN(c, "above", 3, "x", 9) // 9 > threshold    -> admitted
	c.maintain(nil)

	_, ok := c.get("below")
	require.False(t, ok, "sub-threshold candidate must not be admitted")

	got, ok := c.get("at")
	require.True(t, ok)
	require.Equal(t, "a", string(got.Value))

	_, ok = c.get("above")
	require.True(t, ok)
}

func TestReadOnlyCache_VersionFaithful(t *testing.T) {
	c := newReadOnlyCache(8, 1)
	src := &blocks.WriteRecord{Key: "k", Version: 42, BlockNum: 7, TxNum: 3, Value: []byte("val")}
	c.stage("k", src)
	c.maintain(nil)

	got, ok := c.get("k")
	require.True(t, ok)
	require.Equal(t, uint64(42), got.Version)
	require.Equal(t, uint64(7), got.BlockNum)
	require.Equal(t, uint64(3), got.TxNum)
	require.Equal(t, "val", string(got.Value))

	// Returned record is a copy: mutating it must not corrupt the cached entry.
	got.Value = []byte("tampered")
	again, _ := c.get("k")
	require.Equal(t, "val", string(again.Value))
}

func TestReadOnlyCache_EvictOnWriteThenReadmit(t *testing.T) {
	c := newReadOnlyCache(8, 1)
	stageN(c, "k", 1, "old", 5)
	c.maintain(nil)
	_, ok := c.get("k")
	require.True(t, ok, "k admitted")

	// k is written by this gateway -> queued for eviction; boundary drops it so a
	// stale pre-write value is never served after the write commits.
	c.maintain([]string{"k"})
	_, ok = c.get("k")
	require.False(t, ok, "written key must be evicted at the boundary")

	// After the write, k is re-observed at its new committed version and re-admitted.
	stageN(c, "k", 2, "new", 5)
	c.maintain(nil)
	got, ok := c.get("k")
	require.True(t, ok)
	require.Equal(t, uint64(2), got.Version)
	require.Equal(t, "new", string(got.Value))
}

func TestReadOnlyCache_EvictDropsStagedSameKey(t *testing.T) {
	// A key both read (staged, pre-write value) AND written in the same batch must
	// NOT be admitted at that boundary: the staged value predates the write.
	c := newReadOnlyCache(8, 1)
	stageN(c, "k", 1, "pre", 5)
	c.maintain([]string{"k"}) // written this batch
	_, ok := c.get("k")
	require.False(t, ok, "candidate for a key written this batch must be dropped, not admitted")
}

func TestReadOnlyCache_MFUCapacityEvictsLeastUsed(t *testing.T) {
	c := newReadOnlyCache(3, 1)

	// Admit 5 keys with distinct read frequencies; capacity is 3, so the 2
	// least-used must be evicted, keeping the 3 most-frequently-used.
	stageN(c, "a", 1, "a", 1)
	stageN(c, "b", 1, "b", 2)
	stageN(c, "c", 1, "c", 3)
	stageN(c, "d", 1, "d", 10)
	stageN(c, "e", 1, "e", 20)
	c.maintain(nil)

	require.Equal(t, 3, c.len())
	for _, k := range []string{"c", "d", "e"} {
		_, ok := c.get(k)
		require.Truef(t, ok, "most-used key %q must be retained", k)
	}
	for _, k := range []string{"a", "b"} {
		_, ok := c.get(k)
		require.Falsef(t, ok, "least-used key %q must be evicted", k)
	}
}

func TestReadOnlyCache_UsesAccrueAcrossBatchesRaisingRank(t *testing.T) {
	c := newReadOnlyCache(2, 1)

	// Batch 1: admit "steady" (seen once, the LEAST-used entry) and "spike"
	// (seen 5x). Both fit (cap 2).
	stageN(c, "steady", 1, "s", 1)
	stageN(c, "spike", 1, "p", 5)
	c.maintain(nil)
	require.Equal(t, 2, c.len())

	// "steady" is read heavily over subsequent batches -- hits accrue via get() --
	// while "spike" stays cold. Its use count climbs from 1 to 51.
	for i := 0; i < 50; i++ {
		_, ok := c.get("steady")
		require.True(t, ok)
	}

	// Batch 2: a newcomer (seen 10x) forces one eviction (cap 2). The least-used
	// resident is now "spike" (5), NOT "steady" (51) -- proving accrued frequency,
	// not admission order or recency, drives retention.
	stageN(c, "newcomer", 1, "n", 10)
	c.maintain(nil)

	require.Equal(t, 2, c.len())
	_, ok := c.get("steady")
	require.True(t, ok, "heavily-hit key must survive despite the lowest admission rank")
	_, ok = c.get("newcomer")
	require.True(t, ok, "newcomer (10 uses) outranks the cold spike (5)")
	_, ok = c.get("spike")
	require.False(t, ok, "cold key that never accrued hits is the one evicted")
}

func TestReadOnlyCache_DefaultsOnMisWire(t *testing.T) {
	c := newReadOnlyCache(0, 0)
	require.Equal(t, DefaultReadOnlyCacheCapacity, c.capacity)
	require.Equal(t, uint64(DefaultReadOnlyAdmitThreshold), c.threshold)
}

// TestReadOnlyCache_ConcurrentGetStage exercises the batch-time contract: many
// goroutines get/stage concurrently (as warm-pass workers do) with structural
// mutation confined to maintain at the boundary. Run under -race.
func TestReadOnlyCache_ConcurrentGetStage(t *testing.T) {
	c := newReadOnlyCache(64, 2)

	// Pre-admit a set of hot keys.
	for i := 0; i < 32; i++ {
		stageN(c, fmt.Sprintf("hot-%d", i), uint64(i), "v", 4)
	}
	c.maintain(nil)

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c.get(fmt.Sprintf("hot-%d", i%32)) // hits
				c.get(fmt.Sprintf("cold-%d", i%9)) // misses
				c.stage(fmt.Sprintf("cold-%d", i%9), rec(fmt.Sprintf("cold-%d", i%9), 1, "c"))
			}
		}(w)
	}
	wg.Wait()

	// Boundary maintenance after the concurrent phase (single goroutine).
	c.maintain(nil)
	require.LessOrEqual(t, c.len(), 64, "capacity must hold after admissions")
}
