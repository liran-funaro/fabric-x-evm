/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInflightLimiterDisabledWhenNonPositive(t *testing.T) {
	require.Nil(t, newInflightLimiter(0))
	require.Nil(t, newInflightLimiter(-1))

	// A nil limiter is the disabled case: every method must be safe on it so the
	// feeder needs no branch and the default path stays exactly as it was.
	var l *inflightLimiter
	require.NoError(t, l.Acquire(context.Background()))
	l.Release(5)
}

func TestInflightLimiterAdmitsUpToMax(t *testing.T) {
	l := newInflightLimiter(2)
	require.NoError(t, l.Acquire(context.Background()))
	require.NoError(t, l.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, l.Acquire(ctx), "third acquire must block past the bound")
}

func TestInflightLimiterReleaseUnblocksAcquire(t *testing.T) {
	l := newInflightLimiter(1)
	require.NoError(t, l.Acquire(context.Background()))

	acquired := make(chan error, 1)
	go func() { acquired <- l.Acquire(context.Background()) }()

	select {
	case <-acquired:
		t.Fatal("acquire returned while the bound was full")
	case <-time.After(50 * time.Millisecond):
	}

	l.Release(1)
	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("release did not unblock the waiting acquire")
	}
}

func TestInflightLimiterAcquireHonoursContext(t *testing.T) {
	l := newInflightLimiter(1)
	require.NoError(t, l.Acquire(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	require.ErrorIs(t, l.Acquire(ctx), context.Canceled)
}

func TestInflightLimiterOverReleaseDoesNotAddCapacity(t *testing.T) {
	l := newInflightLimiter(1)

	// Nothing is held, so this must not create slots. A rolled-back batch's EVM
	// txs are re-batched and credited when a later batch commits them, so
	// crediting them twice would silently defeat the cap.
	l.Release(10)
	require.NoError(t, l.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, l.Acquire(ctx), "over-release must not raise the bound")
}

// TestInflightLimiterBoundHoldsUnderConcurrency is the property the overnight run
// actually depends on: many feeders and many completers, and the in-flight count
// never exceeds the bound.
func TestInflightLimiterBoundHoldsUnderConcurrency(t *testing.T) {
	const bound = 8
	l := newInflightLimiter(bound)

	const totalWork = 2000

	var (
		mu       sync.Mutex
		inflight int
		peak     int
	)
	done := make(chan struct{})
	completions := make(chan int, totalWork)

	// Completer: releases slots as "commits" land.
	go func() {
		defer close(done)
		for n := range completions {
			mu.Lock()
			inflight -= n
			mu.Unlock()
			l.Release(n)
		}
	}()

	for range totalWork {
		require.NoError(t, l.Acquire(context.Background()))
		mu.Lock()
		inflight++
		if inflight > peak {
			peak = inflight
		}
		mu.Unlock()
		completions <- 1
	}
	close(completions)
	<-done

	require.LessOrEqual(t, peak, bound, "in-flight peaked at %d, above the bound %d", peak, bound)
	require.Positive(t, peak, "test did not exercise the limiter")
}
