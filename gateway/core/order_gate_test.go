package core

import (
	"testing"
	"time"
)

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Arm then Observe the armed TxID closes the channel.
func TestOrderGate_ObserveArmedClosesChannel(t *testing.T) {
	g := NewOrderGate()
	ch := g.Arm("tx-k")
	if closed(ch) {
		t.Fatal("channel closed before observe")
	}
	g.Observe([]string{"other", "tx-k", "more"})
	if !closed(ch) {
		t.Fatal("channel not closed after observing armed TxID")
	}
}

// Observing a non-armed TxID is a no-op; a later matching observe still fires.
func TestOrderGate_ObserveNonMatchingIsNoOp(t *testing.T) {
	g := NewOrderGate()
	ch := g.Arm("tx-k")
	g.Observe([]string{"tx-a", "tx-b"})
	if closed(ch) {
		t.Fatal("channel closed on non-matching observe")
	}
	g.Observe([]string{"tx-k"})
	if !closed(ch) {
		t.Fatal("channel not closed after matching observe")
	}
}

// Disarm clears the slot: a subsequent observe of the old TxID does not panic
// and does not affect a NEW arm.
func TestOrderGate_DisarmClearsSlot(t *testing.T) {
	g := NewOrderGate()
	_ = g.Arm("tx-k")
	g.Disarm()
	g.Observe([]string{"tx-k"}) // stale; must not panic
	ch2 := g.Arm("tx-j")
	g.Observe([]string{"tx-j"})
	if !closed(ch2) {
		t.Fatal("new arm not signaled after disarm of previous")
	}
}

// Double-observe of the armed TxID is idempotent (block redelivery / same tx in
// two orderers' views must not double-close).
func TestOrderGate_ObserveIdempotent(t *testing.T) {
	g := NewOrderGate()
	ch := g.Arm("tx-k")
	g.Observe([]string{"tx-k"})
	g.Observe([]string{"tx-k"}) // must not panic (no double close)
	if !closed(ch) {
		t.Fatal("channel not closed")
	}
}

// Concurrent Observe (consumer goroutine) + Arm (worker) is race-free and every
// arm is eventually signaled. The observer loops continuously (not a fixed
// iteration count) so that an Observe always follows each Arm — otherwise a
// fixed-count observer could exhaust its iterations before the arm loop starts,
// leaving later arms with no observer. Under -race this exercises concurrent
// mutex-protected access to the armed slot from two goroutines.
func TestOrderGate_ConcurrentArmObserve(t *testing.T) {
	g := NewOrderGate()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				g.Observe([]string{"tx-k"})
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		ch := g.Arm("tx-k")
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Error("arm never signaled")
			close(stop)
			<-done
			return
		}
	}
	close(stop)
	<-done
}
