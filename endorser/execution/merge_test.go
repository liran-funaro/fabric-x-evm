package execution

import (
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

func TestMergeResultsUnionAndLastWrite(t *testing.T) {
	v := &blocks.Version{BlockNum: 7}
	r1 := endorsement.ExecutionResult{
		RWS:   blocks.ReadWriteSet{Reads: []blocks.KVRead{{Key: "a", Version: v}}, Writes: []blocks.KVWrite{{Key: "a", Value: []byte("1")}}},
		Event: []byte("e1"),
	}
	r2 := endorsement.ExecutionResult{
		RWS:   blocks.ReadWriteSet{Reads: []blocks.KVRead{{Key: "a", Version: v}, {Key: "b", Version: nil}}, Writes: []blocks.KVWrite{{Key: "a", Value: []byte("2")}, {Key: "b", Value: []byte("9")}}},
		Event: []byte("e2"),
	}

	rws, events := MergeResults([]endorsement.ExecutionResult{r1, r2})

	// Reads: union by key -> {a,b}
	readKeys := map[string]bool{}
	for _, rd := range rws.Reads {
		readKeys[rd.Key] = true
	}
	if !readKeys["a"] || !readKeys["b"] || len(rws.Reads) != 2 {
		t.Fatalf("reads = %+v, want union {a,b}", rws.Reads)
	}
	// Writes: last-write-wins -> a="2", b="9"
	got := map[string]string{}
	for _, w := range rws.Writes {
		got[w.Key] = string(w.Value)
	}
	if got["a"] != "2" || got["b"] != "9" || len(rws.Writes) != 2 {
		t.Fatalf("writes = %+v, want a=2 b=9", rws.Writes)
	}
	// Per-tx events preserved by sub-index.
	if len(events) != 2 || string(events[0]) != "e1" || string(events[1]) != "e2" {
		t.Fatalf("events = %v, want [e1 e2]", events)
	}
}
