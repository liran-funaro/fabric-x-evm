package query_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-x-evm/endorser/query"
)

// stubClient counts GetRows calls and serves a fixed map.
type stubClient struct {
	data  map[string][]byte
	vers  map[string]uint64
	calls atomic.Int64
	ended atomic.Bool
}

func (s *stubClient) BeginView(context.Context) (string, error) { return "v", nil }
func (s *stubClient) EndView(context.Context, string) error     { s.ended.Store(true); return nil }
func (s *stubClient) Close() error                              { return nil }
func (s *stubClient) GetRows(_ context.Context, _, _ string, keys [][]byte) ([]query.Row, error) {
	s.calls.Add(1)
	var rows []query.Row
	for _, k := range keys {
		if v, ok := s.data[string(k)]; ok {
			rows = append(rows, query.Row{Key: k, Value: v, Version: s.vers[string(k)]})
		}
	}
	return rows, nil
}

func TestViewGetMapsRowAndCachesAndCloses(t *testing.T) {
	c := &stubClient{
		data: map[string][]byte{"k1": []byte("v1")},
		vers: map[string]uint64{"k1": 7},
	}
	store := query.NewStore(c, "evm")
	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	rec, err := rs.Get("evm", "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec == nil || string(rec.Value) != "v1" || rec.Version != 7 {
		t.Fatalf("record = %+v, want value v1 version 7", rec)
	}
	// Second read of the same key must hit the in-view cache, not GetRows again.
	if _, err := rs.Get("evm", "k1"); err != nil {
		t.Fatalf("Get 2: %v", err)
	}
	if got := c.calls.Load(); got != 1 {
		t.Fatalf("GetRows calls = %d, want 1 (cached)", got)
	}
	// Absent key -> nil record, no error.
	rec2, err := rs.Get("evm", "missing")
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if rec2 != nil {
		t.Fatalf("missing key record = %+v, want nil", rec2)
	}
	if err := rs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.ended.Load() {
		t.Fatalf("Close did not call EndView")
	}
}
