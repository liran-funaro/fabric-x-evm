/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/endorser/query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeQS struct {
	committerpb.UnimplementedQueryServiceServer
	data map[string][]byte
	vers map[string]uint64
}

func (f *fakeQS) BeginView(context.Context, *committerpb.ViewParameters) (*committerpb.View, error) {
	return &committerpb.View{Id: "v1"}, nil
}
func (f *fakeQS) EndView(context.Context, *committerpb.View) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (f *fakeQS) GetRows(_ context.Context, q *committerpb.Query) (*committerpb.Rows, error) {
	out := &committerpb.Rows{}
	for _, ns := range q.GetNamespaces() {
		rns := &committerpb.RowsNamespace{NsId: ns.GetNsId()}
		for _, k := range ns.GetKeys() {
			if v, ok := f.data[string(k)]; ok {
				rns.Rows = append(rns.Rows, &committerpb.Row{Key: k, Value: v, Version: f.vers[string(k)]})
			}
		}
		out.Namespaces = append(out.Namespaces, rns)
	}
	return out, nil
}

func TestGRPCClientRoundTrip(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	committerpb.RegisterQueryServiceServer(srv, &fakeQS{
		data: map[string][]byte{"k1": []byte("v1")},
		vers: map[string]uint64{"k1": 5},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c := query.NewGRPCClient(conn, time.Second)
	defer c.Close()

	ctx := context.Background()
	view, err := c.BeginView(ctx)
	if err != nil || view != "v1" {
		t.Fatalf("BeginView = %q, %v", view, err)
	}
	rows, err := c.GetRows(ctx, view, "evm", [][]byte{[]byte("k1"), []byte("absent")})
	if err != nil {
		t.Fatalf("GetRows: %v", err)
	}
	if len(rows) != 1 || string(rows[0].Value) != "v1" || rows[0].Version != 5 {
		t.Fatalf("rows = %+v, want one row v1/5", rows)
	}
	if err := c.EndView(ctx, view); err != nil {
		t.Fatalf("EndView: %v", err)
	}
}

// countingQS records how many BeginView/GetRows calls it served, so a test can
// observe which connection in a pool a request landed on. Every instance serves
// the same view id, modelling a server-side view queryable over any connection.
type countingQS struct {
	committerpb.UnimplementedQueryServiceServer
	begins  atomic.Int64
	getRows atomic.Int64
	value   []byte
}

func (f *countingQS) BeginView(context.Context, *committerpb.ViewParameters) (*committerpb.View, error) {
	f.begins.Add(1)
	return &committerpb.View{Id: "v1"}, nil
}
func (f *countingQS) EndView(context.Context, *committerpb.View) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (f *countingQS) GetRows(_ context.Context, q *committerpb.Query) (*committerpb.Rows, error) {
	f.getRows.Add(1)
	out := &committerpb.Rows{}
	for _, ns := range q.GetNamespaces() {
		rns := &committerpb.RowsNamespace{NsId: ns.GetNsId()}
		for _, k := range ns.GetKeys() {
			rns.Rows = append(rns.Rows, &committerpb.Row{Key: k, Value: f.value, Version: 1})
		}
		out.Namespaces = append(out.Namespaces, rns)
	}
	return out, nil
}

// TestGRPCClientPoolRoundRobin verifies that a pooled client spreads GetRows
// evenly across its connections while pinning BeginView to the first connection,
// and that a view begun on the first connection is served over every connection
// (server-side views are addressed by id, not by connection).
func TestGRPCClientPoolRoundRobin(t *testing.T) {
	const numConns = 3
	const callsPerConn = 4

	fakes := make([]*countingQS, numConns)
	conns := make([]*grpc.ClientConn, numConns)
	for i := range numConns {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		fakes[i] = &countingQS{value: []byte("v")}
		committerpb.RegisterQueryServiceServer(srv, fakes[i])
		go srv.Serve(lis)
		defer srv.Stop()

		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = conn
	}

	c := query.NewGRPCClientPool(conns, time.Second)
	defer c.Close()

	ctx := context.Background()
	view, err := c.BeginView(ctx)
	if err != nil || view != "v1" {
		t.Fatalf("BeginView = %q, %v", view, err)
	}

	for range numConns * callsPerConn {
		rows, err := c.GetRows(ctx, view, "evm", [][]byte{[]byte("k")})
		if err != nil {
			t.Fatalf("GetRows: %v", err)
		}
		if len(rows) != 1 || string(rows[0].Value) != "v" {
			t.Fatalf("rows = %+v, want one row v", rows)
		}
	}

	// BeginView is pinned to the first connection only.
	if got := fakes[0].begins.Load(); got != 1 {
		t.Errorf("conn[0] BeginView count = %d, want 1", got)
	}
	for i := 1; i < numConns; i++ {
		if got := fakes[i].begins.Load(); got != 0 {
			t.Errorf("conn[%d] BeginView count = %d, want 0 (BeginView must pin to conn[0])", i, got)
		}
	}

	// GetRows is spread evenly (round-robin) across every connection -- proving no
	// single connection serialises the batch's reads.
	for i := range numConns {
		if got := fakes[i].getRows.Load(); got != callsPerConn {
			t.Errorf("conn[%d] GetRows count = %d, want %d (round-robin)", i, got, callsPerConn)
		}
	}
}

// viewCapturingQS records the View field of every GetRows query it serves, so a
// test can assert the client sends View:nil for an empty viewID and a concrete
// View for a real one. Guarded by a mutex: GetRows runs on the server's handler
// goroutine, the test reads on its own.
type viewCapturingQS struct {
	committerpb.UnimplementedQueryServiceServer
	mu    sync.Mutex
	views []*committerpb.View
}

func (f *viewCapturingQS) BeginView(context.Context, *committerpb.ViewParameters) (*committerpb.View, error) {
	return &committerpb.View{Id: "v1"}, nil
}
func (f *viewCapturingQS) EndView(context.Context, *committerpb.View) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (f *viewCapturingQS) GetRows(_ context.Context, q *committerpb.Query) (*committerpb.Rows, error) {
	f.mu.Lock()
	f.views = append(f.views, q.GetView())
	f.mu.Unlock()
	out := &committerpb.Rows{}
	for _, ns := range q.GetNamespaces() {
		out.Namespaces = append(out.Namespaces, &committerpb.RowsNamespace{NsId: ns.GetNsId()})
	}
	return out, nil
}

// GetRows sends View:nil on the wire when the viewID is empty -- the query
// service's non-consistent (nil-view) read path, which reads current committed
// state on a fresh connection rather than a snapshot pinned by BeginView and
// shared across the view-aggregation window -- and a concrete View when the
// viewID is set. See GRPCClient.GetRows and Store.nilView.
func TestGRPCClientGetRowsNilViewForEmptyViewID(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	qs := &viewCapturingQS{}
	committerpb.RegisterQueryServiceServer(srv, qs)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c := query.NewGRPCClient(conn, time.Second)
	defer c.Close()

	ctx := context.Background()
	// Empty viewID -> View must be nil on the wire.
	if _, err := c.GetRows(ctx, "", "evm", [][]byte{[]byte("k")}); err != nil {
		t.Fatalf("GetRows (empty view): %v", err)
	}
	// Non-empty viewID -> View must carry the id.
	if _, err := c.GetRows(ctx, "v1", "evm", [][]byte{[]byte("k")}); err != nil {
		t.Fatalf("GetRows (view v1): %v", err)
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if len(qs.views) != 2 {
		t.Fatalf("served %d GetRows, want 2", len(qs.views))
	}
	if qs.views[0] != nil {
		t.Fatalf("empty viewID sent View=%+v, want nil (nil-view read path)", qs.views[0])
	}
	if qs.views[1] == nil || qs.views[1].GetId() != "v1" {
		t.Fatalf("viewID v1 sent View=%+v, want {Id:v1}", qs.views[1])
	}
}

// blockingQS is a fake QueryServiceServer whose GetRows blocks until the
// request context is cancelled, simulating an unresponsive query service.
type blockingQS struct {
	committerpb.UnimplementedQueryServiceServer
}

func (f *blockingQS) BeginView(context.Context, *committerpb.ViewParameters) (*committerpb.View, error) {
	return &committerpb.View{Id: "v1"}, nil
}
func (f *blockingQS) EndView(context.Context, *committerpb.View) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (f *blockingQS) GetRows(ctx context.Context, _ *committerpb.Query) (*committerpb.Rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestGRPCClientGetRowsTimesOut verifies that GetRows applies a per-RPC
// deadline derived from viewTimeout, so an unresponsive query service can't
// block the caller indefinitely.
func TestGRPCClientGetRowsTimesOut(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	committerpb.RegisterQueryServiceServer(srv, &blockingQS{})
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c := query.NewGRPCClient(conn, 100*time.Millisecond)
	defer c.Close()

	done := make(chan struct{})
	var rows []query.Row
	var getErr error
	go func() {
		rows, getErr = c.GetRows(context.Background(), "v1", "evm", [][]byte{[]byte("k1")})
		close(done)
	}()

	select {
	case <-done:
		if getErr == nil {
			t.Fatalf("GetRows returned no error, rows = %+v; want deadline-exceeded error", rows)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetRows did not return within 5s; per-RPC deadline was not applied")
	}
}
