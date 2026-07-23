/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query_test

import (
	"context"
	"net"
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
