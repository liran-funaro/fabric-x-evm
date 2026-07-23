/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query_test

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-x-evm/endorser/query"
	"github.com/hyperledger/fabric-x-evm/endorser/storage"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func TestMemClientGetRowsReflectsWrites(t *testing.T) {
	kvs := storage.NewRevertibleLightKVS(storage.NewLightKVS(2))
	// Seed one committed key at version 1 via the KVS batch-update API.
	// storage.KeyValueVersion has no Namespace/Version fields: the namespace is
	// folded into Key as "ns:key" (see Reader.Get), and Update assigns versions
	// automatically (existing version + 1, starting at 0 for a brand-new key).
	// So write "evm:k1" twice to land it at version 1.
	if err := kvs.Update([]storage.KeyValueVersion{
		{Key: "evm:k1", Value: []byte("v0"), BlockNum: 1},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := kvs.Update([]storage.KeyValueVersion{
		{Key: "evm:k1", Value: []byte("v1"), BlockNum: 2},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	c := query.NewMemClient(kvs)
	ctx := context.Background()
	view, err := c.BeginView(ctx)
	if err != nil {
		t.Fatalf("BeginView: %v", err)
	}

	rows, err := c.GetRows(ctx, view, "evm", [][]byte{[]byte("k1"), []byte("absent")})
	if err != nil {
		t.Fatalf("GetRows: %v", err)
	}
	got := map[string]query.Row{}
	for _, r := range rows {
		got[string(r.Key)] = r
	}
	if r, ok := got["k1"]; !ok || string(r.Value) != "v1" || r.Version != 1 {
		t.Fatalf("k1 row = %+v (ok=%v), want value v1 version 1", got["k1"], ok)
	}
	if _, ok := got["absent"]; ok {
		t.Fatalf("absent key should not be returned, got %+v", got["absent"])
	}
	if err := c.EndView(ctx, view); err != nil {
		t.Fatalf("EndView: %v", err)
	}
	_ = blocks.WriteRecord{} // ensure blocks import is real
}
