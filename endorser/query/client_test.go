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
}

// TestMemClientGetRowsOmitsDeletedRecordAfterRevert covers the case flagged in
// review: RevertibleLightKVS.RevertToBlock's merge can leave a key's record
// with IsDelete: true and a stale non-nil Value (the value it had at the
// target block, before it was deleted). GetRows must still treat this as an
// absent key, not a live row.
func TestMemClientGetRowsOmitsDeletedRecordAfterRevert(t *testing.T) {
	kvs := storage.NewRevertibleLightKVS(storage.NewLightKVS(3))

	// Block 1: write "evm:k1" = "v1" (version 0).
	if err := kvs.Update([]storage.KeyValueVersion{
		{Key: "evm:k1", Value: []byte("v1"), BlockNum: 1},
	}); err != nil {
		t.Fatalf("update block 1: %v", err)
	}

	// Block 2: delete "evm:k1".
	if err := kvs.Update([]storage.KeyValueVersion{
		{Key: "evm:k1", IsDelete: true, BlockNum: 2},
	}); err != nil {
		t.Fatalf("update block 2: %v", err)
	}

	// Revert to block 1. The merge in RevertToBlock finds "evm:k1" existed at
	// the target block (1) but not at the current block (2, where it was
	// deleted), so it marks the merged record IsDelete: true while keeping
	// the target snapshot's stale, non-nil Value ("v1") for reference.
	if err := kvs.RevertToBlock(1); err != nil {
		t.Fatalf("revert: %v", err)
	}

	c := query.NewMemClient(kvs)
	ctx := context.Background()
	view, err := c.BeginView(ctx)
	if err != nil {
		t.Fatalf("BeginView: %v", err)
	}

	rows, err := c.GetRows(ctx, view, "evm", [][]byte{[]byte("k1")})
	if err != nil {
		t.Fatalf("GetRows: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("GetRows = %+v, want no rows for a deleted-after-revert key", rows)
	}

	if err := c.EndView(ctx, view); err != nil {
		t.Fatalf("EndView: %v", err)
	}
}
