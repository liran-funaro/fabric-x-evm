/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"

	"github.com/hyperledger/fabric-x-evm/endorser/storage"
)

// MemClient is an in-memory QueryClient over a RevertibleLightKVS. It is used by
// tests and the embedded/Hardhat path: the backing store is kept current by the
// harness's block/notification handlers and supports snapshot/revert. Views are
// trivial (reads observe the store's latest snapshot); this is sufficient for
// single-threaded in-process tests.
type MemClient struct {
	kvs *storage.RevertibleLightKVS
}

var _ QueryClient = (*MemClient)(nil)

// NewMemClient wraps kvs as a QueryClient.
func NewMemClient(kvs *storage.RevertibleLightKVS) *MemClient { return &MemClient{kvs: kvs} }

// Backing exposes the underlying store so callers can register it as a block
// handler and use its revert API (test RPC).
func (c *MemClient) Backing() *storage.RevertibleLightKVS { return c.kvs }

func (c *MemClient) BeginView(context.Context) (string, error) { return "mem", nil }

func (c *MemClient) EndView(context.Context, string) error { return nil }

func (c *MemClient) Close() error { return nil }

func (c *MemClient) GetRows(_ context.Context, _ string, ns string, keys [][]byte) ([]Row, error) {
	snap, err := c.kvs.NewSnapshot(0) // 0 = latest
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	rows := make([]Row, 0, len(keys))
	for _, k := range keys {
		rec, err := snap.Get(ns, string(k))
		if err != nil {
			return nil, err
		}
		if rec == nil {
			continue // absent key: omit, matching the query service
		}
		rows = append(rows, Row{Key: k, Value: rec.Value, Version: rec.Version})
	}
	return rows, nil
}
