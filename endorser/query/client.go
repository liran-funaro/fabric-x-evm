/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

// Package query reads committed world state from the Fabric-X query service.
// It replaces the endorser's previously-synced local state DB: instead of
// following committed blocks into a local VersionedDB, an endorser opens a
// consistent (SERIALIZABLE) view on the query service and reads the keys it
// needs on demand.
package query

import "context"

// Row is one (key, value, version) triple returned for a view read. Version is
// the Fabric-X per-key monotonic MVCC version.
type Row struct {
	Key     []byte
	Value   []byte
	Version uint64
}

// QueryClient is the view/read primitive backing state reads. Implementations:
// a gRPC client to the query service (production) and an in-memory client
// (tests/embedded).
type QueryClient interface {
	// BeginView pins a consistent snapshot of committed state and returns its id.
	BeginView(ctx context.Context) (viewID string, err error)
	// GetRows reads the given keys of one namespace under the view. Keys with no
	// committed value are omitted from the result.
	GetRows(ctx context.Context, viewID, ns string, keys [][]byte) ([]Row, error)
	// EndView releases the view.
	EndView(ctx context.Context, viewID string) error
	// Close releases any client resources (e.g. the gRPC connection).
	Close() error
}
