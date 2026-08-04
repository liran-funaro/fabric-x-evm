<!--
SPDX-License-Identifier: Apache-2.0 AND LGPL-3.0-or-later
-->

# Drop the Endorser Internal State DB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the endorser's synced local world-state DB and per-endorser synchronizer with on-demand reads from the Fabric-X query service under a pinned SERIALIZABLE view, and add a two-phase (warm/authoritative) batch executor for later reuse.

**Architecture:** A new `endorser/query` package exposes a `QueryClient` interface (`BeginView`/`GetRows`/`EndView`) with a gRPC implementation (production, `committerpb.QueryServiceClient`) and an in-memory implementation (tests/embedded, backed by the existing `RevertibleLightKVS`). A `View` implements the existing `execution.ReadStore` port and a `Store` implements `execution.KVSSnapshotter`, so `EVMEngine`/`core.Endorser` change minimally. A new `execution.BatchExecutor` runs the two-phase model. The production `VersionedDBWrapper` + endorser `Synchronizer` are removed.

**Tech Stack:** Go 1.26; go-ethereum EVM; `github.com/hyperledger/fabric-x-common/api/committerpb` (query proto/client, already a direct dep); `github.com/hyperledger/fabric-x-sdk/blocks` (`WriteRecord`); `google.golang.org/grpc` + `credentials` (already deps).

## Global Constraints

- License header on every new Go file (copy verbatim):
  ```go
  /*
  Copyright IBM Corp. All Rights Reserved.

  SPDX-License-Identifier: LGPL-3.0-or-later
  */
  ```
- Target protocol is `fabric-x`; Fabric-X MVCC uses `WriteRecord.Version` (a `uint64` "monotonically increasing per (namespace, key)"). The query service's `Row.version` is the same `uint64` — map 1:1. An absent key ⇒ `ReadStore.Get` returns `(nil, nil)` (never an error).
- Reads use isolation `SERIALIZABLE` (`committerpb.IsoLevel_SERIALIZABLE`).
- No changes to the Orderer or Committer; the query service and its config are existing infrastructure (running as `committer-query-service:7001`, mTLS, in both composes).
- Run Go tests with `go test ./<pkg>/... -run <Name> -count=1 -v`. Build with `make build` (produces `bin/fxevm`). Lint per repo (`make lint` if present).
- Keep the gateway indexer synchronizer (`gwSync`) and `Chain` (SQLite + trie) untouched.

---

## File Structure

**New package `endorser/query/`:**
- `client.go` — `QueryClient` interface, `Row` type, `IsoLevel` passthrough constant.
- `mem_client.go` — `memClient` over a `*storage.RevertibleLightKVS`; exposes the backing store.
- `grpc_client.go` — `grpcClient` over `committerpb.QueryServiceClient` + a `Dial` helper building mTLS creds from `common.ClientConfig`.
- `view.go` — `View` (`execution.ReadStore`) + `Store` (`execution.KVSSnapshotter`).
- `client_test.go`, `view_test.go`, `grpc_client_test.go` — unit tests.

**Modified `endorser/execution/`:**
- `batch_executor.go` (new) — `BatchExecutor` two-phase engine + a `runTx(reader, tx)` primitive extracted from `newExecutor`.
- `batch_executor_test.go` (new).

**Modified config/wiring/docs:**
- `endorser/config/config.go` — add `QueryService`, `ViewTimeout`; change `Database` values; update `Validate`.
- `endorser/app/factory.go` — build `query.Store`; drop the synchronizer from `NewEndorser`.
- `gateway/app/app.go` — remove `endorserSyncs`; source the test-RPC revertible store from the memory path.
- `integration/test_helpers.go` — build endorsers on `query.Store`; keep the backing revertible KVS as `EndorserComponents.KVS`.
- `endorser/storage/versioned_db_wrapper.go` — deleted.
- `integration/fabx.yaml`, `integration/fabx-full.yaml`, gateway config — add the query-service endpoint; set endorser `database: query-service`.
- `docs/COMPATIBILITY.md`, `docs/ARCHITECTURE.md` — update.

---

## Task 1: `QueryClient` interface + `Row` + in-memory client

**Files:**
- Create: `endorser/query/client.go`
- Create: `endorser/query/mem_client.go`
- Test: `endorser/query/client_test.go`

**Interfaces:**
- Produces:
  - `type Row struct { Key []byte; Value []byte; Version uint64 }`
  - `type QueryClient interface { BeginView(ctx context.Context) (string, error); GetRows(ctx context.Context, viewID, ns string, keys [][]byte) ([]Row, error); EndView(ctx context.Context, viewID string) error; Close() error }`
  - `func NewMemClient(kvs *storage.RevertibleLightKVS) *MemClient` and `func (*MemClient) Backing() *storage.RevertibleLightKVS`

- [ ] **Step 1: Write the failing test** — `endorser/query/client_test.go`

```go
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
	if err := kvs.Update(1, []storage.KeyValueVersion{
		{Namespace: "evm", Key: "k1", Value: []byte("v1"), Version: 1},
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/query/... -run TestMemClientGetRowsReflectsWrites -count=1 -v`
Expected: FAIL — package `endorser/query` does not compile / `NewMemClient` undefined.

> Note: confirm `storage.KeyValueVersion` fields (`Namespace,Key,Value,Version`) and `RevertibleLightKVS.Update(blockNumber uint64, updates []KeyValueVersion) error` against `endorser/storage/lightkvs.go` before writing Step 3; adjust the seed call to the actual signature if it differs.

- [ ] **Step 3: Write `endorser/query/client.go`**

```go
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
```

- [ ] **Step 4: Write `endorser/query/mem_client.go`**

```go
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

// NewMemClient wraps kvs as a QueryClient.
func NewMemClient(kvs *storage.RevertibleLightKVS) *MemClient { return &MemClient{kvs: kvs} }

// Backing exposes the underlying store so callers can register it as a block
// handler and use its revert API (test RPC).
func (c *MemClient) Backing() *storage.RevertibleLightKVS { return c.kvs }

func (c *MemClient) BeginView(context.Context) (string, error) { return "mem", nil }

func (c *MemClient) EndView(context.Context, string) error { return nil }

func (c *MemClient) Close() error { return nil }

func (c *MemClient) GetRows(_ context.Context, _ , ns string, keys [][]byte) ([]Row, error) {
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./endorser/query/... -run TestMemClientGetRowsReflectsWrites -count=1 -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add endorser/query/client.go endorser/query/mem_client.go endorser/query/client_test.go
git commit -m "feat(endorser): add QueryClient interface and in-memory implementation"
```

---

## Task 2: `View` (ReadStore) + `Store` (KVSSnapshotter)

**Files:**
- Create: `endorser/query/view.go`
- Test: `endorser/query/view_test.go`

**Interfaces:**
- Consumes: `QueryClient`, `Row` (Task 1); `execution.ReadStore`, `execution.KVSSnapshotter` (existing, in `endorser/execution/executor.go`); `blocks.WriteRecord` (`fabric-x-sdk/blocks`).
- Produces:
  - `func NewStore(client QueryClient, namespace string) *Store`
  - `func (*Store) NewSnapshot(blockNumber uint64) (execution.ReadStore, error)` — implements `execution.KVSSnapshotter`
  - `type View` implementing `execution.ReadStore` (`Get(namespace, key string) (*blocks.WriteRecord, error)`, `Close() error`), with a `countingGetRows` observable in tests.

- [ ] **Step 1: Write the failing test** — `endorser/query/view_test.go`

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/query/... -run TestViewGetMapsRowAndCachesAndCloses -count=1 -v`
Expected: FAIL — `query.NewStore` undefined.

- [ ] **Step 3: Write `endorser/query/view.go`**

```go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"
	"sync"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// Store opens query-service views and hands out per-view ReadStores. It
// implements execution.KVSSnapshotter, so it drops in where the local KVS used
// to sit. blockNumber is ignored (the query service serves latest committed).
type Store struct {
	client    QueryClient
	namespace string
}

// NewStore returns a Store reading namespace ns through client.
func NewStore(client QueryClient, ns string) *Store {
	return &Store{client: client, namespace: ns}
}

// NewSnapshot begins a view and returns it as a ReadStore. The caller closes it
// (which ends the view). blockNumber is ignored.
func (s *Store) NewSnapshot(_ uint64) (execution.ReadStore, error) {
	viewID, err := s.client.BeginView(context.Background())
	if err != nil {
		return nil, err
	}
	return &View{
		client:    s.client,
		viewID:    viewID,
		namespace: s.namespace,
		cache:     make(map[string]*blocks.WriteRecord),
	}, nil
}

// Close closes the underlying client (call at endorser shutdown, not per read).
func (s *Store) Close() error { return s.client.Close() }

// View is a ReadStore backed by one query-service view. Reads are cached in the
// view so a key is fetched at most once and repeated reads are consistent.
type View struct {
	client    QueryClient
	viewID    string
	namespace string

	mu    sync.Mutex
	cache map[string]*blocks.WriteRecord // key -> record; nil value means "known absent"
}

// Get returns the record for (namespace, key), or (nil, nil) if the key has no
// committed value. namespace must equal the store's namespace.
func (v *View) Get(namespace, key string) (*blocks.WriteRecord, error) {
	v.mu.Lock()
	if rec, ok := v.cache[key]; ok {
		v.mu.Unlock()
		return rec, nil
	}
	v.mu.Unlock()

	rows, err := v.client.GetRows(context.Background(), v.viewID, namespace, [][]byte{[]byte(key)})
	if err != nil {
		return nil, err
	}

	var rec *blocks.WriteRecord
	for _, r := range rows {
		if string(r.Key) == key {
			rec = &blocks.WriteRecord{
				Namespace: namespace,
				Key:       key,
				Version:   r.Version,
				Value:     r.Value,
			}
			break
		}
	}

	v.mu.Lock()
	v.cache[key] = rec // caches nil for known-absent keys too
	v.mu.Unlock()
	return rec, nil
}

// Close ends the view.
func (v *View) Close() error {
	return v.client.EndView(context.Background(), v.viewID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/query/... -run TestViewGetMapsRowAndCachesAndCloses -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add endorser/query/view.go endorser/query/view_test.go
git commit -m "feat(endorser): add query-service View (ReadStore) and Store (KVSSnapshotter)"
```

---

## Task 3: gRPC `QueryClient` + mTLS dial helper

**Files:**
- Create: `endorser/query/grpc_client.go`
- Test: `endorser/query/grpc_client_test.go`

**Interfaces:**
- Consumes: `QueryClient`, `Row` (Task 1); `common.ClientConfig` (`common/config.go`); `committerpb.QueryServiceClient` (`fabric-x-common/api/committerpb`).
- Produces:
  - `func Dial(cfg common.ClientConfig) (*grpc.ClientConn, error)`
  - `func NewGRPCClient(conn *grpc.ClientConn, viewTimeout time.Duration) *GRPCClient` — implements `QueryClient`.

- [ ] **Step 1: Write the failing test** — `endorser/query/grpc_client_test.go`

This spins an in-process gRPC server implementing `committerpb.QueryServiceServer` over a fixed map (no committer DB needed), then exercises `GRPCClient` end-to-end through real protobuf.

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/query/... -run TestGRPCClientRoundTrip -count=1 -v`
Expected: FAIL — `query.NewGRPCClient` undefined.

- [ ] **Step 3: Write `endorser/query/grpc_client.go`**

```go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial opens a gRPC connection to the query service. TLS mode "none"/"" is
// insecure; anything else ("tls"/"mtls") loads the client cert/key and CA pool
// from cfg (mirroring the query service's TLS modes).
func Dial(cfg common.ClientConfig) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	switch cfg.TLS.Mode {
	case "", "none":
		creds = insecure.NewCredentials()
	default:
		tlsCfg := &tls.Config{ServerName: cfg.TLS.ServerName, MinVersion: tls.VersionTLS12}
		if cfg.TLS.CertPath != "" || cfg.TLS.KeyPath != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLS.CertPath, cfg.TLS.KeyPath)
			if err != nil {
				return nil, fmt.Errorf("load client keypair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		if len(cfg.TLS.CACertPaths) > 0 {
			pool := x509.NewCertPool()
			for _, p := range cfg.TLS.CACertPaths {
				pem, err := os.ReadFile(p)
				if err != nil {
					return nil, fmt.Errorf("read ca cert %s: %w", p, err)
				}
				if !pool.AppendCertsFromPEM(pem) {
					return nil, fmt.Errorf("append ca cert %s", p)
				}
			}
			tlsCfg.RootCAs = pool
		}
		creds = credentials.NewTLS(tlsCfg)
	}
	return grpc.NewClient(cfg.Endpoint.Address(), grpc.WithTransportCredentials(creds))
}

// GRPCClient is a QueryClient backed by the query service over gRPC.
type GRPCClient struct {
	conn        *grpc.ClientConn
	cl          committerpb.QueryServiceClient
	viewTimeout time.Duration
}

// NewGRPCClient wraps an existing connection. viewTimeout is passed to BeginView
// (0 lets the server pick its maximum).
func NewGRPCClient(conn *grpc.ClientConn, viewTimeout time.Duration) *GRPCClient {
	return &GRPCClient{conn: conn, cl: committerpb.NewQueryServiceClient(conn), viewTimeout: viewTimeout}
}

func (c *GRPCClient) BeginView(ctx context.Context) (string, error) {
	view, err := c.cl.BeginView(ctx, &committerpb.ViewParameters{
		IsoLevel:            committerpb.IsoLevel_SERIALIZABLE,
		TimeoutMilliseconds: uint64(c.viewTimeout.Milliseconds()),
	})
	if err != nil {
		return "", err
	}
	return view.GetId(), nil
}

func (c *GRPCClient) GetRows(ctx context.Context, viewID, ns string, keys [][]byte) ([]Row, error) {
	q := &committerpb.Query{
		View:       &committerpb.View{Id: viewID},
		Namespaces: []*committerpb.QueryNamespace{{NsId: ns, Keys: keys}},
	}
	res, err := c.cl.GetRows(ctx, q)
	if err != nil {
		return nil, err
	}
	var rows []Row
	for _, rns := range res.GetNamespaces() {
		if rns.GetNsId() != ns {
			continue
		}
		for _, r := range rns.GetRows() {
			rows = append(rows, Row{Key: r.GetKey(), Value: r.GetValue(), Version: r.GetVersion()})
		}
	}
	return rows, nil
}

func (c *GRPCClient) EndView(ctx context.Context, viewID string) error {
	_, err := c.cl.EndView(ctx, &committerpb.View{Id: viewID})
	return err
}

func (c *GRPCClient) Close() error { return c.conn.Close() }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/query/... -run TestGRPCClientRoundTrip -count=1 -v`
Expected: PASS.

> If `committerpb.IsoLevel_SERIALIZABLE` name differs, confirm the enum constant name in `fabric-x-common/api/committerpb/query.pb.go` (the proto value is `SERIALIZABLE`).

- [ ] **Step 5: Commit**

```bash
git add endorser/query/grpc_client.go endorser/query/grpc_client_test.go
git commit -m "feat(endorser): add gRPC QueryClient over committerpb.QueryService"
```

---

## Task 4: two-phase `BatchExecutor`

**Files:**
- Modify: `endorser/execution/executor.go` (extract a reader-injecting run primitive)
- Create: `endorser/execution/batch_executor.go`
- Test: `endorser/execution/batch_executor_test.go`

**Interfaces:**
- Consumes: `KVSSnapshotter`, `ReadStore`, `NewStateDB`, `NewExecutor`, `Executor.Send`, `ExtendedStateDB` (existing in `endorser/execution`); `endorsement.ExecutionResult` (`fabric-x-sdk/endorsement`); `blocks.ReadWriteSet` (`fabric-x-sdk/blocks`).
- Produces:
  - `func (e *EVMEngine) ExecuteBatch(ctx context.Context, txs []*types.Transaction) ([]endorsement.ExecutionResult, error)` — N==1 single pass; N>1 warm(parallel)+authoritative(sequential over an in-memory overlay).
  - Unexported helper `overlayReader` implementing `ReadStore` (warm cache + write overlay).

**Design notes for the implementer:**
- The existing `EVMEngine.Execute` already performs the N==1 case (one view, on-demand reads). `ExecuteBatch([tx])` MUST return exactly `Execute`'s result. Implement `Execute` in terms of `ExecuteBatch` (or share the inner "run one tx against a StateDB" code) to keep them identical — DRY.
- For N>1: open one snapshot (one view) for the whole batch. Warm pass — run each tx against that snapshot concurrently, discarding results, so the view cache fills. Authoritative pass — wrap the snapshot in an `overlayReader` whose overlay starts empty; run txs in order, and after each, apply its write-set (`res.RWS.Writes`) into the overlay so the next tx observes it. Collect each tx's `ExecutionResult`.
- The write overlay maps `key -> *blocks.WriteRecord` (Value/IsDelete) and, on `Get`, returns the overlaid value when present (with the version carried from the warm read so MVCC read-versions stay view-consistent), else delegates to the underlying snapshot.

- [ ] **Step 1: Write the failing test** — `endorser/execution/batch_executor_test.go`

Use the package's existing in-memory read backing (see `testkvs_test.go` / `state_test.go` for the established pattern) to seed two accounts, then assert an ordered 2-tx batch where tx2 depends on tx1's write produces a merged, serial result. Skeleton:

```go
package execution_test

// TestExecuteBatchSequentialDependency seeds accounts A and B, then runs an
// ordered batch [A->B transfer, B->C transfer] where the second spends what the
// first delivered. It asserts both succeed and B's final balance reflects tx1
// applied before tx2 (i.e. the authoritative pass saw tx1's write).
func TestExecuteBatchSequentialDependency(t *testing.T) {
	// ... build EVMEngine over the in-memory test KVS used elsewhere in this
	// package (mirror executor_test.go setup) ...
	// results, err := engine.ExecuteBatch(context.Background(), []*types.Transaction{tx1, tx2})
	// require both results OK; assert B balance == initialB + amount1 - amount2.
}

// TestExecuteBatchSingleMatchesExecute asserts ExecuteBatch([tx]) returns the
// same RWS as Execute(tx) for one transaction.
func TestExecuteBatchSingleMatchesExecute(t *testing.T) { /* ... */ }
```

Fill the skeleton using the exact constructor and tx-building helpers already present in `endorser/execution/executor_test.go` (reuse them; do not invent new helpers).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/execution/... -run TestExecuteBatch -count=1 -v`
Expected: FAIL — `engine.ExecuteBatch` undefined.

- [ ] **Step 3: Extract the run primitive in `executor.go`**

Add a method that runs one tx against a supplied `ExtendedStateDB` and returns an `ExecutionResult`, factoring the body currently inside `Execute` (lines 78–105) so both `Execute` and the batch path share it:

```go
// runOn executes tx against the given state (already constructed over a reader)
// and returns the endorsement result. It contains the revert/logs/success
// classification previously inlined in Execute.
func (e *EVMEngine) runOn(state ExtendedStateDB, tx *types.Transaction) (endorsement.ExecutionResult, error) {
	ex, err := NewExecutor(state, noopCloser{}, nil, e.evmConfig)
	if err != nil {
		return endorsement.ExecutionResult{}, err
	}
	ret, err := ex.Send(tx)
	if err != nil {
		if !errors.Is(err, vm.ErrExecutionReverted) {
			return endorsement.ExecutionResult{}, err
		}
		event, mErr := fxcommon.MarshalRevert(ret, "", tx.Hash().Hex())
		if mErr != nil {
			return endorsement.ExecutionResult{}, fmt.Errorf("marshal revert event: %w", mErr)
		}
		return endorsement.ExecutionResult{RWS: state.Result(), Event: event, Status: 201, Message: err.Error(), Payload: ret}, nil
	}
	var logs []byte
	if l := state.Logs(); len(l) > 0 {
		if logs, err = json.Marshal(l); err != nil {
			return endorsement.ExecutionResult{}, fmt.Errorf("marshal logs: %w", err)
		}
	}
	return endorsement.Success(state.Result(), logs, ret), nil
}

// noopCloser is a ReadStore-less closer for states whose reader lifecycle is
// managed by the batch executor.
type noopCloser struct{}

func (noopCloser) Get(string, string) (*blocks.WriteRecord, error) { return nil, nil }
func (noopCloser) Close() error                                    { return nil }
```

Then rewrite `Execute` to build the StateDB over its own snapshot and call `runOn` (keeping the existing `newExecutor(nil)` reader lifecycle). Keep behavior identical.

> Confirm `blocks` is imported in `executor.go` (add `"github.com/hyperledger/fabric-x-sdk/blocks"` if not). Confirm `ExtendedStateDB` exposes `Result()`, `Logs()`, `Snapshot()`, `RevertToSnapshot()` (it does — see `statedb.go`).

- [ ] **Step 4: Write `endorser/execution/batch_executor.go`**

```go
/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// ExecuteBatch runs an ordered batch of transactions under a single view.
//
//   - len(txs)==1: one pass — identical to Execute.
//   - len(txs)>1: a warm pass runs each tx concurrently against the shared view
//     to populate its read cache, then an authoritative pass re-runs them in
//     order against the warm cache plus an in-memory write overlay, so a later
//     tx observes earlier writes.
func (e *EVMEngine) ExecuteBatch(ctx context.Context, txs []*types.Transaction) ([]endorsement.ExecutionResult, error) {
	if len(txs) == 0 {
		return nil, nil
	}
	reader, err := e.kvs.NewSnapshot(0)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if len(txs) == 1 {
		state, err := e.newState(reader)
		if err != nil {
			return nil, err
		}
		res, err := e.runOn(state, txs[0])
		if err != nil {
			return nil, err
		}
		return []endorsement.ExecutionResult{res}, nil
	}

	// Warm pass: fill the view cache concurrently. Results are discarded.
	var wg sync.WaitGroup
	for _, tx := range txs {
		wg.Add(1)
		go func(tx *types.Transaction) {
			defer wg.Done()
			if s, err := e.newState(reader); err == nil {
				_, _ = e.runOn(s, tx) // warm only; ignore result/error
			}
		}(tx)
	}
	wg.Wait()

	// Authoritative pass: sequential over cache + overlay.
	overlay := &overlayReader{under: reader, writes: map[string]*blocks.WriteRecord{}}
	out := make([]endorsement.ExecutionResult, 0, len(txs))
	for _, tx := range txs {
		state, err := e.newState(overlay)
		if err != nil {
			return nil, err
		}
		res, err := e.runOn(state, tx)
		if err != nil {
			return nil, err
		}
		overlay.apply(res.RWS)
		out = append(out, res)
	}
	return out, nil
}

// newState builds an ExtendedStateDB over reader (mirrors newExecutor's stateDB
// construction, without opening a snapshot).
func (e *EVMEngine) newState(reader ReadStore) (ExtendedStateDB, error) {
	stateDB, err := NewStateDB(context.TODO(), reader, e.namespace, 0, e.monotonicVersions)
	if err != nil {
		return nil, err
	}
	if e.evmConfig.DebugLogs {
		return NewStateDBLogger(stateDB), nil
	}
	return stateDB, nil
}

// overlayReader layers an in-memory write set over an underlying ReadStore so
// the authoritative pass sees earlier transactions' writes.
type overlayReader struct {
	under  ReadStore
	mu     sync.Mutex
	writes map[string]*blocks.WriteRecord
}

func (o *overlayReader) Get(ns, key string) (*blocks.WriteRecord, error) {
	o.mu.Lock()
	if rec, ok := o.writes[key]; ok {
		o.mu.Unlock()
		if rec != nil && rec.IsDelete {
			return nil, nil
		}
		return rec, nil
	}
	o.mu.Unlock()
	return o.under.Get(ns, key)
}

func (o *overlayReader) Close() error { return nil } // underlying reader closed by caller

func (o *overlayReader) apply(rws blocks.ReadWriteSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, w := range rws.Writes {
		o.writes[w.Key] = &blocks.WriteRecord{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete}
	}
}
```

> Confirm `blocks.ReadWriteSet` field names (`Writes []KVWrite` with `KVWrite{Key,IsDelete,Value}`) — verified in `fabric-x-sdk/blocks/types.go`. Confirm `NewStateDBLogger` signature in `statedb_logger.go`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./endorser/execution/... -run TestExecuteBatch -count=1 -v`
Expected: PASS. Then run the whole package: `go test ./endorser/execution/... -count=1` — Expected: PASS (Execute unchanged).

- [ ] **Step 6: Commit**

```bash
git add endorser/execution/executor.go endorser/execution/batch_executor.go endorser/execution/batch_executor_test.go
git commit -m "feat(endorser): add two-phase BatchExecutor (warm + authoritative)"
```

---

## Task 5: endorser config for the query service

**Files:**
- Modify: `endorser/config/config.go`
- Test: `endorser/config/config_test.go` (existing)

**Interfaces:**
- Produces: `Endorser.QueryService common.ClientConfig`, `Endorser.ViewTimeout time.Duration`; `DB.Database` accepts `"query-service"` | `"memory"`.

- [ ] **Step 1: Update the failing test first** — add cases to `endorser/config/config_test.go`

Change `validEndorser` to use the new default and add validation cases:

```go
// in validEndorser: replace the Database line and add QueryService
Database: config.DB{Database: "query-service"},
QueryService: common.ClientConfig{Endpoint: &common.Endpoint{Host: "127.0.0.1", Port: 7001}},
```

Add to the `tests` table:

```go
{"memory db ok", func(e *config.Endorser) { e.Database.Database = "memory" }, ""},
{"unknown db type", func(e *config.Endorser) { e.Database.Database = "postgres" }, "database.database"},
{"query-service needs endpoint", func(e *config.Endorser) { e.QueryService.Endpoint = nil }, "query-service"},
```

Also remove the now-invalid `"valid with database path"` and sqlite cases.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./endorser/config/... -count=1 -v`
Expected: FAIL — `QueryService` field undefined and new validation not implemented.

- [ ] **Step 3: Edit `endorser/config/config.go`**

Add the field and rewrite validation:

```go
// Endorser struct: add
QueryService common.ClientConfig `mapstructure:"query-service" yaml:"query-service"`
ViewTimeout  time.Duration       `mapstructure:"view-timeout"  yaml:"view-timeout"`
```

```go
// Validate: replace the database block with
switch cfg.Database.Database {
case "query-service":
	if err := cfg.QueryService.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("query-service: %w", err))
	}
case "memory":
	// no extra requirements
case "":
	errs = append(errs, errors.New("database.database is required"))
default:
	errs = append(errs, fmt.Errorf("database.database: unknown type %q (want query-service or memory)", cfg.Database.Database))
}
```

Add `"time"` to imports. Keep the `DB` struct (`HistorySize` still used by the memory path; `ConnString` retained but unused by the new types).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./endorser/config/... -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add endorser/config/config.go endorser/config/config_test.go
git commit -m "feat(endorser): configure query-service backend; drop sqlite db option"
```

---

## Task 6: factory builds the query store; drop the endorser synchronizer

**Files:**
- Modify: `endorser/app/factory.go`

**Interfaces:**
- Consumes: `query.NewStore`, `query.NewMemClient`, `query.NewGRPCClient`, `query.Dial` (Tasks 1–3); `config.Endorser` (Task 5); `storage.NewRevertibleLightKVS`, `storage.NewLightKVS`.
- Produces:
  - `func NewEndorserCore(cfg config.Endorser, channel, namespace, protocol string, signer sdk.Signer, evmConfig execution.EVMConfig, testImpl bool) (*core.Endorser, execution.KVSSnapshotter, *storage.RevertibleLightKVS, endorsement.Builder, error)` — returns the read store, the revertible backing store (nil for `query-service`), and the builder.
  - `func NewEndorser(cfg config.Endorser, network common.Network, signer sdk.Signer, logger sdk.Logger, testImpl bool) (*core.Endorser, *storage.RevertibleLightKVS, error)` — **no synchronizer** in the return.

**Design notes:**
- `query-service` mode: `conn, _ := query.Dial(cfg.QueryService)`; `store := query.NewStore(query.NewGRPCClient(conn, cfg.ViewTimeout), namespace)`; no revertible backing (return nil).
- `memory` mode: `back := storage.NewRevertibleLightKVS(storage.NewLightKVS(cfg.Database.HistorySize))`; `store := query.NewStore(query.NewMemClient(back), namespace)`; return `back` so the caller can register it as a block handler and use revert.
- `NewEVMEngine(namespace, store, evmConfig, monotonicVersions)` — `store` is a `KVSSnapshotter`, unchanged.
- `NewEndorser` no longer constructs `nfabx.NewSynchronizer`; delete that block and the SDK network imports it used. The gateway/harness now owns all block delivery (for `memory` mode) or the query service serves reads directly (for `query-service` mode).

- [ ] **Step 1: Rewrite `NewEndorserCore`** to the new signature (see Design notes) — replace the `switch dbCfg.Database` block that built `VersionedDBWrapper`/`LightKVS` with the query-store construction above; keep the `builder`/`monotonicVersions` block; pass `store` to `NewEVMEngine`.

- [ ] **Step 2: Rewrite `NewEndorser`** to call the new `NewEndorserCore`, drop the synchronizer creation, and return `(end, back, err)`.

- [ ] **Step 3: Build to find all callers**

Run: `go build ./... 2>&1 | head -40`
Expected: compile errors at `gateway/app/app.go` and `integration/test_helpers.go` (fixed in Task 7). The `endorser/app` package itself must compile in isolation:
Run: `go build ./endorser/...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add endorser/app/factory.go
git commit -m "refactor(endorser): build query-service store in factory; remove endorser synchronizer"
```

---

## Task 7: rewire gateway app + test harness onto the query store

**Files:**
- Modify: `gateway/app/app.go`
- Modify: `integration/test_helpers.go`

**Design notes:**
- `gateway/app/app.go` `newApp`/`buildApp`:
  - Drop `endorserSyncs` entirely (the slice, `append`, and the `buildApp` parameter). Endorsers created by `eapp.NewEndorser` now return `(end, back, err)`.
  - `enableTestRPC` implies the memory path — assert `back != nil` and use it as the `lightKVS` (`estorage.Revertible`) for the test server; register `back` as a `blocks.BlockHandler` on `gwSync` so it stays current (the test RPC path needs live in-memory state). When `enableTestRPC` is false (production `query-service` mode), `back` is nil and no endorser block handler is registered.
  - Keep `gwSync`, `chain`, `gateway` handlers exactly as before.
- `integration/test_helpers.go`:
  - `NewEndorser` (test) → call the new `NewEndorserCore`; return the `*storage.RevertibleLightKVS` backing store as `EndorserComponents.KVS` (so the harness keeps registering it as a `blocks.BlockHandler`/`common.TxHandler` and updating it — line ~201 `handlers = append(handlers, db)` still works).
  - `defaultEndorserFactory` sets `ecfg.Database.Database = "memory"` for in-process harness endorsers unless the config already selects `query-service` (perf against the real service).
  - `EndorserComponents.KVS` type stays `storage.KVS`; `*storage.RevertibleLightKVS` already satisfies it.

- [ ] **Step 1: Edit `gateway/app/app.go`** per Design notes: remove `endorserSyncs`; change the endorser loop to `end, back, err := eapp.NewEndorser(...)`; set `firstKVS = back` (guard nil under `enableTestRPC`); drop `endorserSyncs` from the `buildApp` signature and its synchronizer handler list (endorsers are no longer synchronizer handlers in production); when `enableTestRPC`, append `back` to the `gwSync` handler list.

- [ ] **Step 2: Edit `integration/test_helpers.go`** per Design notes: update `NewEndorser` to the new `NewEndorserCore` signature and return the backing store; set `memory` mode in `defaultEndorserFactory`.

- [ ] **Step 3: Build the whole module**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 4: Run the fast in-process suites**

Run: `go test ./gateway/... ./endorser/... -count=1`
Expected: PASS. Investigate and fix any harness wiring fallout (e.g. a handler that must observe the `back` store) before moving on.

- [ ] **Step 5: Commit**

```bash
git add gateway/app/app.go integration/test_helpers.go
git commit -m "refactor(gateway): wire endorsers onto the query-service store; drop endorser syncs"
```

---

## Task 8: remove the VersionedDB / sqlite read path

**Files:**
- Delete: `endorser/storage/versioned_db_wrapper.go` (+ its `_test.go` if present)
- Modify: any remaining references

- [ ] **Step 1: Find references**

Run: `grep -rn "VersionedDBWrapper\|NewWriteDB\|VersionedDBSnapshot" --include=*.go . 2>/dev/null` (if fish rejects `--include`, use `grep -rn ... $(find . -name '*.go')`).
Expected: references only in the file to delete and possibly `endorser/app` (already rewritten in Task 6).

- [ ] **Step 2: Delete the file(s)**

```bash
git rm endorser/storage/versioned_db_wrapper.go
```
Delete `endorser/storage/versioned_db_wrapper_test.go` too if it exists.

- [ ] **Step 3: Build + vet**

Run: `go build ./... && go vet ./endorser/... ./gateway/...`
Expected: PASS. If `state.NewWriteDB`/SQLite imports are now unused anywhere, remove them.

- [ ] **Step 4: Commit**

```bash
git add -A endorser/storage
git commit -m "refactor(endorser): remove the synced VersionedDB read path"
```

---

## Task 9: configuration files for the query service

**Files:**
- Modify: `integration/fabx-full.yaml`, `integration/fabx.yaml` (and any gateway config that lists endorsers)

- [ ] **Step 1: Inspect current endorser config blocks**

Run: `grep -n "database\|endorsers\|committer\|endpoint" integration/fabx-full.yaml integration/fabx.yaml`
Expected: shows each endorser's `database:` and `committer:` blocks.

- [ ] **Step 2: For each endorser entry, set the query-service backend**

Set `database.database: query-service` and add a `query-service` client block pointing at the running service. For `fabx-full.yaml` (compose service `committer-query-service:7001`, mTLS), mirror the TLS paths already used by the endorser's `committer:` block:

```yaml
    database:
      database: query-service
    query-service:
      endpoint:
        host: committer-query-service
        port: 7001
      tls:
        mode: mtls
        cert-path: <same cert dir as committer block>/server.crt
        key-path:  <same cert dir as committer block>/server.key
        ca-cert-paths:
          - <same ca path as committer block>
    view-timeout: 5s
```

Use the exact cert/key/ca paths already present in that file's `committer:` TLS block (copy them). For `fabx.yaml` (lightweight/local), point `host`/`port` at that compose's query-service mapping and match its TLS mode.

- [ ] **Step 3: Validate config loads**

Run: `cd integration && ../bin/fxevm start -c fabx.yaml --help >/dev/null` (build first with `make build`), or a config-load unit test if one exists.
Expected: no config validation error about `database`/`query-service`.

- [ ] **Step 4: Commit**

```bash
git add integration/fabx-full.yaml integration/fabx.yaml
git commit -m "config: point endorsers at the committer query service"
```

---

## Task 10: documentation

**Files:**
- Modify: `docs/COMPATIBILITY.md`, `docs/ARCHITECTURE.md`

- [ ] **Step 1: `docs/COMPATIBILITY.md`** — add a note that historical-height state reads (`eth_getBalance`/`eth_getStorageAt`/`eth_call` at a past block number) resolve to the **latest committed** state, because the endorser reads through the query service which serves only the latest committed snapshot. Place it near the existing block-tags/`block.number`==0 notes for consistency.

- [ ] **Step 2: `docs/ARCHITECTURE.md`** — in the Endorser section and "Dual Synchronization Architecture", replace the description of the endorser's synced `VersionedDB` + Endorser Synchronizer with: the endorser reads committed state on demand from the Fabric-X query service under a pinned SERIALIZABLE view; it keeps no local world state and runs no state synchronizer. Note the gateway indexer synchronizer is unchanged.

- [ ] **Step 3: Commit**

```bash
git add docs/COMPATIBILITY.md docs/ARCHITECTURE.md
git commit -m "docs: endorser reads from the query service; note historical-read behavior"
```

---

## Task 11: end-to-end perf validation (acceptance)

**Files:** none (validation task). Uses `test.sh`, `setup.sh`.

- [ ] **Step 1: One-time dataset fetch (if not already present)**

Run: `./setup.sh`
Expected: `integration/perf/testdata/USDC_contract.json` and `USDC_dataset.json.gz` exist.

- [ ] **Step 2: Run the perf replay against the full network (real query service)**

Run: `./test.sh`
(This does `make clean-x init-x start-full`, runs `TestReplayJSONDataset` with `-gateway-config fabx-full.yaml`, then `make stop-full clean-x`.)
Expected: the test completes and reports throughput/latency; transactions commit through endorsers that read from `committer-query-service:7001` (no local endorser state DB, no endorser synchronizer).

- [ ] **Step 3: If `BeginView` errors with resource-exhausted (active-view limit)**

Raise `max-active-views` in `testdata/config/committer-query-service.yaml` (currently 4096) to comfortably exceed the perf `-outstanding` value, then re-run Step 2. Record the value used.

- [ ] **Step 4: Record results**

Capture throughput, latency, and goodput for both datasets (edit `test.sh` to point at the high-conflict `USDC_dataset.012020.json.gz` for the second run, per the commented line in `setup.sh`). Confirm conflict-free ≈ goodput and high-conflict is no worse than the previous synced-DB baseline. Note findings in the PR description.

- [ ] **Step 5: Commit any config changes**

```bash
git add testdata/config/committer-query-service.yaml
git commit -m "test: raise query-service active-view limit for perf replay"
```
(Skip if no change was needed.)

---

## Self-Review

- **Spec coverage:** RQ1 (Tasks 6–8), RQ2 (Tasks 2–3, verified by version mapping), RQ3 (Task 4), RQ4 (Tasks 6–7 keep `gwSync`/`Chain`), RQ5 (Task 1 mem client + Task 7 test-RPC revertible), RQ6 (Task 4 determinism test). Open items: historical reads (Task 10), config values (Task 5), active-views (Task 11 Step 3). All covered.
- **Type consistency:** `QueryClient`/`Row` used identically in Tasks 1–4; `NewEndorserCore`/`NewEndorser` new signatures defined in Task 6 and consumed in Task 7; `execution.KVSSnapshotter`/`ReadStore` are the existing ports; `blocks.WriteRecord.Version` (uint64) ↔ `Row.Version` (uint64) 1:1.
- **Verify-before-code flags** (do these inside the task, not as separate steps): `storage.KeyValueVersion` / `RevertibleLightKVS.Update` signature (Task 1); `committerpb.IsoLevel_SERIALIZABLE` constant name (Task 3); `NewStateDBLogger` signature and `blocks.ReadWriteSet.Writes` shape (Task 4); exact cert paths in `fabx-full.yaml` (Task 9).
