/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
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

// GRPCClient is a QueryClient backed by the query service over gRPC. It may hold
// more than one connection: the endorser's warm pass fires a whole batch's state
// reads concurrently, and a single *grpc.ClientConn serializes them behind one
// HTTP/2 transport (one writer goroutine, ~100 concurrent-stream cap). Holding a
// pool of connections and round-robining GetRows across them lets the concurrent
// readers use independent transports. BeginView/EndView are pinned to the first
// connection; the view is a server-side handle (keyed by id, not by connection),
// so GetRows for that view may be issued over any connection in the pool.
type GRPCClient struct {
	conns       []*grpc.ClientConn
	cls         []committerpb.QueryServiceClient
	next        atomic.Uint32
	viewTimeout time.Duration
}

var _ QueryClient = (*GRPCClient)(nil)

// NewGRPCClient wraps a single connection. viewTimeout is passed to BeginView
// (0 lets the server pick its maximum). Equivalent to NewGRPCClientPool with one
// connection.
func NewGRPCClient(conn *grpc.ClientConn, viewTimeout time.Duration) *GRPCClient {
	return NewGRPCClientPool([]*grpc.ClientConn{conn}, viewTimeout)
}

// NewGRPCClientPool wraps one or more connections to the same query service.
// GetRows round-robins across them; BeginView/EndView/Close operate on the pool.
// conns must be non-empty.
func NewGRPCClientPool(conns []*grpc.ClientConn, viewTimeout time.Duration) *GRPCClient {
	cls := make([]committerpb.QueryServiceClient, len(conns))
	for i, conn := range conns {
		cls[i] = committerpb.NewQueryServiceClient(conn)
	}
	return &GRPCClient{conns: conns, cls: cls, viewTimeout: viewTimeout}
}

// pick returns the next client in round-robin order. len(cls) is always >= 1.
func (c *GRPCClient) pick() committerpb.QueryServiceClient {
	if len(c.cls) == 1 {
		return c.cls[0]
	}
	i := c.next.Add(1) - 1
	return c.cls[int(i%uint32(len(c.cls)))]
}

func (c *GRPCClient) BeginView(ctx context.Context) (string, error) {
	if c.viewTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.viewTimeout)
		defer cancel()
	}
	view, err := c.cls[0].BeginView(ctx, &committerpb.ViewParameters{
		IsoLevel:            committerpb.IsoLevel_SERIALIZABLE,
		TimeoutMilliseconds: uint64(c.viewTimeout.Milliseconds()),
	})
	if err != nil {
		return "", err
	}
	return view.GetId(), nil
}

func (c *GRPCClient) GetRows(ctx context.Context, viewID, ns string, keys [][]byte) ([]Row, error) {
	if c.viewTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.viewTimeout)
		defer cancel()
	}
	// An empty viewID selects the query service's non-consistent (nil-view) read
	// path: each GetRows reads CURRENT committed state on a fresh pooled connection
	// (sharedPool) instead of a snapshot pinned by BeginView and shared across the
	// view-aggregation window (sharedLazyTx). The aggregation window makes a freshly
	// begun view observe committed state as of up to ViewAggregationWindow ago, so a
	// just-committed hot key reads back at its pre-commit version -- the stale read
	// that drove the pipelined-auth MVCC abort cascade. Reading current committed
	// state removes that skew at the source; cross-read snapshot consistency is
	// instead provided by the endorser's read-cache (View) + write-cache
	// (VersionedCache) layers. See Store.nilView.
	var view *committerpb.View
	if viewID != "" {
		view = &committerpb.View{Id: viewID}
	}
	q := &committerpb.Query{
		View:       view,
		Namespaces: []*committerpb.QueryNamespace{{NsId: ns, Keys: keys}},
	}
	res, err := c.pick().GetRows(ctx, q)
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
	if viewID == "" {
		return nil // nil-view mode never began a server-side view
	}
	if c.viewTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.viewTimeout)
		defer cancel()
	}
	_, err := c.cls[0].EndView(ctx, &committerpb.View{Id: viewID})
	return err
}

// Close closes every connection in the pool, joining any errors.
func (c *GRPCClient) Close() error {
	var errs []error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
