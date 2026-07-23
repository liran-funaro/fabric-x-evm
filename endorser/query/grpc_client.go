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

var _ QueryClient = (*GRPCClient)(nil)

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
