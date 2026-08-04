/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-x-evm/common"
	endorser "github.com/hyperledger/fabric-x-evm/endorser/config"
)

// Config is the top-level configuration for the combined (embedded-endorsers) deployment.
type Config struct {
	Logging   Logging             `mapstructure:"logging"   yaml:"logging"`
	Network   common.Network      `mapstructure:"network"   yaml:"network"`
	Gateway   Gateway             `mapstructure:"gateway"   yaml:"gateway"`
	Endorsers []endorser.Endorser `mapstructure:"endorsers" yaml:"endorsers"`
}

// Logging is the config for the Fabric Logger
type Logging struct {
	// Format is the log record format specifier for the Logging instance. If the
	// spec is the string "json", log records will be formatted as JSON. Any
	// other string will be provided to the FormatEncoder. Please see
	// fabenc.ParseFormat for details on the supported verbs.
	//
	// If Format is not provided, a default format that provides basic information will
	// be used.
	Format string `mapstructure:"format" yaml:"format"`

	// Spec determines the log levels that are enabled for the logging system. The
	// spec must be in a format that can be processed by ActivateSpec.
	//
	// If Spec is not provided, loggers will be enabled at the INFO level.
	Spec string `mapstructure:"spec" yaml:"spec"`
}

// Gateway contains configuration for the gateway component.
type Gateway struct {
	Listen string `mapstructure:"listen" yaml:"listen"` // HTTP listen address for the Ethereum JSON-RPC API

	Identity common.IdentityConfig `mapstructure:"identity" yaml:"identity"`

	Database DB `mapstructure:"database" yaml:"database"`

	Orderers  []common.ClientConfig `mapstructure:"orderers"  yaml:"orderers"`
	Committer common.ClientConfig   `mapstructure:"committer" yaml:"committer"`

	SyncTimeout time.Duration `mapstructure:"sync-timeout" yaml:"sync-timeout"`

	SubmitterCount      int `mapstructure:"submitter-count"       yaml:"submitter-count"`       // number of batch submitter worker goroutines; defaults to 16 if not set. On the production pipelined gateway path (gateway/app.buildApp) orderer submission is always serialized to a single worker regardless of this value (clamped with a warning if > 1), because the pipelined executor requires committer txs to reach the orderer in submission order; this field is otherwise retained for non-production / independent-workload submitters (see core.NewBatchSubmitter, core.BuildGateway).
	EndorsementChanSize int `mapstructure:"endorsement-chan-size" yaml:"endorsement-chan-size"` // capacity of the endorsement channel; defaults to 1000 if not set
	MaxBatchSize        int `mapstructure:"max-batch-size"        yaml:"max-batch-size"`        // max EVM txs folded into one merged committer tx per drain cycle; 0 = unbounded drain-all (see core.Gateway.SetMaxBatchSize)
	MaxInflight         int `mapstructure:"max-inflight"          yaml:"max-inflight"`          // max submitted-but-unconfirmed committer txs the pipelined executor keeps in flight; <=0 defaults to 16 (see core.Gateway.SetMaxInflight)

	NotifyTimeout time.Duration `mapstructure:"notify-timeout" yaml:"notify-timeout"` // client-side backstop before the pipelined executor resolves an in-flight batch by fallback/rollback if no commit/abort notification arrives; <=0 defaults to 60s (see core.Gateway.SetCommitTimeout)

	Pipelined bool `mapstructure:"pipelined" yaml:"pipelined"` // overlap the concurrent warm pass of batch N+1 with the serial authoritative pass of batch N; default false (serial). Auth reopens onto a FRESH committed view carrying no stale read cache, so authoritative reads resolve against the live in-flight write cache, the write-eviction-safe read-only cache, and current committed state -- never a pinned-stale version. Identical in effect to serial at ANY prefetch depth: with no external (non-EVM) traffic the stale-read MVCC abort cascade cannot occur and the pipeline never livelocks, at any batch size. Default stays false; throughput is being re-measured on ec2 after this read-path fix. See core.Gateway.SetPipelined and scripts/report/pipeline_report.html.

	OrderedSubmit   bool                  `mapstructure:"ordered-submit"   yaml:"ordered-submit"`   // gate committer-tx k+1 until k appears in an ordered block delivered from the assembler (default false). Requires pipelined + a single serialized submitter. Enforces submission order == total order == commit order, eliminating the reorder-driven MVCC-abort livelock at every batch size; may throttle throughput below serial at very small batch sizes (depth-1 block-cut latency). See core.OrderGate.
	OrderedDelivery OrderedDeliveryConfig `mapstructure:"ordered-delivery" yaml:"ordered-delivery"` // NoFT ordered-block delivery stream config; only used when ordered-submit is true.
}

// OrderedDeliveryConfig configures the NoFT ordered-block delivery stream used
// by ordered-submit. TLS and MSP identity are reused from gateway.orderers and
// gateway.identity; only the config-block path (orderer endpoints + channel ID)
// is delivery-specific.
type OrderedDeliveryConfig struct {
	ConfigBlockPath string        `mapstructure:"config-block-path" yaml:"config-block-path"`
	NextBlockNum    uint64        `mapstructure:"next-block-num"    yaml:"next-block-num"` // seek start; 0 = from genesis (safe: replays harmlessly, old TxIDs never match a future arm)
	WaitTimeout     time.Duration `mapstructure:"wait-timeout"      yaml:"wait-timeout"`   // per-batch max wait for the ordered signal before proceeding unordered; <=0 defaults to 5s
}

// DB holds the database paths for the gateway.
type DB struct {
	ConnString string `mapstructure:"connection-string" yaml:"connection-string"` // SQLite connection string for blocks, transactions, and logs
	TriePath   string `mapstructure:"trie-path"         yaml:"trie-path"`         // PebbleDB directory for state root trie; empty = in-memory
}

// Validate checks that required fields are set and values are within acceptable ranges.
func (cfg Config) Validate() error {
	var errs []error

	if cfg.Network.Channel == "" {
		errs = append(errs, errors.New("network.channel is required"))
	}
	if cfg.Network.Namespace == "" {
		errs = append(errs, errors.New("network.namespace is required"))
	}
	if p := cfg.Network.Protocol; p != "" && p != "fabric" && p != "fabric-x" {
		errs = append(errs, errors.New("network.protocol must be 'fabric' or 'fabric-x'"))
	}
	if cfg.Gateway.Listen == "" {
		errs = append(errs, errors.New("gateway.listen is required"))
	} else if err := common.ValidateListenAddress(cfg.Gateway.Listen); err != nil {
		errs = append(errs, err)
	}
	if err := cfg.Gateway.Identity.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("gateway.identity: %w", err))
	}
	if cfg.Gateway.Database.ConnString == "" {
		errs = append(errs, errors.New("gateway.database.connection-string is required"))
	}
	if err := cfg.Gateway.Committer.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("gateway.committer: %w", err))
	}
	if len(cfg.Gateway.Orderers) == 0 {
		errs = append(errs, errors.New("gateway.orderers must have at least one entry"))
	}
	for i, o := range cfg.Gateway.Orderers {
		if err := o.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("gateway.orderers[%d]: %w", i, err))
		}
	}
	if cfg.Gateway.OrderedSubmit {
		if !cfg.Gateway.Pipelined {
			errs = append(errs, errors.New("gateway.ordered-submit requires gateway.pipelined"))
		}
		if cfg.Gateway.OrderedDelivery.ConfigBlockPath == "" {
			errs = append(errs, errors.New("gateway.ordered-delivery.config-block-path is required when ordered-submit is enabled"))
		}
	}
	if len(cfg.Endorsers) == 0 {
		errs = append(errs, errors.New("endorsers must have at least one entry"))
	}
	for i := range cfg.Endorsers {
		errs = append(errs, cfg.Endorsers[i].Validate())
	}

	return errors.Join(errs...)
}
