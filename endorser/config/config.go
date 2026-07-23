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
)

// Endorser contains configuration for a single embedded endorser peer.
type Endorser struct {
	Name         string                `mapstructure:"name"          yaml:"name"`
	Identity     common.IdentityConfig `mapstructure:"identity"      yaml:"identity"`
	Committer    common.ClientConfig   `mapstructure:"committer"     yaml:"committer"`
	Database     DB                    `mapstructure:"database"      yaml:"database"`
	QueryService common.ClientConfig   `mapstructure:"query-service" yaml:"query-service"`
	ViewTimeout  time.Duration         `mapstructure:"view-timeout"  yaml:"view-timeout"`
	// DebugLogs enables per-tx StateDB DEBUG logging via StateDBLogger.
	DebugLogs bool `mapstructure:"debug-logs" yaml:"debug-logs"`
}

// DB holds the database path for an endorser.
type DB struct {
	Database    string `mapstructure:"database" yaml:"database"`
	ConnString  string `mapstructure:"connection-string" yaml:"connection-string"`
	HistorySize int    `mapstructure:"history_size" yaml:"history_size"` // number of historical snapshots to keep (default: 2, use 128 for test RPC)
}

// Validate checks that required fields are set and values are within acceptable ranges.
func (cfg Endorser) Validate() error {
	var errs []error

	if cfg.Name == "" {
		errs = append(errs, errors.New("name is required"))
	}
	if err := cfg.Identity.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("identity: %w", err))
	}
	if err := cfg.Committer.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("committer: %w", err))
	}
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

	return errors.Join(errs...)
}
