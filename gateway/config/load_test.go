/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-evm/gateway/config"
)

func TestLoadFabXSampleConfig(t *testing.T) {
	cfg, err := config.Load("../../integration/fabx.yaml")
	if err != nil {
		t.Fatalf("Load fabx.yaml: %v", err)
	}
	if cfg.Network.Protocol != "fabric-x" {
		t.Errorf("expected protocol fabric-x, got %q", cfg.Network.Protocol)
	}
	if cfg.Network.ChainID != 4011 {
		t.Errorf("expected chain-id 4011, got %d", cfg.Network.ChainID)
	}
	if len(cfg.Endorsers) == 0 {
		t.Error("expected at least one endorser")
	}
	if cfg.Gateway.Listen == "" {
		t.Error("expected gateway.listen to be set")
	}
}

func TestLoadFabricSamplesSampleConfig(t *testing.T) {
	cfg, err := config.Load("../../integration/fablo.yaml")
	if err != nil {
		t.Fatalf("Load fabric-samples.yaml: %v", err)
	}
	if cfg.Network.Protocol != "fabric" {
		t.Errorf("expected protocol fabric, got %q", cfg.Network.Protocol)
	}
	if len(cfg.Endorsers) != 2 {
		t.Errorf("expected 2 endorsers, got %d", len(cfg.Endorsers))
	}
}

func TestLoadNotifyTimeout(t *testing.T) {
	yaml := `
network:
  channel: mychannel
  namespace: basic

gateway:
  listen: "0.0.0.0:8545"
  notify-timeout: 30s
`
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Gateway.NotifyTimeout != 30*time.Second {
		t.Errorf("expected notify-timeout 30s, got %s", cfg.Gateway.NotifyTimeout)
	}
}
