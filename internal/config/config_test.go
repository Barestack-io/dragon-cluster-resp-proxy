package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadYAMLThenEnvWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	body := []byte(`
unix_socket: /tmp/from-yaml.sock
cluster_seeds:
  - 10.0.0.1:6379
pool_max_per_node: 16
log_level: debug
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("DCRP_UNIX_SOCKET", "/tmp/from-env.sock")
	t.Setenv("DCRP_CLUSTER_SEEDS", "10.0.0.8:7000,10.0.0.9:7000")
	t.Setenv("DCRP_POOL_MAX_PER_NODE", "128")
	t.Setenv("DCRP_DIAL_TIMEOUT", "1s")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UnixSocket != "/tmp/from-env.sock" {
		t.Fatalf("unix socket: %s", cfg.UnixSocket)
	}
	if len(cfg.ClusterSeeds) != 2 || cfg.ClusterSeeds[1] != "10.0.0.9:7000" {
		t.Fatalf("seeds: %#v", cfg.ClusterSeeds)
	}
	if cfg.PoolMaxPerNode != 128 {
		t.Fatalf("pool max: %d", cfg.PoolMaxPerNode)
	}
	if cfg.DialTimeout != time.Second {
		t.Fatalf("dial timeout: %s", cfg.DialTimeout)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("log level should remain from yaml: %s", cfg.LogLevel)
	}
}

func TestAddressMapEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("cluster_seeds:\n  - 127.0.0.1:6379\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DCRP_ADDRESS_MAP", "192.168.0.131:6383=127.0.0.1:16383,192.168.0.18:6383=127.0.0.1:26383")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AddressMap["192.168.0.131:6383"] != "127.0.0.1:16383" {
		t.Fatalf("%#v", cfg.AddressMap)
	}
}

func TestMaxDatabasesEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("cluster_seeds:\n  - 127.0.0.1:6379\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DCRP_MAX_DATABASES", "32")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxDatabases != 32 {
		t.Fatalf("max databases: %d", cfg.MaxDatabases)
	}
}

func TestValidateRequiresSeeds(t *testing.T) {
	cfg := Defaults()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error without seeds")
	}
}
