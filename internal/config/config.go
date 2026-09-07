package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "DCRP_"

// Config is the full runtime configuration. Environment variables win over YAML.
type Config struct {
	UnixSocket          string        `yaml:"unix_socket"`
	UnixSocketMode      string        `yaml:"unix_socket_mode"`
	ClusterSeeds        []string      `yaml:"cluster_seeds"`
	ClusterUsername     string        `yaml:"cluster_username"`
	ClusterPassword     string        `yaml:"cluster_password"`
	TLS                 TLSConfig     `yaml:"tls"`
	BackendFlavor       string        `yaml:"backend_flavor"`
	PoolMinPerNode      int           `yaml:"pool_min_per_node"`
	PoolMaxPerNode      int           `yaml:"pool_max_per_node"`
	DialTimeout         time.Duration `yaml:"dial_timeout"`
	ReadTimeout         time.Duration `yaml:"read_timeout"`
	WriteTimeout        time.Duration `yaml:"write_timeout"`
	ReconnectBackoffMin time.Duration `yaml:"reconnect_backoff_min"`
	ReconnectBackoffMax time.Duration `yaml:"reconnect_backoff_max"`
	TopologyRefresh     time.Duration `yaml:"topology_refresh"`
	MaxClientConns      int           `yaml:"max_client_conns"`
	BlockingMaxWait     time.Duration `yaml:"blocking_max_wait"`
	MetricsAddr         string        `yaml:"metrics_addr"`
	LogLevel            string        `yaml:"log_level"`
	Pprof               bool          `yaml:"pprof"`
	PprofAddr           string        `yaml:"pprof_addr"`
	ShutdownTimeout     time.Duration `yaml:"shutdown_timeout"`
	ReadFromReplicas    bool          `yaml:"read_from_replicas"`
	ClientAuthPassword  string        `yaml:"client_auth_password"`
	HashTagOpen         string        `yaml:"hash_tag_open"`
	HashTagClose        string        `yaml:"hash_tag_close"`
	MovedHopLimit       int           `yaml:"moved_hop_limit"`
	// AddressMap rewrites advertised cluster endpoints (e.g. private IPs)
	// to locally reachable addresses. Used for SSH tunnels.
	AddressMap map[string]string `yaml:"address_map"`
	// MaxDatabases is the number of virtual DBs SELECT may use (indexes 0..N-1).
	// DB 0 is unprefixed; DB N>0 stores keys as dbN:<key>.
	MaxDatabases int `yaml:"max_databases"`
	// MaxPipeline is the max number of client commands flushed to backends
	// as one write/read burst per connection.
	MaxPipeline int `yaml:"max_pipeline"`
}

// TLSConfig holds optional backend TLS settings.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// Defaults returns a Config with production-oriented defaults.
func Defaults() Config {
	return Config{
		UnixSocket:          "/tmp/dragon-cluster-resp-proxy.sock",
		UnixSocketMode:      "0770",
		BackendFlavor:       "auto",
		PoolMinPerNode:      2,
		PoolMaxPerNode:      64,
		DialTimeout:         3 * time.Second,
		ReadTimeout:         30 * time.Second,
		WriteTimeout:        5 * time.Second,
		ReconnectBackoffMin: 50 * time.Millisecond,
		ReconnectBackoffMax: 2 * time.Second,
		TopologyRefresh:     10 * time.Second,
		MaxClientConns:      10000,
		BlockingMaxWait:     60 * time.Second,
		MetricsAddr:         ":9090",
		LogLevel:            "info",
		Pprof:               false,
		PprofAddr:           "127.0.0.1:6060",
		ShutdownTimeout:     15 * time.Second,
		ReadFromReplicas:    false,
		HashTagOpen:         "{",
		HashTagClose:        "}",
		MovedHopLimit:       5,
		MaxDatabases:        16,
		MaxPipeline:         128,
	}
}

// UnixSocketPerm parses UnixSocketMode as an octal file mode.
func (c Config) UnixSocketPerm() (os.FileMode, error) {
	v, err := strconv.ParseUint(c.UnixSocketMode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("unix_socket_mode: %w", err)
	}
	return os.FileMode(v), nil
}

// HashTagRunes returns the open/close hash-tag delimiters.
func (c Config) HashTagRunes() (open, close byte) {
	open, close = '{', '}'
	if len(c.HashTagOpen) > 0 {
		open = c.HashTagOpen[0]
	}
	if len(c.HashTagClose) > 0 {
		close = c.HashTagClose[0]
	}
	return open, close
}

// Validate checks invariants after YAML+env merge.
func (c Config) Validate() error {
	if c.UnixSocket == "" {
		return errors.New("unix_socket is required")
	}
	if len(c.ClusterSeeds) == 0 {
		return errors.New("cluster_seeds is required")
	}
	switch strings.ToLower(c.BackendFlavor) {
	case "auto", "dragonfly", "redis":
	default:
		return fmt.Errorf("backend_flavor %q must be auto, dragonfly, or redis", c.BackendFlavor)
	}
	if c.PoolMaxPerNode < 1 {
		return errors.New("pool_max_per_node must be >= 1")
	}
	if c.PoolMinPerNode < 0 || c.PoolMinPerNode > c.PoolMaxPerNode {
		return errors.New("pool_min_per_node must be between 0 and pool_max_per_node")
	}
	if c.MaxClientConns < 1 {
		return errors.New("max_client_conns must be >= 1")
	}
	if c.MovedHopLimit < 1 {
		return errors.New("moved_hop_limit must be >= 1")
	}
	if c.MaxDatabases < 1 {
		return errors.New("max_databases must be >= 1")
	}
	if c.MaxPipeline < 1 {
		return errors.New("max_pipeline must be >= 1")
	}
	if c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 {
		return errors.New("dial/read/write timeouts must be > 0")
	}
	if _, err := c.UnixSocketPerm(); err != nil {
		return err
	}
	return nil
}

type fileConfig struct {
	UnixSocket          string            `yaml:"unix_socket"`
	UnixSocketMode      string            `yaml:"unix_socket_mode"`
	ClusterSeeds        []string          `yaml:"cluster_seeds"`
	ClusterUsername     string            `yaml:"cluster_username"`
	ClusterPassword     string            `yaml:"cluster_password"`
	TLS                 TLSConfig         `yaml:"tls"`
	BackendFlavor       string            `yaml:"backend_flavor"`
	PoolMinPerNode      *int              `yaml:"pool_min_per_node"`
	PoolMaxPerNode      *int              `yaml:"pool_max_per_node"`
	DialTimeout         time.Duration     `yaml:"dial_timeout"`
	ReadTimeout         time.Duration     `yaml:"read_timeout"`
	WriteTimeout        time.Duration     `yaml:"write_timeout"`
	ReconnectBackoffMin time.Duration     `yaml:"reconnect_backoff_min"`
	ReconnectBackoffMax time.Duration     `yaml:"reconnect_backoff_max"`
	TopologyRefresh     time.Duration     `yaml:"topology_refresh"`
	MaxClientConns      *int              `yaml:"max_client_conns"`
	BlockingMaxWait     time.Duration     `yaml:"blocking_max_wait"`
	MetricsAddr         string            `yaml:"metrics_addr"`
	LogLevel            string            `yaml:"log_level"`
	Pprof               *bool             `yaml:"pprof"`
	PprofAddr           string            `yaml:"pprof_addr"`
	ShutdownTimeout     time.Duration     `yaml:"shutdown_timeout"`
	ReadFromReplicas    *bool             `yaml:"read_from_replicas"`
	ClientAuthPassword  string            `yaml:"client_auth_password"`
	HashTagOpen         string            `yaml:"hash_tag_open"`
	HashTagClose        string            `yaml:"hash_tag_close"`
	MovedHopLimit       *int              `yaml:"moved_hop_limit"`
	AddressMap          map[string]string `yaml:"address_map"`
	MaxDatabases        *int              `yaml:"max_databases"`
	MaxPipeline         *int              `yaml:"max_pipeline"`
}
