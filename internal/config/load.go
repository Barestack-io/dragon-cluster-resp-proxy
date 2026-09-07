package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads optional YAML from path (or DCRP_CONFIG), then overlays environment
// variables. Environment values always win.
func Load(path string) (Config, error) {
	if path == "" {
		path = os.Getenv(envPrefix + "CONFIG")
	}
	cfg := Defaults()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("reading config %s: %w", path, err)
		}
		var fc fileConfig
		if err := yaml.Unmarshal(raw, &fc); err != nil {
			return Config{}, fmt.Errorf("parsing config %s: %w", path, err)
		}
		applyFile(&cfg, fc)
	}
	applyEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func applyFile(cfg *Config, fc fileConfig) {
	if fc.UnixSocket != "" {
		cfg.UnixSocket = fc.UnixSocket
	}
	if fc.UnixSocketMode != "" {
		cfg.UnixSocketMode = fc.UnixSocketMode
	}
	if len(fc.ClusterSeeds) > 0 {
		cfg.ClusterSeeds = append([]string(nil), fc.ClusterSeeds...)
	}
	if fc.ClusterUsername != "" {
		cfg.ClusterUsername = fc.ClusterUsername
	}
	if fc.ClusterPassword != "" {
		cfg.ClusterPassword = fc.ClusterPassword
	}
	cfg.TLS.Enabled = cfg.TLS.Enabled || fc.TLS.Enabled
	if fc.TLS.CAFile != "" {
		cfg.TLS.CAFile = fc.TLS.CAFile
	}
	if fc.TLS.CertFile != "" {
		cfg.TLS.CertFile = fc.TLS.CertFile
	}
	if fc.TLS.KeyFile != "" {
		cfg.TLS.KeyFile = fc.TLS.KeyFile
	}
	cfg.TLS.InsecureSkipVerify = cfg.TLS.InsecureSkipVerify || fc.TLS.InsecureSkipVerify
	if fc.BackendFlavor != "" {
		cfg.BackendFlavor = fc.BackendFlavor
	}
	if fc.PoolMinPerNode != nil {
		cfg.PoolMinPerNode = *fc.PoolMinPerNode
	}
	if fc.PoolMaxPerNode != nil {
		cfg.PoolMaxPerNode = *fc.PoolMaxPerNode
	}
	if fc.DialTimeout > 0 {
		cfg.DialTimeout = fc.DialTimeout
	}
	if fc.ReadTimeout > 0 {
		cfg.ReadTimeout = fc.ReadTimeout
	}
	if fc.WriteTimeout > 0 {
		cfg.WriteTimeout = fc.WriteTimeout
	}
	if fc.ReconnectBackoffMin > 0 {
		cfg.ReconnectBackoffMin = fc.ReconnectBackoffMin
	}
	if fc.ReconnectBackoffMax > 0 {
		cfg.ReconnectBackoffMax = fc.ReconnectBackoffMax
	}
	if fc.TopologyRefresh > 0 {
		cfg.TopologyRefresh = fc.TopologyRefresh
	}
	if fc.MaxClientConns != nil {
		cfg.MaxClientConns = *fc.MaxClientConns
	}
	if fc.BlockingMaxWait > 0 {
		cfg.BlockingMaxWait = fc.BlockingMaxWait
	}
	if fc.MetricsAddr != "" {
		cfg.MetricsAddr = fc.MetricsAddr
	}
	if fc.LogLevel != "" {
		cfg.LogLevel = fc.LogLevel
	}
	if fc.Pprof != nil {
		cfg.Pprof = *fc.Pprof
	}
	if fc.PprofAddr != "" {
		cfg.PprofAddr = fc.PprofAddr
	}
	if fc.ShutdownTimeout > 0 {
		cfg.ShutdownTimeout = fc.ShutdownTimeout
	}
	if fc.ReadFromReplicas != nil {
		cfg.ReadFromReplicas = *fc.ReadFromReplicas
	}
	if fc.ClientAuthPassword != "" {
		cfg.ClientAuthPassword = fc.ClientAuthPassword
	}
	if fc.HashTagOpen != "" {
		cfg.HashTagOpen = fc.HashTagOpen
	}
	if fc.HashTagClose != "" {
		cfg.HashTagClose = fc.HashTagClose
	}
	if fc.MovedHopLimit != nil {
		cfg.MovedHopLimit = *fc.MovedHopLimit
	}
	if len(fc.AddressMap) > 0 {
		cfg.AddressMap = cloneMap(fc.AddressMap)
	}
	if fc.MaxDatabases != nil {
		cfg.MaxDatabases = *fc.MaxDatabases
	}
	if fc.MaxPipeline != nil {
		cfg.MaxPipeline = *fc.MaxPipeline
	}
}

func applyEnv(cfg *Config) {
	setStr := func(key string, dst *string) {
		if v, ok := os.LookupEnv(envPrefix + key); ok && v != "" {
			*dst = v
		}
	}
	setBool := func(key string, dst *bool) {
		if v, ok := os.LookupEnv(envPrefix + key); ok && v != "" {
			*dst = parseBool(v)
		}
	}
	setInt := func(key string, dst *int) {
		if v, ok := os.LookupEnv(envPrefix + key); ok && v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	setDur := func(key string, dst *time.Duration) {
		if v, ok := os.LookupEnv(envPrefix + key); ok && v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				*dst = d
			}
		}
	}

	setStr("UNIX_SOCKET", &cfg.UnixSocket)
	setStr("UNIX_SOCKET_MODE", &cfg.UnixSocketMode)
	if v, ok := os.LookupEnv(envPrefix + "CLUSTER_SEEDS"); ok && v != "" {
		cfg.ClusterSeeds = splitCSV(v)
	}
	setStr("CLUSTER_USERNAME", &cfg.ClusterUsername)
	setStr("CLUSTER_PASSWORD", &cfg.ClusterPassword)
	setBool("TLS_ENABLED", &cfg.TLS.Enabled)
	setStr("TLS_CA_FILE", &cfg.TLS.CAFile)
	setStr("TLS_CERT_FILE", &cfg.TLS.CertFile)
	setStr("TLS_KEY_FILE", &cfg.TLS.KeyFile)
	setBool("TLS_INSECURE_SKIP_VERIFY", &cfg.TLS.InsecureSkipVerify)
	setStr("BACKEND_FLAVOR", &cfg.BackendFlavor)
	setInt("POOL_MIN_PER_NODE", &cfg.PoolMinPerNode)
	setInt("POOL_MAX_PER_NODE", &cfg.PoolMaxPerNode)
	setDur("DIAL_TIMEOUT", &cfg.DialTimeout)
	setDur("READ_TIMEOUT", &cfg.ReadTimeout)
	setDur("WRITE_TIMEOUT", &cfg.WriteTimeout)
	setDur("RECONNECT_BACKOFF_MIN", &cfg.ReconnectBackoffMin)
	setDur("RECONNECT_BACKOFF_MAX", &cfg.ReconnectBackoffMax)
	setDur("TOPOLOGY_REFRESH", &cfg.TopologyRefresh)
	setInt("MAX_CLIENT_CONNS", &cfg.MaxClientConns)
	setDur("BLOCKING_MAX_WAIT", &cfg.BlockingMaxWait)
	setStr("METRICS_ADDR", &cfg.MetricsAddr)
	setStr("LOG_LEVEL", &cfg.LogLevel)
	setBool("PPROF", &cfg.Pprof)
	setStr("PPROF_ADDR", &cfg.PprofAddr)
	setDur("SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)
	setBool("READ_FROM_REPLICAS", &cfg.ReadFromReplicas)
	setStr("CLIENT_AUTH_PASSWORD", &cfg.ClientAuthPassword)
	setStr("HASH_TAG_OPEN", &cfg.HashTagOpen)
	setStr("HASH_TAG_CLOSE", &cfg.HashTagClose)
	setInt("MOVED_HOP_LIMIT", &cfg.MovedHopLimit)
	if v, ok := os.LookupEnv(envPrefix + "ADDRESS_MAP"); ok && v != "" {
		cfg.AddressMap = parseAddressMap(v)
	}
	setInt("MAX_DATABASES", &cfg.MaxDatabases)
	setInt("MAX_PIPELINE", &cfg.MaxPipeline)
}

func parseAddressMap(v string) map[string]string {
	out := make(map[string]string)
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		from, to, ok := strings.Cut(part, "=")
		if !ok || from == "" || to == "" {
			continue
		}
		out[from] = to
	}
	return out
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
