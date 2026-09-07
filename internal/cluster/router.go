package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/config"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/pool"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

// Router owns topology, per-node pools, and command execution with MOVED/ASK.
type Router struct {
	cfg     config.Config
	log     *slog.Logger
	metrics *metrics.Metrics
	slots   Map
	flavor  Flavor

	mu    sync.Mutex
	pools map[string]*pool.Pool
	tls   *tls.Config

	open, close byte
}

// NewRouter builds a router and performs an initial topology refresh.
func NewRouter(ctx context.Context, cfg config.Config, log *slog.Logger, m *metrics.Metrics) (*Router, error) {
	r := &Router{
		cfg:     cfg,
		log:     log,
		metrics: m,
		pools:   make(map[string]*pool.Pool),
		open:    '{',
		close:   '}',
	}
	r.open, r.close = cfg.HashTagRunes()
	if cfg.TLS.Enabled {
		tc, err := buildTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		r.tls = tc
	}
	switch strings.ToLower(cfg.BackendFlavor) {
	case "dragonfly":
		r.flavor = FlavorDragonfly
	case "redis":
		r.flavor = FlavorRedis
	}

	if err := r.Refresh(ctx); err != nil {
		log.WarnContext(ctx, "initial topology refresh failed; using seeds only", "error", err)
		r.installSeeds()
	}
	return r, nil
}

func (r *Router) installSeeds() {
	t := newEmptyTopology()
	t.Flavor = r.flavor
	for _, seed := range r.cfg.ClusterSeeds {
		n := Node{Addr: seed, Role: "master", Health: HealthOnline, Master: true}
		idx := upsertNode(t, n)
		_ = idx
	}
	if len(t.Nodes) > 0 {
		for i := range t.Slots {
			t.Slots[i] = 0
		}
	}
	r.slots.Store(t)
}

// Flavor returns the detected or configured backend flavor.
func (r *Router) Flavor() Flavor {
	if t := r.slots.Load(); t != nil && t.Flavor != FlavorUnknown {
		return t.Flavor
	}
	return r.flavor
}

// Slots returns the atomic slot map.
func (r *Router) Slots() *Map { return &r.slots }

// SlotOf hashes key with configured tag delimiters.
func (r *Router) SlotOf(key []byte) uint16 {
	return KeySlot(key, r.open, r.close)
}

// CrossSlot reports whether keys span more than one slot.
func (r *Router) CrossSlot(keys [][]byte) bool {
	if len(keys) <= 1 {
		return false
	}
	s := r.SlotOf(keys[0])
	for i := 1; i < len(keys); i++ {
		if r.SlotOf(keys[i]) != s {
			return true
		}
	}
	return false
}

// UniqueSlot returns the shared slot or false if empty/cross-slot.
func (r *Router) UniqueSlot(keys [][]byte) (uint16, bool) {
	if len(keys) == 0 {
		return 0, false
	}
	s := r.SlotOf(keys[0])
	for i := 1; i < len(keys); i++ {
		if r.SlotOf(keys[i]) != s {
			return 0, false
		}
	}
	return s, true
}

// Generation returns the current topology generation.
func (r *Router) Generation() uint64 {
	if t := r.slots.Load(); t != nil {
		return t.Generation
	}
	return 0
}

// Refresh queries CLUSTER SHARDS (then SLOTS) from a seed/master.
func (r *Router) Refresh(ctx context.Context) error {
	addrs := r.candidateAddrs()
	var last error
	for _, addr := range addrs {
		if err := r.refreshFrom(ctx, addr); err != nil {
			last = err
			if isClusterNotConfigured(err) {
				if r.metrics != nil {
					r.metrics.BackendErrors.WithLabelValues("cluster_not_configured").Inc()
				}
				continue
			}
			continue
		}
		if r.metrics != nil {
			r.metrics.TopologyRefresh.WithLabelValues("ok").Inc()
		}
		return nil
	}
	if r.metrics != nil {
		r.metrics.TopologyRefresh.WithLabelValues("error").Inc()
	}
	if last == nil {
		last = errors.New("no cluster seeds")
	}
	return last
}

func (r *Router) candidateAddrs() []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(a string) {
		if a == "" {
			return
		}
		if _, ok := seen[a]; ok {
			return
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	if t := r.slots.Load(); t != nil {
		for _, n := range t.Nodes {
			if n.Master {
				add(n.Addr)
			}
		}
	}
	for _, s := range r.cfg.ClusterSeeds {
		add(s)
	}
	return out
}

func (r *Router) refreshFrom(ctx context.Context, addr string) error {
	p := r.poolFor(addr, false)
	c, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(c)

	if r.flavor == FlavorUnknown || r.cfg.BackendFlavor == "auto" {
		info, ierr := c.Do([][]byte{[]byte("INFO"), []byte("server")}, r.cfg.WriteTimeout, r.cfg.ReadTimeout)
		if ierr == nil && !info.IsError() {
			r.flavor = DetectFlavor(string(info.Str))
		}
	}

	v, err := c.Do([][]byte{[]byte("CLUSTER"), []byte("SHARDS")}, r.cfg.WriteTimeout, r.cfg.ReadTimeout)
	var topo *Topology
	if err == nil && !v.IsError() {
		topo, err = ParseShards(v)
	} else {
		v, err = c.Do([][]byte{[]byte("CLUSTER"), []byte("SLOTS")}, r.cfg.WriteTimeout, r.cfg.ReadTimeout)
		if err != nil {
			return err
		}
		if v.IsError() {
			return fmt.Errorf("%s", v.Str)
		}
		topo, err = ParseSlots(v)
	}
	if err != nil {
		return err
	}
	topo.Flavor = r.flavor
	r.slots.Store(topo)
	r.syncPools(topo)
	r.log.InfoContext(ctx, "topology refreshed", "nodes", len(topo.Nodes), "flavor", topo.Flavor.String(), "from", addr)
	return nil
}

func isClusterNotConfigured(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Cluster is not yet configured")
}

func (r *Router) syncPools(t *Topology) {
	r.mu.Lock()
	defer r.mu.Unlock()
	live := map[string]struct{}{}
	for _, n := range t.Nodes {
		addr := r.rewrite(n.Addr)
		live[addr] = struct{}{}
		if _, ok := r.pools[addr]; !ok {
			r.pools[addr] = r.newPool(addr, !n.Master && r.cfg.ReadFromReplicas)
		}
	}
	for addr, p := range r.pools {
		if _, ok := live[addr]; !ok {
			p.Close()
			delete(r.pools, addr)
		}
	}
}

func (r *Router) newPool(addr string, replica bool) *pool.Pool {
	return pool.New(context.Background(), pool.Options{
		Addr:         addr,
		MinIdle:      r.cfg.PoolMinPerNode,
		MaxSize:      r.cfg.PoolMaxPerNode,
		DialTimeout:  r.cfg.DialTimeout,
		ReadTimeout:  r.cfg.ReadTimeout,
		WriteTimeout: r.cfg.WriteTimeout,
		BackoffMin:   r.cfg.ReconnectBackoffMin,
		BackoffMax:   r.cfg.ReconnectBackoffMax,
		TLS:          r.tls,
		Username:     r.cfg.ClusterUsername,
		Password:     r.cfg.ClusterPassword,
		ReadOnly:     replica && r.Flavor() == FlavorRedis,
	})
}

func (r *Router) rewrite(addr string) string {
	if mapped, ok := r.cfg.AddressMap[addr]; ok && mapped != "" {
		return mapped
	}
	return addr
}

func (r *Router) poolFor(addr string, replica bool) *pool.Pool {
	addr = r.rewrite(addr)
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pools[addr]; ok {
		return p
	}
	p := r.newPool(addr, replica)
	r.pools[addr] = p
	return p
}

// Pool returns the pool for addr, creating it if needed.
func (r *Router) Pool(addr string) *pool.Pool {
	return r.poolFor(addr, false)
}

// NodeForSlot returns the node that should serve slot.
func (r *Router) NodeForSlot(slot uint16, read bool) (Node, bool) {
	t := r.slots.Load()
	if t == nil {
		return Node{}, false
	}
	return t.NodeForSlot(slot, read && r.cfg.ReadFromReplicas)
}

// AnyMaster returns any known master (for keyless commands).
func (r *Router) AnyMaster() (Node, bool) {
	if t := r.slots.Load(); t != nil {
		if n, ok := t.FirstMaster(); ok {
			return n, true
		}
	}
	if len(r.cfg.ClusterSeeds) > 0 {
		return Node{Addr: r.cfg.ClusterSeeds[0], Master: true, Health: HealthOnline}, true
	}
	return Node{}, false
}

// MasterAddrs returns known master addresses (topology first, then seeds).
func (r *Router) MasterAddrs() []string {
	return r.candidateAddrs()
}

// Exec sends argv to the owner of slot, following MOVED/ASK.
func (r *Router) Exec(ctx context.Context, slot uint16, argv [][]byte, read bool, readTO time.Duration) (resp.Value, error) {
	return r.execFrame(ctx, slot, nil, argv, read, readTO, false)
}

// ExecRaw sends a pre-encoded frame (same hop/MOVED logic).
func (r *Router) ExecRaw(ctx context.Context, slot uint16, frame []byte, read bool, readTO time.Duration) (resp.Value, error) {
	return r.execFrame(ctx, slot, frame, nil, read, readTO, false)
}

func (r *Router) execFrame(ctx context.Context, slot uint16, frame []byte, argv [][]byte, read bool, readTO time.Duration, asking bool) (resp.Value, error) {
	if readTO <= 0 {
		readTO = r.cfg.ReadTimeout
	}
	var last resp.Value
	addrHint := ""
	for hop := 0; hop < r.cfg.MovedHopLimit; hop++ {
		if err := ctx.Err(); err != nil {
			return resp.Value{}, err
		}
		addr := addrHint
		if addr == "" {
			n, ok := r.NodeForSlot(slot, read && !asking)
			if !ok {
				n, ok = r.AnyMaster()
				if !ok {
					return resp.Value{}, errors.New("no backend nodes")
				}
			}
			addr = n.Addr
		}
		p := r.poolFor(addr, read && r.cfg.ReadFromReplicas)
		c, err := p.Get(ctx)
		if err != nil {
			return resp.Value{}, err
		}
		if asking {
			if _, err := c.Do([][]byte{[]byte("ASKING")}, r.cfg.WriteTimeout, r.cfg.ReadTimeout); err != nil {
				p.Drop(c)
				return resp.Value{}, err
			}
		}
		var v resp.Value
		if frame != nil {
			v, err = c.DoRaw(frame, r.cfg.WriteTimeout, readTO)
		} else {
			v, err = c.Do(argv, r.cfg.WriteTimeout, readTO)
		}
		if err != nil {
			p.Drop(c)
			return resp.Value{}, err
		}
		p.Put(c)
		last = v
		if !v.IsError() {
			return v, nil
		}
		red, ok := ParseRedirect(v.Str)
		if !ok {
			if v.EqualKind("LOADING") && r.metrics != nil {
				r.metrics.BackendErrors.WithLabelValues("loading").Inc()
			}
			return v, nil
		}
		if r.metrics != nil {
			r.metrics.Redirects.WithLabelValues(red.Kind).Inc()
		}
		slot = red.Slot
		addrHint = red.Addr
		if red.Kind == "MOVED" {
			r.slots.UpdateSlot(red.Slot, red.Addr, "")
			asking = false
			continue
		}
		// ASK: one-shot, do not persist
		asking = true
		read = false
	}
	return last, nil
}

// Checkout returns a dedicated connection to the owner of slot (WATCH/blocking).
func (r *Router) Checkout(ctx context.Context, slot uint16) (*pool.Conn, *pool.Pool, error) {
	n, ok := r.NodeForSlot(slot, false)
	if !ok {
		n, ok = r.AnyMaster()
		if !ok {
			return nil, nil, errors.New("no backend nodes")
		}
	}
	p := r.poolFor(n.Addr, false)
	c, err := p.Get(ctx)
	return c, p, err
}

// CheckoutAddr checks out a connection to a specific address.
func (r *Router) CheckoutAddr(ctx context.Context, addr string) (*pool.Conn, *pool.Pool, error) {
	p := r.poolFor(addr, false)
	c, err := p.Get(ctx)
	return c, p, err
}

// RunRefreshLoop refreshes topology until ctx is cancelled.
func (r *Router) RunRefreshLoop(ctx context.Context) {
	t := time.NewTimer(r.cfg.TopologyRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Refresh(ctx); err != nil {
				r.log.WarnContext(ctx, "topology refresh failed", "error", err)
			}
			r.exportPoolStats()
			t.Reset(r.cfg.TopologyRefresh)
		}
	}
}

func (r *Router) exportPoolStats() {
	if r.metrics == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for addr, p := range r.pools {
		idle, active, waiters := p.Stats()
		r.metrics.Pool.WithLabelValues(addr, "idle").Set(float64(idle))
		r.metrics.Pool.WithLabelValues(addr, "active").Set(float64(active))
		r.metrics.Pool.WithLabelValues(addr, "waiters").Set(float64(waiters))
	}
}

// Close closes all pools.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pools {
		p.Close()
	}
	r.pools = map[string]*pool.Pool{}
}

func buildTLS(c config.TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify} //nolint:gosec // operator-controlled
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("tls: invalid ca_file")
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
