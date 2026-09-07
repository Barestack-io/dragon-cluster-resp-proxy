package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

// Health is a Dragonfly/Valkey node health state.
type Health uint8

const (
	HealthUnknown Health = iota
	HealthOnline
	HealthLoading
	HealthFail
	HealthHidden
)

func parseHealth(s string) Health {
	switch strings.ToLower(s) {
	case "online":
		return HealthOnline
	case "loading":
		return HealthLoading
	case "fail":
		return HealthFail
	case "hidden":
		return HealthHidden
	default:
		return HealthOnline
	}
}

func (h Health) String() string {
	switch h {
	case HealthOnline:
		return "online"
	case HealthLoading:
		return "loading"
	case HealthFail:
		return "fail"
	case HealthHidden:
		return "hidden"
	default:
		return "unknown"
	}
}

// Flavor is the detected backend.
type Flavor uint8

const (
	FlavorUnknown Flavor = iota
	FlavorDragonfly
	FlavorRedis
	FlavorEmulated
)

func (f Flavor) String() string {
	switch f {
	case FlavorDragonfly:
		return "dragonfly"
	case FlavorRedis:
		return "redis"
	case FlavorEmulated:
		return "emulated"
	default:
		return "unknown"
	}
}

// Node is a cluster endpoint.
type Node struct {
	ID     string
	Addr   string
	Role   string // master or replica
	Health Health
	Master bool
}

// Topology is an immutable snapshot of slot ownership.
type Topology struct {
	Generation uint64
	Flavor     Flavor
	Slots      [SlotCount]int32 // index into Nodes; -1 = unknown
	Nodes      []Node
	Masters    []int // indexes into Nodes
}

// Empty reports whether no slots are mapped.
func (t *Topology) Empty() bool {
	if t == nil || len(t.Nodes) == 0 {
		return true
	}
	return t.Slots[0] < 0 && t.Slots[1] < 0 && t.Slots[SlotCount/2] < 0
}

// NodeForSlot returns the master (or replica if wantReplica) for slot.
func (t *Topology) NodeForSlot(slot uint16, wantReplica bool) (Node, bool) {
	if t == nil || slot >= SlotCount {
		return Node{}, false
	}
	idx := t.Slots[slot]
	if idx < 0 || int(idx) >= len(t.Nodes) {
		return Node{}, false
	}
	master := t.Nodes[idx]
	if !wantReplica {
		return master, true
	}
	for i := range t.Nodes {
		n := t.Nodes[i]
		if !n.Master && n.Health == HealthOnline && replicaOf(n, master) {
			return n, true
		}
	}
	return master, true
}

func replicaOf(n, master Node) bool {
	// We do not store replica-of id separately on SLOTS; replicas share the same
	// slot range. Approximate: any non-master with same slot mapping is a replica
	// of that master. Callers pass masters from Slots[] which already point at the master.
	_ = master
	return !n.Master
}

// Map is an atomic pointer to the current Topology.
type Map struct {
	ptr atomic.Pointer[Topology]
	gen atomic.Uint64
}

// Load returns the current topology (may be nil before first refresh).
func (m *Map) Load() *Topology {
	return m.ptr.Load()
}

// Store installs topo and assigns a generation.
func (m *Map) Store(topo *Topology) {
	if topo == nil {
		return
	}
	topo.Generation = m.gen.Add(1)
	m.ptr.Store(topo)
}

// UpdateSlot patches a single slot (MOVED) by cloning the current map.
func (m *Map) UpdateSlot(slot uint16, addr, id string) {
	cur := m.Load()
	next := cloneTopology(cur)
	if next == nil {
		next = newEmptyTopology()
	}
	idx := nodeIndex(next, addr, id)
	if idx < 0 {
		next.Nodes = append(next.Nodes, Node{
			ID:     id,
			Addr:   addr,
			Role:   "master",
			Health: HealthOnline,
			Master: true,
		})
		idx = len(next.Nodes) - 1
		next.Masters = append(next.Masters, idx)
	}
	next.Slots[slot] = int32(idx)
	m.Store(next)
}

func cloneTopology(t *Topology) *Topology {
	if t == nil {
		return nil
	}
	n := *t
	n.Nodes = append([]Node(nil), t.Nodes...)
	n.Masters = append([]int(nil), t.Masters...)
	return &n
}

func newEmptyTopology() *Topology {
	t := &Topology{}
	for i := range t.Slots {
		t.Slots[i] = -1
	}
	return t
}

func nodeIndex(t *Topology, addr, id string) int {
	for i, n := range t.Nodes {
		if addr != "" && n.Addr == addr {
			return i
		}
		if id != "" && n.ID == id {
			return i
		}
	}
	return -1
}

// ParseSlots builds a Topology from a CLUSTER SLOTS array value.
func ParseSlots(v resp.Value) (*Topology, error) {
	if v.Type != resp.TypeArray || v.Null {
		return nil, fmt.Errorf("cluster slots: not an array")
	}
	t := newEmptyTopology()
	for _, entry := range v.Values {
		if entry.Type != resp.TypeArray || len(entry.Values) < 3 {
			continue
		}
		start := int(entry.Values[0].Int)
		end := int(entry.Values[1].Int)
		for ni := 2; ni < len(entry.Values); ni++ {
			node, ok := parseSlotsNode(entry.Values[ni])
			if !ok {
				continue
			}
			node.Master = ni == 2
			if node.Master {
				node.Role = "master"
			} else {
				node.Role = "replica"
			}
			idx := upsertNode(t, node)
			if node.Master {
				for s := start; s <= end && s < SlotCount; s++ {
					t.Slots[s] = int32(idx)
				}
			}
		}
	}
	return t, nil
}

func parseSlotsNode(v resp.Value) (Node, bool) {
	if v.Type != resp.TypeArray || len(v.Values) < 2 {
		return Node{}, false
	}
	ip := string(v.Values[0].Str)
	port := v.Values[1].Int
	if v.Values[1].Type == resp.TypeBulkString || v.Values[1].Type == resp.TypeSimpleString {
		p, err := strconv.ParseInt(string(v.Values[1].Str), 10, 64)
		if err == nil {
			port = p
		}
	}
	id := ""
	if len(v.Values) >= 3 {
		id = string(v.Values[2].Str)
	}
	if ip == "" || port <= 0 {
		return Node{}, false
	}
	return Node{
		ID:     id,
		Addr:   fmt.Sprintf("%s:%d", ip, port),
		Health: HealthOnline,
	}, true
}

// ParseShards builds a Topology from CLUSTER SHARDS (Redis 7 / Dragonfly).
func ParseShards(v resp.Value) (*Topology, error) {
	if v.Type != resp.TypeArray || v.Null {
		return nil, fmt.Errorf("cluster shards: not an array")
	}
	t := newEmptyTopology()
	for _, shard := range v.Values {
		if shard.Type != resp.TypeArray {
			continue
		}
		var ranges [][2]int
		var nodes []Node
		for i := 0; i+1 < len(shard.Values); i += 2 {
			key := strings.ToLower(string(shard.Values[i].Str))
			val := shard.Values[i+1]
			switch key {
			case "slots":
				for j := 0; j+1 < len(val.Values); j += 2 {
					ranges = append(ranges, [2]int{int(val.Values[j].Int), int(val.Values[j+1].Int)})
				}
			case "nodes":
				for _, nv := range val.Values {
					if n, ok := parseShardNode(nv); ok {
						nodes = append(nodes, n)
					}
				}
			}
		}
		masterIdx := -1
		for _, n := range nodes {
			if n.Health == HealthHidden {
				continue
			}
			idx := upsertNode(t, n)
			if n.Master {
				masterIdx = idx
			}
		}
		if masterIdx >= 0 {
			for _, r := range ranges {
				for s := r[0]; s <= r[1] && s < SlotCount; s++ {
					t.Slots[s] = int32(masterIdx)
				}
			}
		}
	}
	return t, nil
}

func parseShardNode(v resp.Value) (Node, bool) {
	if v.Type != resp.TypeArray {
		return Node{}, false
	}
	var n Node
	n.Health = HealthOnline
	for i := 0; i+1 < len(v.Values); i += 2 {
		k := strings.ToLower(string(v.Values[i].Str))
		val := v.Values[i+1]
		s := string(val.Str)
		switch k {
		case "id":
			n.ID = s
		case "endpoint", "ip":
			if n.Addr == "" || k == "endpoint" {
				host := s
				// port filled later
				if host != "" {
					n.Addr = host
				}
			}
		case "port":
			port := val.Int
			if val.Type == resp.TypeBulkString {
				p, err := strconv.ParseInt(string(val.Str), 10, 64)
				if err == nil {
					port = p
				}
			}
			if n.Addr != "" && !strings.Contains(n.Addr, ":") {
				n.Addr = fmt.Sprintf("%s:%d", n.Addr, port)
			}
		case "role":
			n.Role = strings.ToLower(s)
			n.Master = n.Role == "master"
		case "health":
			n.Health = parseHealth(s)
		}
	}
	if n.Addr == "" || !strings.Contains(n.Addr, ":") {
		return Node{}, false
	}
	return n, true
}

func upsertNode(t *Topology, n Node) int {
	if i := nodeIndex(t, n.Addr, n.ID); i >= 0 {
		if n.ID != "" {
			t.Nodes[i].ID = n.ID
		}
		t.Nodes[i].Role = n.Role
		t.Nodes[i].Master = n.Master
		t.Nodes[i].Health = n.Health
		return i
	}
	t.Nodes = append(t.Nodes, n)
	idx := len(t.Nodes) - 1
	if n.Master {
		t.Masters = append(t.Masters, idx)
	}
	return idx
}

// DetectFlavor inspects an INFO reply body.
func DetectFlavor(info string) Flavor {
	low := strings.ToLower(info)
	if strings.Contains(low, "dragonfly_version") {
		if strings.Contains(low, "cluster_mode:emulated") || strings.Contains(low, "cluster_enabled:0") {
			return FlavorEmulated
		}
		return FlavorDragonfly
	}
	return FlavorRedis
}

// FirstMaster returns the first master node or empty.
func (t *Topology) FirstMaster() (Node, bool) {
	if t == nil {
		return Node{}, false
	}
	for _, i := range t.Masters {
		if i >= 0 && i < len(t.Nodes) {
			return t.Nodes[i], true
		}
	}
	for _, n := range t.Nodes {
		if n.Master || n.Role == "master" {
			return n, true
		}
	}
	if len(t.Nodes) > 0 {
		return t.Nodes[0], true
	}
	return Node{}, false
}
