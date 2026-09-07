package cluster

import (
	"bytes"
	"testing"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func TestKeySlotHashTag(t *testing.T) {
	// Redis cluster known vectors: "{user}.foo" and "{user}.bar" share a slot.
	a := KeySlot([]byte("{user}.foo"), '{', '}')
	b := KeySlot([]byte("{user}.bar"), '{', '}')
	if a != b {
		t.Fatalf("hash tags should match: %d vs %d", a, b)
	}
	if KeySlot([]byte("{}x"), '{', '}') == KeySlot([]byte("{a}x"), '{', '}') && string(HashTag([]byte("{}x"), '{', '}')) != "{}x" {
		t.Fatal("empty tag should use full key")
	}
	if string(HashTag([]byte("nbrace"), '{', '}')) != "nbrace" {
		t.Fatal("no tag")
	}
	if KeySlot([]byte("foo"), '{', '}') > 16383 {
		t.Fatal("slot out of range")
	}
}

func TestParseRedirectIPv6(t *testing.T) {
	r, ok := ParseRedirect([]byte("MOVED 12 2001:db8::1:7000"))
	if !ok {
		t.Fatal("parse")
	}
	if r.Slot != 12 || r.Kind != "MOVED" {
		t.Fatalf("%+v", r)
	}
	host, port, ok := SplitHostPort(r.Addr)
	if !ok || port != "7000" || host != "2001:db8::1" {
		t.Fatalf("host=%s port=%s", host, port)
	}
}

func TestParseSlots(t *testing.T) {
	raw := resp.Encode(nil, resp.Array(
		resp.Array(
			resp.Integer(0), resp.Integer(16383),
			resp.Array(resp.BulkString([]byte("10.0.0.1")), resp.Integer(6379), resp.BulkString([]byte("abc"))),
		),
	))
	v, err := resp.NewReader(bytes.NewReader(raw)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	topo, err := ParseSlots(v)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := topo.NodeForSlot(100, false)
	if !ok || n.Addr != "10.0.0.1:6379" {
		t.Fatalf("%+v", n)
	}
}

func TestMapUpdateSlot(t *testing.T) {
	var m Map
	m.UpdateSlot(7, "127.0.0.1:7001", "id1")
	topo := m.Load()
	n, ok := topo.NodeForSlot(7, false)
	if !ok || n.Addr != "127.0.0.1:7001" {
		t.Fatalf("%+v ok=%v", n, ok)
	}
	if topo.Generation == 0 {
		t.Fatal("generation")
	}
}

func TestDetectFlavor(t *testing.T) {
	if DetectFlavor("dragonfly_version:1.40.2\r\n") != FlavorDragonfly {
		t.Fatal("dragonfly")
	}
	if DetectFlavor("redis_version:7.2.0\r\n") != FlavorRedis {
		t.Fatal("redis")
	}
}

func BenchmarkKeySlot(b *testing.B) {
	k := []byte("user:{42}:profile")
	b.ReportAllocs()
	for b.Loop() {
		_ = KeySlot(k, '{', '}')
	}
}
