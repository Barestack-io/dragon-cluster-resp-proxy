package pubsub

import (
	"testing"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func TestRewritePublish(t *testing.T) {
	argv := [][]byte{[]byte("PUBLISH"), []byte("ch"), []byte("hi")}
	got, did := RewritePublish(cluster.FlavorDragonfly, argv)
	if !did || string(got[0]) != "SPUBLISH" {
		t.Fatalf("got %q did=%v", got[0], did)
	}
	_, did = RewritePublish(cluster.FlavorRedis, argv)
	if did {
		t.Fatal("redis should not rewrite")
	}
}

func TestRewriteIncoming(t *testing.T) {
	v := resp.Array(
		resp.BulkString([]byte("smessage")),
		resp.BulkString([]byte("ch")),
		resp.BulkString([]byte("hi")),
	)
	got := rewriteIncoming(v)
	if string(got.Values[0].Str) != "message" {
		t.Fatalf("%q", got.Values[0].Str)
	}
}
