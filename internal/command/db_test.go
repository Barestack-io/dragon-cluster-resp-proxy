package command

import (
	"bytes"
	"testing"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func TestDBPrefixDB0Unprefixed(t *testing.T) {
	if p := DBPrefix(0); p != nil {
		t.Fatalf("db0 prefix %q", p)
	}
	if got := string(PrefixKey(0, []byte("foo"))); got != "foo" {
		t.Fatalf("db0 key %q", got)
	}
	if got := string(PrefixKey(4, []byte("foo"))); got != "db4:foo" {
		t.Fatalf("db4 key %q", got)
	}
	if got := string(StripKey(4, []byte("db4:foo"))); got != "foo" {
		t.Fatalf("strip %q", got)
	}
	if !HasVirtualDBPrefix([]byte("db4:foo")) {
		t.Fatal("expected virtual prefix")
	}
	if HasVirtualDBPrefix([]byte("foo")) || HasVirtualDBPrefix([]byte("db:foo")) {
		t.Fatal("false virtual prefix")
	}
}

func TestRewriteArgvDB0Noop(t *testing.T) {
	in := argvOf("SET", "k", "v")
	out := RewriteArgv(in, 0)
	if string(out[1]) != "k" {
		t.Fatalf("db0 should not rewrite, got %s", out[1])
	}
}

func TestRewriteArgvDB4(t *testing.T) {
	got := RewriteArgv(argvOf("SET", "k", "v"), 4)
	if string(got[1]) != "db4:k" {
		t.Fatalf("SET key %s", got[1])
	}
	got = RewriteArgv(argvOf("MGET", "a", "b"), 4)
	if string(got[1]) != "db4:a" || string(got[2]) != "db4:b" {
		t.Fatalf("MGET %#v", got)
	}
	got = RewriteArgv(argvOf("EVAL", "return 1", "1", "ka", "arg"), 4)
	if string(got[3]) != "db4:ka" || string(got[4]) != "arg" {
		t.Fatalf("EVAL %#v", got)
	}
	got = RewriteArgv(argvOf("SCAN", "0"), 4)
	if len(got) < 4 || !bytes.EqualFold(got[2], []byte("MATCH")) || string(got[3]) != "db4:*" {
		t.Fatalf("SCAN inject %#v", got)
	}
	got = RewriteArgv(argvOf("SCAN", "0", "MATCH", "foo*"), 4)
	if string(got[3]) != "db4:foo*" {
		t.Fatalf("SCAN MATCH %s", got[3])
	}
	got = RewriteArgv(argvOf("KEYS", "*"), 4)
	if string(got[1]) != "db4:*" {
		t.Fatalf("KEYS %s", got[1])
	}
	got = RewriteArgv(argvOf("COPY", "src", "dst", "DB", "2", "REPLACE"), 4)
	if string(got[1]) != "db4:src" || string(got[2]) != "db2:dst" {
		t.Fatalf("COPY %#v", got)
	}
	if len(got) != 4 || string(got[3]) != "REPLACE" {
		t.Fatalf("COPY should drop DB option: %#v", got)
	}
	got = RewriteArgv(argvOf("PUBLISH", "chan", "hi"), 4)
	if string(got[1]) != "chan" {
		t.Fatalf("PUBLISH must not prefix channel: %s", got[1])
	}
}

func TestStripReplyKeys(t *testing.T) {
	v := StripReplyKeys("KEYS", 4, resp.Array(resp.BulkString([]byte("db4:a")), resp.BulkString([]byte("db4:b"))))
	if string(v.Values[0].Str) != "a" || string(v.Values[1].Str) != "b" {
		t.Fatalf("KEYS strip %#v", v)
	}
	scan := resp.Array(resp.BulkString([]byte("0")), resp.Array(resp.BulkString([]byte("db4:x"))))
	got := StripReplyKeys("SCAN", 4, scan)
	if string(got.Values[1].Values[0].Str) != "x" {
		t.Fatalf("SCAN strip %#v", got)
	}
	blpop := resp.Array(resp.BulkString([]byte("db4:q")), resp.BulkString([]byte("v")))
	got = StripReplyKeys("BLPOP", 4, blpop)
	if string(got.Values[0].Str) != "q" || string(got.Values[1].Str) != "v" {
		t.Fatalf("BLPOP strip %#v", got)
	}
	got = StripReplyKeys("GET", 4, resp.BulkString([]byte("db4:v")))
	if string(got.Str) != "db4:v" {
		t.Fatal("GET values must not be stripped")
	}
}

func TestParseDBIndex(t *testing.T) {
	n, err := ParseDBIndex([]byte("4"), 16)
	if err != nil || n != 4 {
		t.Fatalf("got %d %v", n, err)
	}
	if _, err := ParseDBIndex([]byte("0"), 16); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseDBIndex([]byte("16"), 16); err == nil {
		t.Fatal("expected range error")
	}
	if _, err := ParseDBIndex([]byte("nope"), 16); err == nil {
		t.Fatal("expected invalid")
	}
}
