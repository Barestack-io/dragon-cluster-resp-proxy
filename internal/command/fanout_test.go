package command

import (
	"testing"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func splitSlot(k []byte) uint16 {
	if len(k) > 0 && k[0] < 'm' {
		return 1
	}
	return 2
}

func TestFanoutOf(t *testing.T) {
	cases := []struct {
		argv [][]byte
		want Fanout
	}{
		{argvOf("DEL", "a", "b"), FanoutSum},
		{argvOf("UNLINK", "a"), FanoutSum},
		{argvOf("EXISTS", "a", "b"), FanoutSum},
		{argvOf("TOUCH", "a", "b"), FanoutSum},
		{argvOf("MGET", "a", "b"), FanoutMGet},
		{argvOf("MSET", "a", "1", "b", "2"), FanoutMSet},
		{argvOf("MSETNX", "a", "1", "b", "2"), FanoutNone},
		{argvOf("SINTER", "a", "b"), FanoutNone},
		{argvOf("GET", "a"), FanoutNone},
	}
	for _, tc := range cases {
		if got := FanoutOf(tc.argv); got != tc.want {
			t.Fatalf("%s: FanoutOf=%d want %d", tc.argv[0], got, tc.want)
		}
	}
}

func TestGroupBySlot(t *testing.T) {
	keys := [][]byte{[]byte("alpha"), []byte("zzzzzzzz"), []byte("beta")}
	groups := GroupBySlot(keys, splitSlot)
	if len(groups) != 2 {
		t.Fatalf("groups=%d want 2", len(groups))
	}
	if groups[0].Slot != 1 || len(groups[0].Index) != 2 || groups[0].Index[0] != 0 || groups[0].Index[1] != 2 {
		t.Fatalf("group0 %+v", groups[0])
	}
	if groups[1].Slot != 2 || len(groups[1].Index) != 1 || groups[1].Index[0] != 1 {
		t.Fatalf("group1 %+v", groups[1])
	}
}

func TestRebuildFanoutArgv(t *testing.T) {
	del := argvOf("DEL", "alpha", "zzzzzzzz", "beta")
	groups := GroupBySlot(ExtractKeys(del), splitSlot)
	got := RebuildFanoutArgv(del, groups[0])
	want := argvOf("DEL", "alpha", "beta")
	assertArgv(t, got, want)
	got = RebuildFanoutArgv(del, groups[1])
	assertArgv(t, got, argvOf("DEL", "zzzzzzzz"))

	mset := argvOf("MSET", "alpha", "v1", "zzzzzzzz", "v2")
	mg := GroupBySlot(ExtractKeys(mset), splitSlot)
	got = RebuildFanoutArgv(mset, mg[0])
	assertArgv(t, got, argvOf("MSET", "alpha", "v1"))
	got = RebuildFanoutArgv(mset, mg[1])
	assertArgv(t, got, argvOf("MSET", "zzzzzzzz", "v2"))
}

func TestMergeSum(t *testing.T) {
	got := MergeSum([]resp.Value{resp.Integer(1), resp.Integer(2)})
	if got.Type != resp.TypeInteger || got.Int != 3 {
		t.Fatalf("sum=%v want 3", got)
	}
	err := resp.Error("ERR boom")
	got = MergeSum([]resp.Value{resp.Integer(1), err})
	if !got.IsError() {
		t.Fatal("expected error")
	}
}

func TestMergeMGetOrder(t *testing.T) {
	// alpha (slot 1) then zzzzzzzz (slot 2) must stay [v1, v2] regardless of group order.
	groups := []KeyGroup{
		{Slot: 2, Index: []int{1}},
		{Slot: 1, Index: []int{0}},
	}
	replies := []resp.Value{
		resp.Array(resp.BulkString([]byte("v2"))),
		resp.Array(resp.BulkString([]byte("v1"))),
	}
	got := MergeMGet(2, groups, replies)
	if got.Type != resp.TypeArray || len(got.Values) != 2 {
		t.Fatalf("reply %+v", got)
	}
	if string(got.Values[0].Str) != "v1" || string(got.Values[1].Str) != "v2" {
		t.Fatalf("order %q %q", got.Values[0].Str, got.Values[1].Str)
	}
}

func TestMergeMSet(t *testing.T) {
	got := MergeMSet([]resp.Value{resp.SimpleString("OK"), resp.SimpleString("OK")})
	if got.Type != resp.TypeSimpleString || string(got.Str) != "OK" {
		t.Fatalf("got %+v", got)
	}
	got = MergeMSet([]resp.Value{resp.SimpleString("OK"), resp.Error("ERR no")})
	if !got.IsError() {
		t.Fatal("expected error")
	}
}

func assertArgv(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len %d want %d (%q vs %q)", len(got), len(want), got, want)
	}
	for i := range got {
		if string(got[i]) != string(want[i]) {
			t.Fatalf("argv[%d]=%s want %s", i, got[i], want[i])
		}
	}
}
