package command

import (
	"testing"
)

func TestExtractKeys(t *testing.T) {
	cases := []struct {
		argv [][]byte
		want []string
	}{
		{argvOf("GET", "a"), []string{"a"}},
		{argvOf("MGET", "a", "b", "c"), []string{"a", "b", "c"}},
		{argvOf("MSET", "a", "1", "b", "2"), []string{"a", "b"}},
		{argvOf("BLPOP", "k1", "k2", "0"), []string{"k1", "k2"}},
		{argvOf("EVAL", "return 1", "2", "ka", "kb", "arg"), []string{"ka", "kb"}},
		{argvOf("XREAD", "COUNT", "10", "STREAMS", "s1", "s2", "0-0", "0-0"), []string{"s1", "s2"}},
		{argvOf("PING"), nil},
		{argvOf("LMPOP", "2", "k1", "k2", "LEFT"), []string{"k1", "k2"}},
		{argvOf("ZUNIONSTORE", "dst", "2", "a", "b"), []string{"dst", "a", "b"}},
	}
	for _, tc := range cases {
		got := ExtractKeys(tc.argv)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %d keys want %d", tc.argv[0], len(got), len(tc.want))
		}
		for i := range got {
			if string(got[i]) != tc.want[i] {
				t.Fatalf("%s: key[%d]=%s want %s", tc.argv[0], i, got[i], tc.want[i])
			}
		}
	}
}

func TestIsBlocking(t *testing.T) {
	if !IsBlocking(argvOf("BLPOP", "k", "1")) {
		t.Fatal("BLPOP")
	}
	if IsBlocking(argvOf("XREAD", "STREAMS", "s", "0")) {
		t.Fatal("XREAD without BLOCK")
	}
	if !IsBlocking(argvOf("XREAD", "BLOCK", "1000", "STREAMS", "s", "0")) {
		t.Fatal("XREAD BLOCK")
	}
}

func argvOf(parts ...string) [][]byte {
	out := make([][]byte, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}
