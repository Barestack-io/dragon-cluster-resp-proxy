package command

import (
	"bytes"
	"strconv"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

// Kind classifies a Redis command for routing.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindRead
	KindWrite
	KindBlock
	KindPubSub
	KindTxn
	KindScript
	KindConnection
	KindAdmin
)

// KeySpec describes 1-based key positions in argv after the command name
// (Redis COMMAND INFO convention). Last=-1 means last argument.
type KeySpec struct {
	First int
	Last  int
	Step  int
}

// Meta is static command metadata.
type Meta struct {
	Name string
	Kind Kind
	Keys KeySpec
}

// Lookup returns metadata for an uppercase command name.
func Lookup(upperName []byte) (Meta, bool) {
	m, ok := table[string(upperName)]
	return m, ok
}

// NameUpper returns argv[0] uppercased into dst (reused buffer).
func NameUpper(dst, argv0 []byte) []byte {
	if cap(dst) < len(argv0) {
		dst = make([]byte, len(argv0))
	}
	return resp.UpperASCII(dst[:len(argv0)], argv0)
}

// ExtractKeys returns the keys in argv using the command table and special cases.
func ExtractKeys(argv [][]byte) [][]byte {
	idx := KeyIndexes(argv)
	if len(idx) == 0 {
		return nil
	}
	out := make([][]byte, len(idx))
	for i, j := range idx {
		out[i] = argv[j]
	}
	return out
}

// KeyIndexes returns 0-based argv indexes of keys (and key-like patterns).
func KeyIndexes(argv [][]byte) []int {
	if len(argv) == 0 {
		return nil
	}
	name := make([]byte, len(argv[0]))
	resp.UpperASCII(name, argv[0])
	switch string(name) {
	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO":
		return evalKeyIdx(argv)
	case "XREAD", "XREADGROUP":
		return xreadKeyIdx(argv)
	case "MIGRATE":
		return migrateKeyIdx(argv)
	case "LMPOP", "ZMPOP", "SINTERCARD":
		return numkeysIdx(argv, 1)
	case "BLMPOP", "BZMPOP":
		return numkeysIdx(argv, 2)
	case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
		return destNumkeysIdx(argv)
	case "ZUNION", "ZINTER", "ZDIFF":
		return numkeysIdx(argv, 1)
	case "MEMORY":
		if len(argv) >= 3 && resp.EqualFoldASCII(argv[1], []byte("USAGE")) {
			return []int{2}
		}
		return nil
	case "KEYS":
		if len(argv) >= 2 {
			return []int{1}
		}
		return nil
	case "SCAN":
		return scanMatchIdx(argv)
	case "SORT":
		return sortKeyIdx(argv)
	case "GEORADIUS", "GEORADIUSBYMEMBER":
		idx := indexesBySpec(argv, table[string(name)].Keys)
		return append(idx, storeOptionIdx(argv)...)
	}
	m, ok := table[string(name)]
	if !ok || m.Keys.First == 0 {
		return nil
	}
	return indexesBySpec(argv, m.Keys)
}

func indexesBySpec(argv [][]byte, spec KeySpec) []int {
	if spec.First <= 0 {
		return nil
	}
	step := spec.Step
	if step <= 0 {
		step = 1
	}
	first := spec.First
	last := spec.Last
	if last == 0 {
		last = first
	}
	if last < 0 {
		last = len(argv) + last
	}
	if first >= len(argv) || last < first {
		return nil
	}
	if last >= len(argv) {
		last = len(argv) - 1
	}
	n := ((last - first) / step) + 1
	if n <= 0 {
		return nil
	}
	out := make([]int, 0, n)
	for i := first; i <= last; i += step {
		out = append(out, i)
	}
	return out
}

func evalKeyIdx(argv [][]byte) []int {
	// EVAL script numkeys [key ...] [arg ...]
	if len(argv) < 3 {
		return nil
	}
	n, err := strconv.Atoi(string(argv[2]))
	if err != nil || n < 0 {
		return nil
	}
	end := 3 + n
	if end > len(argv) {
		end = len(argv)
	}
	if 3 >= end {
		return nil
	}
	out := make([]int, 0, end-3)
	for i := 3; i < end; i++ {
		out = append(out, i)
	}
	return out
}

func xreadKeyIdx(argv [][]byte) []int {
	// XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
	for i := 1; i < len(argv); i++ {
		if resp.EqualFoldASCII(argv[i], []byte("STREAMS")) {
			rest := argv[i+1:]
			if len(rest) < 2 || len(rest)%2 != 0 {
				return nil
			}
			n := len(rest) / 2
			out := make([]int, n)
			for j := 0; j < n; j++ {
				out[j] = i + 1 + j
			}
			return out
		}
	}
	return nil
}

func migrateKeyIdx(argv [][]byte) []int {
	// MIGRATE host port key|"" dest-db timeout [KEYS k...]
	if len(argv) < 6 {
		return nil
	}
	for i := 6; i < len(argv); i++ {
		if resp.EqualFoldASCII(argv[i], []byte("KEYS")) {
			if i+1 >= len(argv) {
				return nil
			}
			out := make([]int, 0, len(argv)-i-1)
			for j := i + 1; j < len(argv); j++ {
				out = append(out, j)
			}
			return out
		}
	}
	if len(argv[3]) > 0 {
		return []int{3}
	}
	return nil
}

func numkeysIdx(argv [][]byte, numkeysAt int) []int {
	if numkeysAt >= len(argv) {
		return nil
	}
	n, err := strconv.Atoi(string(argv[numkeysAt]))
	if err != nil || n < 0 {
		return nil
	}
	start := numkeysAt + 1
	end := start + n
	if start >= len(argv) {
		return nil
	}
	if end > len(argv) {
		end = len(argv)
	}
	out := make([]int, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, i)
	}
	return out
}

func destNumkeysIdx(argv [][]byte) []int {
	// DEST numkeys key [key ...]
	if len(argv) < 3 {
		if len(argv) >= 2 {
			return []int{1}
		}
		return nil
	}
	out := []int{1}
	out = append(out, numkeysIdx(argv, 2)...)
	return out
}

func scanMatchIdx(argv [][]byte) []int {
	for i := 2; i < len(argv)-1; i++ {
		if resp.EqualFoldASCII(argv[i], []byte("MATCH")) {
			return []int{i + 1}
		}
	}
	return nil
}

func sortKeyIdx(argv [][]byte) []int {
	var out []int
	if len(argv) >= 2 {
		out = append(out, 1)
	}
	for i := 2; i < len(argv)-1; i++ {
		if resp.EqualFoldASCII(argv[i], []byte("BY")) {
			if !resp.EqualFoldASCII(argv[i+1], []byte("nosort")) {
				out = append(out, i+1)
			}
			i++
			continue
		}
		if resp.EqualFoldASCII(argv[i], []byte("GET")) {
			if !bytes.Equal(argv[i+1], []byte("#")) {
				out = append(out, i+1)
			}
			i++
			continue
		}
		if resp.EqualFoldASCII(argv[i], []byte("STORE")) {
			out = append(out, i+1)
			i++
		}
	}
	return out
}

func storeOptionIdx(argv [][]byte) []int {
	var out []int
	for i := 1; i < len(argv)-1; i++ {
		if resp.EqualFoldASCII(argv[i], []byte("STORE")) || resp.EqualFoldASCII(argv[i], []byte("STOREDIST")) {
			out = append(out, i+1)
		}
	}
	return out
}

// IsBlocking reports whether argv is a blocking command (including XREAD BLOCK).
func IsBlocking(argv [][]byte) bool {
	if len(argv) == 0 {
		return false
	}
	name := make([]byte, len(argv[0]))
	resp.UpperASCII(name, argv[0])
	if m, ok := table[string(name)]; ok && m.Kind == KindBlock {
		return true
	}
	switch string(name) {
	case "XREAD", "XREADGROUP":
		for i := 1; i < len(argv); i++ {
			if resp.EqualFoldASCII(argv[i], []byte("BLOCK")) {
				return true
			}
		}
	}
	return false
}

// PubSubOp classifies pub/sub commands.
type PubSubOp uint8

const (
	PubSubNone PubSubOp = iota
	PubSubSubscribe
	PubSubUnsubscribe
	PubSubPSubscribe
	PubSubPUnsubscribe
	PubSubSSubscribe
	PubSubSUnsubscribe
	PubSubPublish
	PubSubSPublish
	PubSubPubSub
)

// ClassifyPubSub returns the pub/sub operation for argv[0].
func ClassifyPubSub(argv0 []byte) PubSubOp {
	name := make([]byte, len(argv0))
	resp.UpperASCII(name, argv0)
	switch string(name) {
	case "SUBSCRIBE":
		return PubSubSubscribe
	case "UNSUBSCRIBE":
		return PubSubUnsubscribe
	case "PSUBSCRIBE":
		return PubSubPSubscribe
	case "PUNSUBSCRIBE":
		return PubSubPUnsubscribe
	case "SSUBSCRIBE":
		return PubSubSSubscribe
	case "SUNSUBSCRIBE":
		return PubSubSUnsubscribe
	case "PUBLISH":
		return PubSubPublish
	case "SPUBLISH":
		return PubSubSPublish
	case "PUBSUB":
		return PubSubPubSub
	default:
		return PubSubNone
	}
}

// KindOf returns the kind for argv[0].
func KindOf(argv0 []byte) Kind {
	name := make([]byte, len(argv0))
	resp.UpperASCII(name, argv0)
	if m, ok := table[string(name)]; ok {
		return m.Kind
	}
	return KindUnknown
}
