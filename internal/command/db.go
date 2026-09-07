package command

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

// DBPrefix returns the on-wire key prefix for a virtual database.
// DB 0 is unprefixed so existing cluster data stays reachable.
func DBPrefix(db int) []byte {
	if db <= 0 {
		return nil
	}
	buf := make([]byte, 0, 8)
	buf = append(buf, 'd', 'b')
	buf = strconv.AppendInt(buf, int64(db), 10)
	return append(buf, ':')
}

// HasVirtualDBPrefix reports whether key starts with db<digits>: (N>0).
func HasVirtualDBPrefix(key []byte) bool {
	if !bytes.HasPrefix(key, []byte("db")) {
		return false
	}
	i := 2
	if i >= len(key) || key[i] < '0' || key[i] > '9' {
		return false
	}
	for i < len(key) && key[i] >= '0' && key[i] <= '9' {
		i++
	}
	return i > 2 && i < len(key) && key[i] == ':'
}

// PrefixKey prepends the virtual-DB prefix. DB 0 is a no-op.
func PrefixKey(db int, key []byte) []byte {
	p := DBPrefix(db)
	if len(p) == 0 {
		return key
	}
	out := make([]byte, 0, len(p)+len(key))
	out = append(out, p...)
	return append(out, key...)
}

// StripKey removes the virtual-DB prefix if present.
func StripKey(db int, key []byte) []byte {
	p := DBPrefix(db)
	if len(p) == 0 || !bytes.HasPrefix(key, p) {
		return key
	}
	return key[len(p):]
}

// ParseDBIndex parses a SELECT/MOVE/COPY DB argument.
func ParseDBIndex(b []byte, max int) (int, error) {
	if max <= 0 {
		max = 16
	}
	n, err := strconv.Atoi(string(b))
	if err != nil || n < 0 {
		return 0, errInvalidDB
	}
	if n >= max {
		return 0, errDBRange
	}
	return n, nil
}

var (
	errInvalidDB = errors.New("invalid DB index")
	errDBRange   = errors.New("DB index is out of range")
)

// InvalidDBError is the client-facing SELECT/MOVE DB parse error.
func InvalidDBError() resp.Value {
	return resp.Error("ERR invalid DB index")
}

// DBRangeError is the client-facing out-of-range DB error.
func DBRangeError() resp.Value {
	return resp.Error("ERR DB index is out of range")
}

// DBIndexError maps ParseDBIndex errors to RESP errors.
func DBIndexError(err error) resp.Value {
	if err == nil {
		return resp.Value{}
	}
	if err == errDBRange {
		return DBRangeError()
	}
	return InvalidDBError()
}

// NeedsDBRewrite reports whether argv contains keys (or key patterns) that
// must be prefixed when the session is not on DB 0.
func NeedsDBRewrite(argv [][]byte) bool {
	if len(argv) == 0 {
		return false
	}
	name := make([]byte, len(argv[0]))
	resp.UpperASCII(name, argv[0])
	switch string(name) {
	case "PING", "ECHO", "AUTH", "QUIT", "SELECT", "HELLO", "RESET",
		"MULTI", "EXEC", "DISCARD", "UNWATCH",
		"CLUSTER", "INFO", "COMMAND", "CONFIG", "TIME", "ROLE", "LOLWUT",
		"CLIENT", "READONLY", "READWRITE", "REPLICAOF", "SLAVEOF", "WAIT",
		"LATENCY", "FLUSHALL", "SWAPDB":
		return false
	case "PUBLISH", "SPUBLISH", "SUBSCRIBE", "SSUBSCRIBE",
		"UNSUBSCRIBE", "SUNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBSUB":
		return false
	default:
		return true
	}
}

// RewriteArgv clones argv and prefixes keys/patterns for virtual db.
// DB 0 returns argv unchanged.
func RewriteArgv(argv [][]byte, db int) [][]byte {
	if db <= 0 || !NeedsDBRewrite(argv) {
		return argv
	}
	name := make([]byte, len(argv[0]))
	resp.UpperASCII(name, argv[0])
	switch string(name) {
	case "COPY":
		return rewriteCOPY(argv, db)
	case "SCAN":
		return rewriteSCAN(argv, db)
	case "KEYS":
		return rewriteKEYS(argv, db)
	case "SORT":
		return rewriteSORT(argv, db)
	case "MOVE":
		return rewriteMOVE(argv, db)
	}
	out := cloneArgv(argv)
	for _, i := range KeyIndexes(out) {
		if i > 0 && i < len(out) {
			out[i] = PrefixKey(db, out[i])
		}
	}
	return out
}

func rewriteCOPY(argv [][]byte, db int) [][]byte {
	if len(argv) < 3 {
		return cloneArgv(argv)
	}
	destDB := db
	replace := false
	for i := 3; i < len(argv); i++ {
		if resp.EqualFoldASCII(argv[i], []byte("REPLACE")) {
			replace = true
			continue
		}
		if resp.EqualFoldASCII(argv[i], []byte("DB")) && i+1 < len(argv) {
			if n, err := strconv.Atoi(string(argv[i+1])); err == nil && n >= 0 {
				destDB = n
			}
			i++
		}
	}
	out := [][]byte{cloneBytes(argv[0]), PrefixKey(db, argv[1]), PrefixKey(destDB, argv[2])}
	if replace {
		out = append(out, []byte("REPLACE"))
	}
	return out
}

func rewriteSCAN(argv [][]byte, db int) [][]byte {
	out := cloneArgv(argv)
	p := DBPrefix(db)
	if len(p) == 0 {
		return out
	}
	for i := 2; i < len(out)-1; i++ {
		if resp.EqualFoldASCII(out[i], []byte("MATCH")) {
			out[i+1] = PrefixKey(db, out[i+1])
			return out
		}
	}
	out = append(out, []byte("MATCH"), append(p, '*'))
	return out
}

func rewriteKEYS(argv [][]byte, db int) [][]byte {
	out := cloneArgv(argv)
	if len(out) >= 2 {
		out[1] = PrefixKey(db, out[1])
	}
	return out
}

func rewriteSORT(argv [][]byte, db int) [][]byte {
	out := cloneArgv(argv)
	if len(out) >= 2 {
		out[1] = PrefixKey(db, out[1])
	}
	for i := 2; i < len(out); i++ {
		if i+1 >= len(out) {
			break
		}
		if resp.EqualFoldASCII(out[i], []byte("BY")) {
			if !resp.EqualFoldASCII(out[i+1], []byte("nosort")) {
				out[i+1] = PrefixKey(db, out[i+1])
			}
			i++
			continue
		}
		if resp.EqualFoldASCII(out[i], []byte("GET")) {
			if !bytes.Equal(out[i+1], []byte("#")) {
				out[i+1] = PrefixKey(db, out[i+1])
			}
			i++
			continue
		}
		if resp.EqualFoldASCII(out[i], []byte("STORE")) {
			out[i+1] = PrefixKey(db, out[i+1])
			i++
		}
	}
	return out
}

func rewriteMOVE(argv [][]byte, db int) [][]byte {
	// MOVE is executed locally; still prefix the source key for callers that
	// want a rewritten form. Dest DB is argv[2] and is not a key.
	out := cloneArgv(argv)
	if len(out) >= 2 {
		out[1] = PrefixKey(db, out[1])
	}
	return out
}

// StripReplyKeys removes virtual-DB prefixes from replies that echo key names.
func StripReplyKeys(name string, db int, v resp.Value) resp.Value {
	if db <= 0 || v.IsError() || v.Null {
		return v
	}
	switch name {
	case "KEYS":
		return stripArrayOfKeys(db, v)
	case "SCAN":
		if v.Type == resp.TypeArray && len(v.Values) >= 2 {
			items := append([]resp.Value(nil), v.Values...)
			items[1] = stripArrayOfKeys(db, items[1])
			return resp.Array(items...)
		}
		return v
	case "RANDOMKEY":
		return stripBulk(db, v)
	case "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX", "LMPOP", "BLMPOP", "ZMPOP", "BZMPOP":
		return stripFirstArrayKey(db, v)
	case "XREAD", "XREADGROUP":
		return stripXRead(db, v)
	default:
		return v
	}
}

func stripBulk(db int, v resp.Value) resp.Value {
	if v.Null || (v.Type != resp.TypeBulkString && v.Type != resp.TypeSimpleString) {
		return v
	}
	return resp.BulkString(append([]byte(nil), StripKey(db, v.Str)...))
}

func stripArrayOfKeys(db int, v resp.Value) resp.Value {
	if v.Type != resp.TypeArray || v.Null {
		return v
	}
	items := make([]resp.Value, len(v.Values))
	for i := range v.Values {
		items[i] = stripBulk(db, v.Values[i])
	}
	return resp.Array(items...)
}

func stripFirstArrayKey(db int, v resp.Value) resp.Value {
	if v.Type != resp.TypeArray || v.Null || len(v.Values) == 0 {
		return v
	}
	items := append([]resp.Value(nil), v.Values...)
	items[0] = stripBulk(db, items[0])
	return resp.Array(items...)
}

func stripXRead(db int, v resp.Value) resp.Value {
	if v.Type != resp.TypeArray || v.Null {
		return v
	}
	items := make([]resp.Value, len(v.Values))
	for i, stream := range v.Values {
		if stream.Type == resp.TypeArray && len(stream.Values) >= 1 {
			inner := append([]resp.Value(nil), stream.Values...)
			inner[0] = stripBulk(db, inner[0])
			items[i] = resp.Array(inner...)
			continue
		}
		items[i] = stream
	}
	return resp.Array(items...)
}

func cloneArgv(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i, a := range in {
		out[i] = cloneBytes(a)
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
