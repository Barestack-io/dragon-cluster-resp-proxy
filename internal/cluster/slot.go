package cluster

// HashTag returns the Redis/Dragonfly hash-tag slice of key using open/close
// delimiters. Empty tags (e.g. "{}") use the full key.
func HashTag(key []byte, open, close byte) []byte {
	start := -1
	for i := 0; i < len(key); i++ {
		if key[i] == open {
			start = i
			break
		}
	}
	if start < 0 {
		return key
	}
	for i := start + 1; i < len(key); i++ {
		if key[i] == close {
			if i == start+1 {
				return key
			}
			return key[start+1 : i]
		}
	}
	return key
}

// KeySlot returns crc16(tag(key)) & 0x3FFF.
func KeySlot(key []byte, open, close byte) uint16 {
	return crc16(HashTag(key, open, close)) & slotMask
}

// Redirect is a parsed MOVED or ASK error.
type Redirect struct {
	Kind string // MOVED or ASK
	Slot uint16
	Addr string
}

// ParseRedirect parses "-MOVED 3999 127.0.0.1:7001" style payloads (no leading '-').
func ParseRedirect(payload []byte) (Redirect, bool) {
	// KIND slot addr
	i := 0
	for i < len(payload) && payload[i] != ' ' {
		i++
	}
	if i == 0 || i >= len(payload) {
		return Redirect{}, false
	}
	kind := string(payload[:i])
	if kind != "MOVED" && kind != "ASK" {
		return Redirect{}, false
	}
	i++
	j := i
	for j < len(payload) && payload[j] != ' ' {
		j++
	}
	if j <= i || j >= len(payload) {
		return Redirect{}, false
	}
	slot, ok := atoi16(payload[i:j])
	if !ok || slot >= SlotCount {
		return Redirect{}, false
	}
	addr := string(payload[j+1:])
	if len(addr) == 0 {
		return Redirect{}, false
	}
	return Redirect{Kind: kind, Slot: slot, Addr: addr}, true
}

func atoi16(b []byte) (uint16, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n int
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n >= SlotCount {
			return 0, false
		}
	}
	return uint16(n), true
}

// SplitHostPort splits addr using the last colon so IPv6 MOVED targets parse.
func SplitHostPort(addr string) (host, port string, ok bool) {
	i := lastColon(addr)
	if i <= 0 || i == len(addr)-1 {
		return "", "", false
	}
	return addr[:i], addr[i+1:], true
}

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}
