package command

import "github.com/barestack/dragon-cluster-resp-proxy/internal/resp"

// KeyGroup is the set of keys (by index into the extracted key list) that share a slot.
type KeyGroup struct {
	Slot  uint16
	Index []int
}

// GroupBySlot partitions keys into first-seen slot order.
func GroupBySlot(keys [][]byte, slotFn func([]byte) uint16) []KeyGroup {
	if len(keys) == 0 {
		return nil
	}
	order := make([]uint16, 0, 2)
	bySlot := make(map[uint16][]int, 2)
	for i, k := range keys {
		s := slotFn(k)
		if _, ok := bySlot[s]; !ok {
			order = append(order, s)
		}
		bySlot[s] = append(bySlot[s], i)
	}
	out := make([]KeyGroup, len(order))
	for i, s := range order {
		out[i] = KeyGroup{Slot: s, Index: bySlot[s]}
	}
	return out
}

// RebuildFanoutArgv builds a same-slot command from a KeyGroup of ExtractKeys indexes.
func RebuildFanoutArgv(argv [][]byte, g KeyGroup) [][]byte {
	if len(argv) == 0 {
		return nil
	}
	idx := KeyIndexes(argv)
	out := make([][]byte, 0, 1+2*len(g.Index))
	out = append(out, argv[0])
	switch FanoutOf(argv) {
	case FanoutMSet:
		for _, ki := range g.Index {
			if ki < 0 || ki >= len(idx) {
				continue
			}
			ai := idx[ki]
			out = append(out, argv[ai])
			if ai+1 < len(argv) {
				out = append(out, argv[ai+1])
			}
		}
	default:
		for _, ki := range g.Index {
			if ki < 0 || ki >= len(idx) {
				continue
			}
			out = append(out, argv[idx[ki]])
		}
	}
	return out
}

// MergeSum adds integer replies (DEL / UNLINK / EXISTS / TOUCH).
func MergeSum(replies []resp.Value) resp.Value {
	var sum int64
	for _, r := range replies {
		if r.IsError() {
			return r
		}
		if r.Type != resp.TypeInteger {
			return resp.Error("ERR fan-out expected integer reply")
		}
		sum += r.Int
	}
	return resp.Integer(sum)
}

// MergeMSet returns OK if every group succeeded.
func MergeMSet(replies []resp.Value) resp.Value {
	for _, r := range replies {
		if r.IsError() {
			return r
		}
	}
	return resp.SimpleString("OK")
}

// MergeMGet rebuilds an MGET array in the original key order.
func MergeMGet(nKeys int, groups []KeyGroup, replies []resp.Value) resp.Value {
	if nKeys < 0 {
		nKeys = 0
	}
	out := make([]resp.Value, nKeys)
	for i := range out {
		out[i] = resp.NullBulk()
	}
	if len(groups) != len(replies) {
		return resp.Error("ERR fan-out MGET reply length mismatch")
	}
	for i, g := range groups {
		r := replies[i]
		if r.IsError() {
			return r
		}
		if r.Type != resp.TypeArray || r.Null || len(r.Values) != len(g.Index) {
			return resp.Error("ERR fan-out MGET reply length mismatch")
		}
		for j, ki := range g.Index {
			if ki >= 0 && ki < nKeys {
				out[ki] = r.Values[j]
			}
		}
	}
	return resp.Array(out...)
}
