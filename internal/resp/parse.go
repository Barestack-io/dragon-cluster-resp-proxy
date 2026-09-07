package resp

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"sync"
)

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

func getBuf() *[]byte {
	return bufPool.Get().(*[]byte)
}

func putBuf(b *[]byte) {
	if cap(*b) > 64*1024 {
		return
	}
	*b = (*b)[:0]
	bufPool.Put(b)
}

// Reader reads RESP values from a buffered stream.
type Reader struct {
	br *bufio.Reader
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader {
	return NewReaderSize(r, 32*1024)
}

// NewReaderSize wraps r with a bufio buffer of at least size bytes.
func NewReaderSize(r io.Reader, size int) *Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return &Reader{br: br}
	}
	if size < 4096 {
		size = 4096
	}
	return &Reader{br: bufio.NewReaderSize(r, size)}
}

// Buffered returns the underlying bufio.Reader.
func (r *Reader) Buffered() *bufio.Reader {
	return r.br
}

// BufferedLen is the number of bytes already in the reader (no syscall).
func (r *Reader) BufferedLen() int {
	return r.br.Buffered()
}

// ReadValue reads one complete RESP value, capturing raw wire bytes.
func (r *Reader) ReadValue() (Value, error) {
	raw := getBuf()
	v, err := r.readValue(raw)
	if err != nil {
		putBuf(raw)
		return Value{}, err
	}
	v.Raw = append([]byte(nil), (*raw)...)
	putBuf(raw)
	return v, nil
}

func (r *Reader) readValue(raw *[]byte) (Value, error) {
	line, err := r.readLine(raw)
	if err != nil {
		return Value{}, err
	}
	if len(line) < 1 {
		return Value{}, ErrProtocol
	}
	switch Type(line[0]) {
	case TypeSimpleString:
		return Value{Type: TypeSimpleString, Str: clone(line[1:])}, nil
	case TypeError:
		return Value{Type: TypeError, Str: clone(line[1:])}, nil
	case TypeInteger:
		n, err := parseInt(line[1:])
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeInteger, Int: n}, nil
	case TypeBulkString, TypeBlobError, TypeVerbatim:
		return r.readBulk(Type(line[0]), line[1:], raw)
	case TypeArray, TypePush, TypeSet:
		return r.readAggregate(Type(line[0]), line[1:], raw, false)
	case TypeMap:
		return r.readAggregate(TypeMap, line[1:], raw, true)
	case TypeNull:
		return Value{Type: TypeNull, Null: true}, nil
	case TypeBoolean:
		v := Value{Type: TypeBoolean}
		if len(line) > 1 && (line[1] == 't' || line[1] == 'T') {
			v.Int = 1
		}
		return v, nil
	case TypeDouble, TypeBigNumber:
		return Value{Type: Type(line[0]), Str: clone(line[1:])}, nil
	case TypeAttribute:
		// Attributes prefix the next value; skip the map then return the value.
		if _, err := r.readAggregate(TypeAttribute, line[1:], raw, true); err != nil {
			return Value{}, err
		}
		return r.readValue(raw)
	default:
		return Value{}, ErrProtocol
	}
}

func (r *Reader) readBulk(t Type, lenField []byte, raw *[]byte) (Value, error) {
	n, err := parseInt(lenField)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{Type: t, Null: true}, nil
	}
	if n > 512*1024*1024 {
		return Value{}, ErrProtocol
	}
	need := int(n) + 2
	buf := make([]byte, need)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return Value{}, err
	}
	*raw = append(*raw, buf...)
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return Value{}, ErrProtocol
	}
	return Value{Type: t, Str: buf[:n]}, nil
}

func (r *Reader) readAggregate(t Type, lenField []byte, raw *[]byte, isMap bool) (Value, error) {
	n, err := parseInt(lenField)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{Type: t, Null: true}, nil
	}
	count := int(n)
	if isMap {
		count *= 2
	}
	if count > 1_000_000 {
		return Value{}, ErrProtocol
	}
	items := make([]Value, count)
	for i := 0; i < count; i++ {
		items[i], err = r.readValue(raw)
		if err != nil {
			return Value{}, err
		}
	}
	return Value{Type: t, Values: items}, nil
}

func (r *Reader) readLine(raw *[]byte) ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		var full []byte
		full = append(full, line...)
		rest, err := r.br.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		full = append(full, rest...)
		line = full
	} else if err != nil {
		return nil, err
	}
	*raw = append(*raw, line...)
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrProtocol
	}
	return line[:len(line)-2], nil
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, ErrProtocol
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, ErrProtocol
	}
	return n, nil
}

func clone(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ArrayToArgv extracts bulk/simple string elements from a command array.
func ArrayToArgv(v Value) ([][]byte, error) {
	if v.Type != TypeArray || v.Null {
		return nil, ErrProtocol
	}
	argv := make([][]byte, len(v.Values))
	for i, el := range v.Values {
		switch el.Type {
		case TypeBulkString, TypeSimpleString:
			argv[i] = el.Str
		default:
			return nil, ErrProtocol
		}
	}
	return argv, nil
}

// EqualFoldASCII reports whether a and b are equal ignoring ASCII case.
func EqualFoldASCII(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// UpperASCII copies b to dst in-place uppercasing ASCII letters. dst must be
// at least len(b). Returns the uppercased slice of dst.
func UpperASCII(dst, b []byte) []byte {
	n := len(b)
	for i := 0; i < n; i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		dst[i] = c
	}
	return dst[:n]
}

// HasPrefixFold reports whether s starts with prefix, ASCII fold.
func HasPrefixFold(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	return EqualFoldASCII(s[:len(prefix)], prefix)
}

// BytesContainsToken reports whether s starts with token followed by space or EOS.
func BytesContainsToken(s, token []byte) bool {
	if !bytes.HasPrefix(s, token) {
		return false
	}
	if len(s) == len(token) {
		return true
	}
	return s[len(token)] == ' '
}
