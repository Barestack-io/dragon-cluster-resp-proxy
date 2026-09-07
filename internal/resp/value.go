package resp

import (
	"bytes"
	"errors"
	"strconv"
)

// Type is a RESP type prefix.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
	TypeNull         Type = '_'
	TypeBoolean      Type = '#'
	TypeDouble       Type = ','
	TypeBigNumber    Type = '('
	TypeBlobError    Type = '!'
	TypeVerbatim     Type = '='
	TypeMap          Type = '%'
	TypeSet          Type = '~'
	TypePush         Type = '>'
	TypeAttribute    Type = '|'
)

// Sentinel errors for the parser.
var (
	ErrIncomplete = errors.New("resp: incomplete")
	ErrProtocol   = errors.New("resp: protocol error")
)

// Value is a parsed RESP value. Raw holds the original wire bytes when read
// from the network so replies can be forwarded without re-encoding.
type Value struct {
	Type   Type
	Str    []byte
	Int    int64
	Values []Value
	Raw    []byte
	Null   bool
}

// IsError reports whether v is a RESP error (simple or blob).
func (v Value) IsError() bool {
	return v.Type == TypeError || v.Type == TypeBlobError
}

// ErrorBytes returns the error payload without the leading '-'.
func (v Value) ErrorBytes() []byte {
	return v.Str
}

// ErrorKind returns the first token of an error (MOVED, ASK, ERR, CROSSSLOT, LOADING).
func (v Value) ErrorKind() []byte {
	if !v.IsError() {
		return nil
	}
	i := bytes.IndexByte(v.Str, ' ')
	if i < 0 {
		return v.Str
	}
	return v.Str[:i]
}

// EqualKind reports whether the error kind equals name (ASCII, case-sensitive).
func (v Value) EqualKind(name string) bool {
	return bytes.Equal(v.ErrorKind(), []byte(name))
}

// BulkString builds a bulk string value (not encoded until Encode).
func BulkString(s []byte) Value {
	if s == nil {
		return Value{Type: TypeBulkString, Null: true}
	}
	return Value{Type: TypeBulkString, Str: s}
}

// SimpleString builds a simple string.
func SimpleString(s string) Value {
	return Value{Type: TypeSimpleString, Str: []byte(s)}
}

// Error builds a simple error (payload without leading '-').
func Error(s string) Value {
	return Value{Type: TypeError, Str: []byte(s)}
}

// Integer builds an integer value.
func Integer(n int64) Value {
	return Value{Type: TypeInteger, Int: n}
}

// Array builds an array value.
func Array(items ...Value) Value {
	return Value{Type: TypeArray, Values: items}
}

// NullArray is a RESP2 null array.
func NullArray() Value {
	return Value{Type: TypeArray, Null: true}
}

// NullBulk is a RESP2 null bulk string.
func NullBulk() Value {
	return Value{Type: TypeBulkString, Null: true}
}

// CommandArray builds a RESP array of bulk strings from argv.
func CommandArray(argv [][]byte) Value {
	items := make([]Value, len(argv))
	for i, a := range argv {
		items[i] = BulkString(a)
	}
	return Array(items...)
}

// Encode appends the RESP encoding of v to buf and returns the result.
func Encode(buf []byte, v Value) []byte {
	if len(v.Raw) > 0 {
		return append(buf, v.Raw...)
	}
	switch v.Type {
	case TypeSimpleString:
		buf = append(buf, '+')
		buf = append(buf, v.Str...)
		return appendCRLF(buf)
	case TypeError:
		buf = append(buf, '-')
		buf = append(buf, v.Str...)
		return appendCRLF(buf)
	case TypeInteger:
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, v.Int, 10)
		return appendCRLF(buf)
	case TypeBulkString:
		if v.Null {
			return append(buf, '$', '-', '1', '\r', '\n')
		}
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(v.Str)), 10)
		buf = appendCRLF(buf)
		buf = append(buf, v.Str...)
		return appendCRLF(buf)
	case TypeArray, TypePush:
		prefix := byte(v.Type)
		if v.Null {
			return append(buf, prefix, '-', '1', '\r', '\n')
		}
		buf = append(buf, prefix)
		buf = strconv.AppendInt(buf, int64(len(v.Values)), 10)
		buf = appendCRLF(buf)
		for i := range v.Values {
			buf = Encode(buf, v.Values[i])
		}
		return buf
	case TypeNull:
		return append(buf, '_', '\r', '\n')
	case TypeBoolean:
		if v.Int != 0 {
			return append(buf, '#', 't', '\r', '\n')
		}
		return append(buf, '#', 'f', '\r', '\n')
	case TypeMap, TypeSet:
		buf = append(buf, byte(v.Type))
		n := len(v.Values)
		if v.Type == TypeMap {
			n /= 2
		}
		buf = strconv.AppendInt(buf, int64(n), 10)
		buf = appendCRLF(buf)
		for i := range v.Values {
			buf = Encode(buf, v.Values[i])
		}
		return buf
	default:
		buf = append(buf, byte(v.Type))
		buf = append(buf, v.Str...)
		return appendCRLF(buf)
	}
}

func appendCRLF(buf []byte) []byte {
	return append(buf, '\r', '\n')
}

// EncodeCommand appends a RESP array command for argv.
func EncodeCommand(buf []byte, argv [][]byte) []byte {
	return Encode(buf, CommandArray(argv))
}
