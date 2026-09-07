package resp

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestRoundTripCommand(t *testing.T) {
	cmd := EncodeCommand(nil, [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	v, err := NewReader(bytes.NewReader(cmd)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	argv, err := ArrayToArgv(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(argv) != 3 || string(argv[0]) != "SET" || string(argv[2]) != "v" {
		t.Fatalf("argv=%q", argv)
	}
	if !bytes.Equal(v.Raw, cmd) {
		t.Fatalf("raw mismatch\n%s\n%s", v.Raw, cmd)
	}
}

func TestErrorKindMOVED(t *testing.T) {
	raw := []byte("-MOVED 3999 127.0.0.1:7001\r\n")
	v, err := NewReader(bytes.NewReader(raw)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if !v.EqualKind("MOVED") {
		t.Fatalf("kind=%q", v.ErrorKind())
	}
}

func TestNullBulk(t *testing.T) {
	v, err := NewReader(strings.NewReader("$-1\r\n")).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if !v.Null || v.Type != TypeBulkString {
		t.Fatalf("%+v", v)
	}
}

func TestIncomplete(t *testing.T) {
	_, err := NewReader(strings.NewReader("*2\r\n$3\r\nGET")).ReadValue()
	if err == nil {
		t.Fatal("expected error")
	}
	if err != io.ErrUnexpectedEOF && err != io.EOF && err != ErrProtocol {
		// ReadFull returns UnexpectedEOF
		if !strings.Contains(err.Error(), "EOF") {
			t.Fatalf("err=%v", err)
		}
	}
}

func TestEncodeSimple(t *testing.T) {
	got := Encode(nil, SimpleString("OK"))
	if string(got) != "+OK\r\n" {
		t.Fatalf("%q", got)
	}
	got = Encode(nil, Error("ERR nope"))
	if string(got) != "-ERR nope\r\n" {
		t.Fatalf("%q", got)
	}
}

func BenchmarkParseCommand(b *testing.B) {
	cmd := EncodeCommand(nil, [][]byte{[]byte("GET"), []byte("user:{42}:name")})
	b.ReportAllocs()
	for b.Loop() {
		v, err := NewReader(bytes.NewReader(cmd)).ReadValue()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := ArrayToArgv(v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeCommand(b *testing.B) {
	argv := [][]byte{[]byte("SET"), []byte("k"), []byte("v")}
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		buf = EncodeCommand(buf[:0], argv)
	}
}
