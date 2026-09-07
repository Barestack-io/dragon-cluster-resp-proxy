package pool

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func TestPoolGetPut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer func() { _ = c.Close() }()
				r := resp.NewReader(c)
				for {
					if _, err := r.ReadValue(); err != nil {
						return
					}
					_, _ = c.Write([]byte("+OK\r\n"))
				}
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	p := New(context.Background(), Options{
		Addr:         ln.Addr().String(),
		MinIdle:      0,
		MaxSize:      2,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer p.Close()

	ctx := context.Background()
	c, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Do([][]byte{[]byte("PING")}, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(v.Str), "OK") && v.Type != resp.TypeSimpleString {
		t.Fatalf("%+v", v)
	}
	p.Put(c)
	idle, _, _ := p.Stats()
	if idle < 1 {
		t.Fatalf("idle=%d", idle)
	}
}

func TestPutAfterClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 64)
		_, _ = c.Read(buf)
	}()
	t.Cleanup(func() { _ = ln.Close() })

	p := New(context.Background(), Options{
		Addr:        ln.Addr().String(),
		MaxSize:     2,
		DialTimeout: time.Second,
	})
	c, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Put(c) // must not panic
}
