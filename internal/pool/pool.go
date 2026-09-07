package pool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

// Options configure a per-node pool.
type Options struct {
	Addr         string
	MinIdle      int
	MaxSize      int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	BackoffMin   time.Duration
	BackoffMax   time.Duration
	TLS          *tls.Config
	Username     string
	Password     string
	ReadOnly     bool // send READONLY after connect (Redis replicas)
	OnState      func(addr string, idle, active, waiters int)
}

// Conn is a pooled backend connection.
type Conn struct {
	nc net.Conn
	r  *resp.Reader
	w  []byte
	p  *Pool
}

// Pool is a bounded channel pool of backend connections.
type Pool struct {
	opts    Options
	conns   chan *Conn
	mu      sync.Mutex
	active  atomic.Int32
	waiters atomic.Int32
	closed  atomic.Bool
	dials   atomic.Uint64
}

// New creates a pool and optionally pre-fills MinIdle connections.
func New(ctx context.Context, opts Options) *Pool {
	if opts.MaxSize < 1 {
		opts.MaxSize = 1
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 3 * time.Second
	}
	p := &Pool{
		opts:  opts,
		conns: make(chan *Conn, opts.MaxSize),
	}
	n := opts.MinIdle
	if n > opts.MaxSize {
		n = opts.MaxSize
	}
	for i := 0; i < n; i++ {
		c, err := p.dial(ctx)
		if err != nil {
			break
		}
		p.conns <- c
	}
	return p
}

// Addr returns the pool target.
func (p *Pool) Addr() string { return p.opts.Addr }

// Stats returns idle/active/waiters.
func (p *Pool) Stats() (idle, active, waiters int) {
	return len(p.conns), int(p.active.Load()), int(p.waiters.Load())
}

// Get checks out a connection, dialing if needed.
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	if p.closed.Load() {
		return nil, errors.New("pool closed")
	}
	p.waiters.Add(1)
	defer p.waiters.Add(-1)

	select {
	case c := <-p.conns:
		if c != nil {
			p.active.Add(1)
			return c, nil
		}
	default:
	}

	if int(p.active.Load())+len(p.conns) < p.opts.MaxSize {
		c, err := p.dial(ctx)
		if err == nil {
			p.active.Add(1)
			return c, nil
		}
		// fall through to wait for an idle conn
		if ctx.Err() != nil {
			return nil, err
		}
	}

	select {
	case c := <-p.conns:
		if c == nil {
			return nil, errors.New("pool closed")
		}
		p.active.Add(1)
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put returns a healthy connection to the pool or closes it.
func (p *Pool) Put(c *Conn) {
	if c == nil {
		return
	}
	p.active.Add(-1)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		c.close()
		return
	}
	select {
	case p.conns <- c:
	default:
		c.close()
	}
}

// Drop closes c without returning it.
func (p *Pool) Drop(c *Conn) {
	if c == nil {
		return
	}
	p.active.Add(-1)
	c.close()
}

// Close drains and closes all connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Swap(true) {
		return
	}
	close(p.conns)
	for c := range p.conns {
		c.close()
	}
}

func (p *Pool) dial(ctx context.Context) (*Conn, error) {
	dctx, cancel := context.WithTimeout(ctx, p.opts.DialTimeout)
	defer cancel()
	var d net.Dialer
	network := "tcp"
	addr := p.opts.Addr
	nc, err := d.DialContext(dctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}
	if p.opts.TLS != nil {
		tconn := tls.Client(nc, p.opts.TLS)
		if err := tconn.HandshakeContext(dctx); err != nil {
			_ = nc.Close()
			return nil, err
		}
		nc = tconn
	}
	c := &Conn{
		nc: nc,
		r:  resp.NewReader(nc),
		w:  make([]byte, 0, 256),
		p:  p,
	}
	p.dials.Add(1)
	if p.opts.Password != "" {
		if err := c.auth(p.opts.Username, p.opts.Password, p.opts.WriteTimeout, p.opts.ReadTimeout); err != nil {
			c.close()
			return nil, err
		}
	}
	if p.opts.ReadOnly {
		if err := c.roundTrip([][]byte{[]byte("READONLY")}, p.opts.WriteTimeout, p.opts.ReadTimeout); err != nil {
			c.close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Conn) auth(user, pass string, wt, rt time.Duration) error {
	var argv [][]byte
	if user != "" {
		argv = [][]byte{[]byte("AUTH"), []byte(user), []byte(pass)}
	} else {
		argv = [][]byte{[]byte("AUTH"), []byte(pass)}
	}
	return c.roundTrip(argv, wt, rt)
}

func (c *Conn) roundTrip(argv [][]byte, wt, rt time.Duration) error {
	v, err := c.Do(argv, wt, rt)
	if err != nil {
		return err
	}
	if v.IsError() {
		return fmt.Errorf("backend: %s", v.Str)
	}
	return nil
}

// Do writes argv and reads one reply.
func (c *Conn) Do(argv [][]byte, writeTO, readTO time.Duration) (resp.Value, error) {
	c.w = resp.EncodeCommand(c.w[:0], argv)
	return c.DoRaw(c.w, writeTO, readTO)
}

// DoRaw writes a pre-encoded RESP frame and reads one reply.
func (c *Conn) DoRaw(frame []byte, writeTO, readTO time.Duration) (resp.Value, error) {
	if err := c.nc.SetWriteDeadline(time.Now().Add(writeTO)); err != nil {
		return resp.Value{}, err
	}
	if _, err := c.nc.Write(frame); err != nil {
		return resp.Value{}, err
	}
	if err := c.nc.SetReadDeadline(time.Now().Add(readTO)); err != nil {
		return resp.Value{}, err
	}
	return c.r.ReadValue()
}

// WriteRaw writes without reading.
func (c *Conn) WriteRaw(frame []byte, writeTO time.Duration) error {
	if err := c.nc.SetWriteDeadline(time.Now().Add(writeTO)); err != nil {
		return err
	}
	_, err := c.nc.Write(frame)
	return err
}

// Read reads one value.
func (c *Conn) Read(readTO time.Duration) (resp.Value, error) {
	if err := c.nc.SetReadDeadline(time.Now().Add(readTO)); err != nil {
		return resp.Value{}, err
	}
	return c.r.ReadValue()
}

// NetConn exposes the underlying connection (pubsub hijack).
func (c *Conn) NetConn() net.Conn { return c.nc }

// Reader exposes the RESP reader (pubsub).
func (c *Conn) Reader() *resp.Reader { return c.r }

func (c *Conn) close() {
	if c.nc != nil {
		_ = c.nc.Close()
	}
}

// Detach removes c from pool accounting so the caller owns the net.Conn.
func (c *Conn) Detach() {
	c.p = nil
}
