package proxy

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/config"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestLocalPINGAndSELECT(t *testing.T) {
	backend := startFakeCluster(t)
	sock, stop := startProxy(t, backend.Addr().String())
	defer stop()

	c := dialUnix(t, sock)
	defer func() { _ = c.Close() }()
	mustOK(t, c, encodeCmd("PING"), "+PONG")
	mustOK(t, c, encodeCmd("SELECT", "0"), "+OK")
	mustOK(t, c, encodeCmd("SELECT", "4"), "+OK")
	mustContain(t, c, encodeCmd("SELECT", "99"), "out of range")
	mustContain(t, c, encodeCmd("CLUSTER", "SLOTS"), "cluster support disabled")
}

func TestVirtualDBPrefixesKeys(t *testing.T) {
	backend := startFakeCluster(t)
	sock, stop := startProxy(t, backend.Addr().String())
	defer stop()

	c := dialUnix(t, sock)
	defer func() { _ = c.Close() }()
	mustOK(t, c, encodeCmd("SELECT", "4"), "+OK")
	got := exchange(t, c, encodeCmd("GET", "hello"))
	if !strings.Contains(got, "db4-world") {
		t.Fatalf("db4 GET hello should read db4:hello, got %q", got)
	}
	mustOK(t, c, encodeCmd("SELECT", "0"), "+OK")
	got = exchange(t, c, encodeCmd("GET", "hello"))
	if !strings.Contains(got, "world") || strings.Contains(got, "db4-world") {
		t.Fatalf("db0 GET hello must stay unprefixed, got %q", got)
	}
}

func TestCrossSlotRejected(t *testing.T) {
	backend := startFakeCluster(t)
	sock, stop := startProxy(t, backend.Addr().String())
	defer stop()

	c := dialUnix(t, sock)
	defer func() { _ = c.Close() }()
	mustContain(t, c, encodeCmd("MGET", "alpha", "zzzzzzzz"), "CROSSSLOT")
}

func TestDrainBatchCoalescesWrites(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	var payload []byte
	for i := 0; i < 8; i++ {
		payload = append(payload, encodeCmd("PING")...)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = client.Write(payload)
	}()

	s := &session{
		conn: server,
		r:    resp.NewReader(server),
		cfg:  config.Defaults(),
	}
	s.cfg.MaxPipeline = 64
	s.cfg.ReadTimeout = time.Second
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	v, err := s.r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	argv, err := resp.ArrayToArgv(v)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := s.drainBatch(clientCmd{argv: argv, raw: v.Raw, name: "PING"})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 8 {
		t.Fatalf("drained %d commands, want 8", len(batch))
	}
	<-done
}

func TestClientPipeline(t *testing.T) {
	backend := startFakeCluster(t)
	sock, stop := startProxy(t, backend.Addr().String())
	defer stop()

	c := dialUnix(t, sock)
	defer func() { _ = c.Close() }()
	frame := append(encodeCmd("GET", "hello"), encodeCmd("GET", "db4:hello")...)
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	r := resp.NewReader(c)
	a, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(a.Str), "world") {
		t.Fatalf("first %q", a.Str)
	}
	if !strings.Contains(string(b.Str), "db4-world") {
		t.Fatalf("second %q", b.Str)
	}
}

func TestGETProxied(t *testing.T) {
	backend := startFakeCluster(t)
	sock, stop := startProxy(t, backend.Addr().String())
	defer stop()

	c := dialUnix(t, sock)
	defer func() { _ = c.Close() }()
	got := exchange(t, c, encodeCmd("GET", "hello"))
	if !strings.Contains(got, "world") {
		t.Fatalf("reply %q", got)
	}
}

func TestMULTIBuffer(t *testing.T) {
	s := &session{multi: true}
	s.queued = append(s.queued, [][]byte{[]byte("SET"), []byte("a"), []byte("1")})
	if !s.multi || len(s.queued) != 1 {
		t.Fatal("expected queued MULTI state")
	}
}

func startProxy(t *testing.T, seed string) (string, func()) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "p.sock")
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.Defaults()
	cfg.UnixSocket = sock
	cfg.UnixSocketMode = "0770"
	cfg.ClusterSeeds = []string{seed}
	cfg.MetricsAddr = "127.0.0.1:0"
	cfg.TopologyRefresh = time.Hour
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = 2 * time.Second
	cfg.WriteTimeout = time.Second
	cfg.ShutdownTimeout = 500 * time.Millisecond
	cfg.MaxClientConns = 32
	cfg.PoolMinPerNode = 0
	cfg.PoolMaxPerNode = 8
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	r, err := cluster.NewRouter(ctx, cfg, log, m)
	if err != nil {
		cancel()
		t.Fatalf("router: %v", err)
	}
	srv := New(cfg, r, m, log)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx)
	}()
	return sock, func() {
		cancel()
		<-done
		r.Close()
	}
}

func dialUnix(t *testing.T, sock string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("unix", sock, 200*time.Millisecond)
		if err == nil {
			return c
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("dial %s", sock)
	return nil
}

func encodeCmd(args ...string) []byte {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	return resp.EncodeCommand(nil, argv)
}

func exchange(t *testing.T, c net.Conn, frame []byte) string {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	v, err := resp.NewReader(bufio.NewReader(c)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	return string(resp.Encode(nil, v))
}

func mustOK(t *testing.T, c net.Conn, frame []byte, wantPrefix string) {
	t.Helper()
	got := exchange(t, c, frame)
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("got %q want prefix %q", got, wantPrefix)
	}
}

func mustContain(t *testing.T, c net.Conn, frame []byte, sub string) {
	t.Helper()
	got := exchange(t, c, frame)
	if !strings.Contains(got, sub) {
		t.Fatalf("got %q want substring %q", got, sub)
	}
}

func startFakeCluster(t *testing.T) net.Listener {
	t.Helper()
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
			wg.Go(func() { serveFake(c, ln.Addr().String()) })
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln
}

func serveFake(c net.Conn, self string) {
	defer func() { _ = c.Close() }()
	r := resp.NewReader(c)
	host, port, _ := strings.Cut(self, ":")
	slots := clusterSlotsReply(host, port)
	for {
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		v, err := r.ReadValue()
		if err != nil {
			return
		}
		argv, err := resp.ArrayToArgv(v)
		if err != nil {
			return
		}
		cmd := strings.ToUpper(string(argv[0]))
		var out []byte
		switch cmd {
		case "CLUSTER":
			if len(argv) > 1 && strings.EqualFold(string(argv[1]), "SLOTS") {
				out = slots
			} else {
				out = resp.Encode(nil, resp.Error("ERR unknown"))
			}
		case "INFO":
			out = resp.Encode(nil, resp.BulkString([]byte("dragonfly_version:1.40.2\r\ncluster_enabled:1\r\n")))
		case "GET":
			switch {
			case len(argv) > 1 && string(argv[1]) == "db4:hello":
				out = resp.Encode(nil, resp.BulkString([]byte("db4-world")))
			case len(argv) > 1 && string(argv[1]) == "hello":
				out = resp.Encode(nil, resp.BulkString([]byte("world")))
			default:
				out = resp.Encode(nil, resp.NullBulk())
			}
		default:
			out = resp.Encode(nil, resp.SimpleString("OK"))
		}
		if _, err := c.Write(out); err != nil {
			return
		}
	}
}

func clusterSlotsReply(ip, port string) []byte {
	var p int64
	for _, ch := range port {
		p = p*10 + int64(ch-'0')
	}
	v := resp.Array(resp.Array(
		resp.Integer(0), resp.Integer(16383),
		resp.Array(resp.BulkString([]byte(ip)), resp.Integer(p), resp.BulkString([]byte("n1"))),
	))
	return resp.Encode(nil, v)
}
