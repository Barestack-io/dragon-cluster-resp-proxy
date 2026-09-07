//go:build integration

package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/config"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/proxy"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

func TestDragonflyClusterProxy(t *testing.T) {
	if os.Getenv("DCRP_INTEGRATION") == "" {
		t.Skip("set DCRP_INTEGRATION=1 and start test/compose")
	}
	a := envOr("DCRP_DFLY_A", "127.0.0.1:16379")
	b := envOr("DCRP_DFLY_B", "127.0.0.1:16380")
	bootstrapDragonfly(t, a, b)

	sock, stop := startProxy(t, []string{a, b})
	defer stop()
	c := dialUnix(t, sock)
	defer c.Close()

	mustContain(t, c, cmd("PING"), "PONG")
	mustContain(t, c, cmd("SET", "user:{42}:name", "ada"), "OK")
	mustContain(t, c, cmd("GET", "user:{42}:name"), "ada")
	mustContain(t, c, cmd("MGET", "user:{42}:name", "other"), "CROSSSLOT")
	mustContain(t, c, cmd("PSUBSCRIBE", "x*"), "PSUBSCRIBE is not supported")
}

func TestRedisClusterComposeOptional(t *testing.T) {
	if os.Getenv("DCRP_REDIS_INTEGRATION") == "" {
		t.Skip("set DCRP_REDIS_INTEGRATION=1 after redis-cli --cluster create")
	}
	seed := envOr("DCRP_REDIS_SEED", "127.0.0.1:17000")
	sock, stop := startProxy(t, []string{seed})
	defer stop()
	c := dialUnix(t, sock)
	defer c.Close()
	mustContain(t, c, cmd("PING"), "PONG")
}

func bootstrapDragonfly(t *testing.T, a, b string) {
	t.Helper()
	idA := clusterMyID(t, a)
	idB := clusterMyID(t, b)
	hostA, portA, _ := strings.Cut(a, ":")
	hostB, portB, _ := strings.Cut(b, ":")
	cfg := fmt.Sprintf(`[
	  {"slot_ranges":[{"start":0,"end":8191}],"master":{"id":"%s","ip":"%s","port":%s},"replicas":[]},
	  {"slot_ranges":[{"start":8192,"end":16383}],"master":{"id":"%s","ip":"%s","port":%s},"replicas":[]}
	]`, idA, hostA, portA, idB, hostB, portB)
	for _, addr := range []string{a, b} {
		reply := redisDo(t, addr, [][]byte{[]byte("DFLYCLUSTER"), []byte("CONFIG"), []byte(cfg)})
		if strings.Contains(reply, "ERR") && !strings.Contains(reply, "OK") {
			t.Fatalf("config %s: %s", addr, reply)
		}
	}
}

func clusterMyID(t *testing.T, addr string) string {
	t.Helper()
	raw := redisDo(t, addr, [][]byte{[]byte("CLUSTER"), []byte("MYID")})
	v, err := resp.NewReader(strings.NewReader(raw)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(v.Str))
	if id == "" {
		t.Fatalf("empty MYID from %s: %q", addr, raw)
	}
	return id
}

func redisDo(t *testing.T, addr string, argv [][]byte) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write(resp.EncodeCommand(nil, argv)); err != nil {
		t.Fatal(err)
	}
	v, err := resp.NewReader(bufio.NewReader(c)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	return string(resp.Encode(nil, v))
}

func startProxy(t *testing.T, seeds []string) (string, func()) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "p.sock")
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.Defaults()
	cfg.UnixSocket = sock
	cfg.ClusterSeeds = seeds
	cfg.PoolMinPerNode = 0
	cfg.TopologyRefresh = time.Hour
	cfg.ShutdownTimeout = time.Second
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r, err := cluster.NewRouter(ctx, cfg, log, metrics.New())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv := proxy.New(cfg, r, metrics.New(), log)
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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("unix", sock, time.Second)
		if err == nil {
			return c
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dial %s", sock)
	return nil
}

func cmd(args ...string) []byte {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	return resp.EncodeCommand(nil, argv)
}

func mustContain(t *testing.T, c net.Conn, frame, sub string) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	v, err := resp.NewReader(bufio.NewReader(c)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	got := string(resp.Encode(nil, v))
	if !strings.Contains(got, sub) {
		t.Fatalf("got %q want %q", got, sub)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestComposeFileExists(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not required for this check")
	}
	_, err := os.Stat(filepath.Join("..", "compose", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
}
