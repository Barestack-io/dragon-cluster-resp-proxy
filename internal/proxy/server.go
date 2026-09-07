package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/config"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
)

// Server accepts unix-socket clients and proxies them to the cluster.
type Server struct {
	cfg     config.Config
	router  *cluster.Router
	metrics *metrics.Metrics
	log     *slog.Logger

	ln     net.Listener
	sem    chan struct{}
	wg     sync.WaitGroup
	active atomic.Int32
}

// New constructs a Server.
func New(cfg config.Config, router *cluster.Router, m *metrics.Metrics, log *slog.Logger) *Server {
	return &Server{
		cfg:     cfg,
		router:  router,
		metrics: m,
		log:     log,
		sem:     make(chan struct{}, cfg.MaxClientConns),
	}
}

// Serve listens on the configured unix socket until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	if err := os.Remove(s.cfg.UnixSocket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", s.cfg.UnixSocket)
	if err != nil {
		return err
	}
	perm, err := s.cfg.UnixSocketPerm()
	if err != nil {
		_ = ln.Close()
		return err
	}
	if err := os.Chmod(s.cfg.UnixSocket, perm); err != nil {
		_ = ln.Close()
		return err
	}
	s.ln = ln
	s.log.InfoContext(ctx, "listening", "socket", s.cfg.UnixSocket)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				s.waitDrain()
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			s.waitDrain()
			return err
		}
		select {
		case s.sem <- struct{}{}:
		default:
			_, _ = c.Write(respErrBusy)
			_ = c.Close()
			continue
		}
		s.wg.Go(func() {
			defer func() { <-s.sem }()
			s.handleConn(ctx, c)
		})
	}
}

var respErrBusy = []byte("-ERR max client connections reached\r\n")

func (s *Server) handleConn(ctx context.Context, c net.Conn) {
	s.active.Add(1)
	if s.metrics != nil {
		s.metrics.ClientConns.Inc()
	}
	defer func() {
		s.active.Add(-1)
		if s.metrics != nil {
			s.metrics.ClientConns.Dec()
		}
	}()
	sess := newSession(s.cfg, s.router, s.metrics, s.log, c)
	sess.run(ctx)
}

func (s *Server) waitDrain() {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	t := time.NewTimer(s.cfg.ShutdownTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		s.log.Warn("shutdown timeout waiting for client sessions")
	}
}

// Close closes the listener if Serve is not running.
func (s *Server) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}
