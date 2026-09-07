package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/config"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/proxy"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/version"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "", "YAML config path (or DCRP_CONFIG)")
	pprofFlag := flag.Bool("pprof", false, "enable pprof HTTP endpoints")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		_, _ = os.Stdout.WriteString(version.Version + "\n")
		return 0
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("config", "error", err)
		return 1
	}
	if *pprofFlag {
		cfg.Pprof = true
	}

	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	router, err := cluster.NewRouter(ctx, cfg, log, m)
	if err != nil {
		log.Error("router", "error", err)
		return 1
	}
	defer router.Close()

	var bg sync.WaitGroup
	bg.Go(func() { router.RunRefreshLoop(ctx) })

	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsMux(m),
		ReadHeaderTimeout: 5 * time.Second,
	}
	bg.Go(func() {
		log.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "error", err)
		}
	})

	var pprofSrv *http.Server
	if cfg.Pprof {
		pprofSrv = &http.Server{
			Addr:              cfg.PprofAddr,
			Handler:           pprofMux(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		bg.Go(func() {
			log.Info("pprof listening", "addr", cfg.PprofAddr)
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("pprof server", "error", err)
			}
		})
	}

	srv := proxy.New(cfg, router, m, log)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			log.Error("proxy serve", "error", err)
			stop()
			return 1
		}
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	if pprofSrv != nil {
		_ = pprofSrv.Shutdown(shutdownCtx)
	}
	stop()
	bg.Wait()
	log.Info("shutdown complete", "version", version.Version)
	return 0
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func metricsMux(m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func pprofMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
