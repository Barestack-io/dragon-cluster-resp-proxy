package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "dragon_cluster_resp_proxy"

// Metrics holds all Prometheus instruments.
type Metrics struct {
	reg *prometheus.Registry

	// rate(dragon_cluster_resp_proxy_commands_total[5m])
	// increase(dragon_cluster_resp_proxy_commands_total{result="error"}[5m])
	Commands *prometheus.CounterVec

	// histogram_quantile(0.99, sum(rate(dragon_cluster_resp_proxy_command_duration_seconds_bucket[5m])) by (le, command))
	Duration *prometheus.HistogramVec

	// rate(dragon_cluster_resp_proxy_redirects_total[5m])
	Redirects *prometheus.CounterVec

	// rate(dragon_cluster_resp_proxy_topology_refresh_total[5m])
	TopologyRefresh *prometheus.CounterVec

	// dragon_cluster_resp_proxy_client_connections
	ClientConns prometheus.Gauge

	// dragon_cluster_resp_proxy_pool_connections{state="idle|active|waiters"}
	Pool *prometheus.GaugeVec

	// dragon_cluster_resp_proxy_pubsub_subscribers
	PubSubSubs prometheus.Gauge

	// rate(dragon_cluster_resp_proxy_pubsub_rewrites_total[5m])
	PubSubRewrites prometheus.Counter

	// rate(dragon_cluster_resp_proxy_pubsub_resubscribe_total[5m])
	PubSubResub prometheus.Counter

	// rate(dragon_cluster_resp_proxy_backend_errors_total[5m])
	BackendErrors *prometheus.CounterVec
}

// New registers metrics on a private registry (plus Go runtime collectors).
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}
	m.Commands = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "commands_total",
		Help:      "Commands processed by result class.",
	}, []string{"command", "result"})
	m.Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "command_duration_seconds",
		Help:      "Command handling duration.",
		Buckets:   []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
	}, []string{"command"})
	m.Redirects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "redirects_total",
		Help:      "MOVED/ASK redirects followed internally.",
	}, []string{"kind"})
	m.TopologyRefresh = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "topology_refresh_total",
		Help:      "Topology refresh attempts.",
	}, []string{"result"})
	m.ClientConns = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "client_connections",
		Help:      "Current unix-socket client connections.",
	})
	m.Pool = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "pool_connections",
		Help:      "Backend pool sizes.",
	}, []string{"backend", "state"})
	m.PubSubSubs = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "pubsub_subscribers",
		Help:      "Client sessions in subscribe mode.",
	})
	m.PubSubRewrites = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "pubsub_rewrites_total",
		Help:      "PUBLISH/SUBSCRIBE rewritten to SPUBLISH/SSUBSCRIBE.",
	})
	m.PubSubResub = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "pubsub_resubscribe_total",
		Help:      "Resubscribes after slot migration.",
	})
	m.BackendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "backend_errors_total",
		Help:      "Backend errors by class.",
	}, []string{"class"})

	reg.MustRegister(
		m.Commands, m.Duration, m.Redirects, m.TopologyRefresh,
		m.ClientConns, m.Pool, m.PubSubSubs, m.PubSubRewrites, m.PubSubResub, m.BackendErrors,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

// ObserveCommand records one command.
func (m *Metrics) ObserveCommand(cmd, result string, d time.Duration) {
	if m == nil {
		return
	}
	if cmd == "" {
		cmd = "unknown"
	}
	m.Commands.WithLabelValues(cmd, result).Inc()
	m.Duration.WithLabelValues(cmd).Observe(d.Seconds())
}
