// Package metrics exposes concise Prometheus operational metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry     *prometheus.Registry
	crawls       *prometheus.CounterVec
	materialized *prometheus.CounterVec
	failed       *prometheus.CounterVec
	deleted      *prometheus.CounterVec
	events       *prometheus.CounterVec
}

func New() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry(), crawls: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hronir_crawls_total", Help: "Completed Connected Systems crawls."}, []string{"source", "kind", "complete"}), materialized: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hronir_records_materialized_total", Help: "Records written to the target."}, []string{"source", "kind"}), failed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hronir_resource_failures_total", Help: "Resources that failed to map or write."}, []string{"source", "kind"}), deleted: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hronir_records_deleted_total", Help: "Records deleted after a verified absence."}, []string{"source", "kind"}), events: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "hronir_resource_events_total", Help: "Accepted CloudEvents."}, []string{"source", "kind"})}
	m.registry.MustRegister(m.crawls, m.materialized, m.failed, m.deleted, m.events)
	return m
}
func (m *Metrics) Crawl(source, kind string, complete bool) {
	value := "false"
	if complete {
		value = "true"
	}
	m.crawls.WithLabelValues(source, kind, value).Inc()
}
func (m *Metrics) Materialized(source, kind string) {
	m.materialized.WithLabelValues(source, kind).Inc()
}
func (m *Metrics) Failed(source, kind string)  { m.failed.WithLabelValues(source, kind).Inc() }
func (m *Metrics) Deleted(source, kind string) { m.deleted.WithLabelValues(source, kind).Inc() }
func (m *Metrics) Event(source, kind string)   { m.events.WithLabelValues(source, kind).Inc() }
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
