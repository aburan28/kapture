package hub

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Prometheus metrics for the hub control plane, registered with the
// controller-runtime registry so they ride the manager's /metrics
// endpoint.
var (
	metricConnectedSpokes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kapture_hub_connected_spokes",
		Help: "Spokes with a recent heartbeat.",
	})
	metricActiveCaptures = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kapture_hub_active_captures",
		Help: "Active TrafficCaptures reported by connected spokes.",
	})
	metricActiveReplays = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kapture_hub_active_replays",
		Help: "Active replay shards reported by connected spokes.",
	})
	metricHeartbeats = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kapture_hub_heartbeats_total",
		Help: "Heartbeat RPCs accepted from spokes.",
	})
	metricDirectivesQueued = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kapture_hub_directives_queued_total",
		Help: "Directives queued for delivery to spokes.",
	})
	metricDirectiveRejects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kapture_hub_directive_rejects_total",
		Help: "Directives rejected before delivery.",
	}, []string{"reason"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		metricConnectedSpokes,
		metricActiveCaptures,
		metricActiveReplays,
		metricHeartbeats,
		metricDirectivesQueued,
		metricDirectiveRejects,
	)
}
