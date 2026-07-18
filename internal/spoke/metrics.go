package spoke

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// metricHubRPCErrors counts failed spoke→hub RPCs by RPC name. A rising
// rate on one spoke means that spoke cannot reach the hub; captures and
// replays keep running, but status aggregation and directives stall.
var metricHubRPCErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "kapture_spoke_hub_rpc_errors_total",
	Help: "Failed spoke-to-hub RPCs.",
}, []string{"rpc"})

func init() {
	ctrlmetrics.Registry.MustRegister(metricHubRPCErrors)
}
