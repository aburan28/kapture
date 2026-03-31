package hub

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

// CaptureHubReconciler reconciles the singleton CaptureHub CR.
// It starts/stops the gRPC server based on the CaptureHub spec and
// updates the CR status with aggregated agent information.
type CaptureHubReconciler struct {
	client.Client
	Log logr.Logger

	mu     sync.Mutex
	server *Server
	cancel context.CancelFunc
}

// SetupWithManager registers the reconciler with the controller-runtime manager.
func (r *CaptureHubReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&capturev1alpha1.CaptureHub{}).
		Complete(r)
}

// Reconcile handles CaptureHub CR changes: starts/reconfigures the gRPC server
// and updates the status with aggregated agent data.
func (r *CaptureHubReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("capturehub", req.Name)

	var hub capturev1alpha1.CaptureHub
	if err := r.Get(ctx, req.NamespacedName, &hub); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// CaptureHub deleted — stop the gRPC server.
			log.Info("CaptureHub deleted, stopping gRPC server")
			r.stopServer()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Ensure the gRPC server is running with the configured address.
	if err := r.ensureServer(ctx, &hub); err != nil {
		log.Error(err, "failed to ensure gRPC server")
		return ctrl.Result{}, err
	}

	// Update CaptureHub status with aggregated agent information.
	if err := r.updateStatus(ctx, &hub); err != nil {
		log.Error(err, "failed to update CaptureHub status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// ensureServer starts or reconfigures the gRPC server.
func (r *CaptureHubReconciler) ensureServer(ctx context.Context, hub *capturev1alpha1.CaptureHub) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	grpcAddress := hub.Spec.GRPCAddress
	if grpcAddress == "" {
		grpcAddress = ":9443"
	}
	dashboardAddress := hub.Spec.DashboardAddress
	if dashboardAddress == "" {
		dashboardAddress = ":8082"
	}

	// If the server is already running on the right addresses, nothing to do.
	if r.server != nil && r.server.grpcAddress == grpcAddress && r.server.DashboardAddress() == dashboardAddress {
		return nil
	}

	// Stop existing server if the address changed.
	if r.server != nil {
		r.Log.Info("Stopping existing hub servers for reconfiguration")
		r.cancel()
		r.server.Stop()
		r.server = nil
		r.cancel = nil
	}

	r.Log.Info("Starting hub servers", "grpcAddress", grpcAddress, "dashboardAddress", dashboardAddress)
	r.server = NewServer(grpcAddress, dashboardAddress)

	serverCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go func() {
		if err := r.server.Start(serverCtx); err != nil {
			r.Log.Error(err, "gRPC server stopped with error")
		}
	}()

	return nil
}

// stopServer gracefully stops the gRPC server.
func (r *CaptureHubReconciler) stopServer() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.server != nil {
		r.cancel()
		r.server.Stop()
		r.server = nil
		r.cancel = nil
	}
}

// updateStatus writes aggregated agent data to the CaptureHub CR status.
func (r *CaptureHubReconciler) updateStatus(ctx context.Context, hub *capturev1alpha1.CaptureHub) error {
	r.mu.Lock()
	srv := r.server
	r.mu.Unlock()

	if srv == nil {
		return fmt.Errorf("gRPC server not running")
	}

	hub.Status.ConnectedAgents = srv.ConnectedAgentCount()
	hub.Status.ActiveCaptures = srv.ActiveCaptureCount()

	agentSnapshots := srv.AgentStatuses()
	hub.Status.Agents = make([]capturev1alpha1.CaptureHubAgentStatus, len(agentSnapshots))
	for i, ss := range agentSnapshots {
		t := metav1.NewTime(ss.LastHeartbeat)
		hub.Status.Agents[i] = capturev1alpha1.CaptureHubAgentStatus{
			Name:           ss.Name,
			LastHeartbeat:  &t,
			ActiveCaptures: ss.ActiveCaptures,
		}
	}

	return r.Status().Update(ctx, hub)
}

// GetServer returns the current gRPC server instance (for testing).
func (r *CaptureHubReconciler) GetServer() *Server {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.server
}
