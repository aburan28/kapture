package hub

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

// CaptureHubReconciler reconciles the singleton CaptureHub CR.
// It starts/stops the gRPC server based on the CaptureHub spec and
// updates the CR status with aggregated spoke information.
type CaptureHubReconciler struct {
	client.Client
	Log logr.Logger

	mu        sync.Mutex
	server    *Server
	cancel    context.CancelFunc
	tlsSecret string // "namespace/name" of the TLS secret in use, "" = plaintext
}

// SetupWithManager registers the reconciler with the controller-runtime manager.
func (r *CaptureHubReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&capturev1alpha1.CaptureHub{}).
		Complete(r)
}

// Reconcile handles CaptureHub CR changes: starts/reconfigures the gRPC server
// and updates the status with aggregated spoke data. The gRPC server is a
// process-wide singleton, so it always follows the authoritative CaptureHub
// (oldest by creation time) regardless of which CR triggered the reconcile —
// creating or deleting an extra CaptureHub must not retarget or stop a
// running hub.
func (r *CaptureHubReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("capturehub", req.Name)

	var hubs capturev1alpha1.CaptureHubList
	if err := r.List(ctx, &hubs); err != nil {
		return ctrl.Result{}, err
	}

	hub := authoritativeHub(hubs.Items)
	if hub == nil {
		log.Info("no CaptureHub resources remain, stopping gRPC server")
		r.stopServer()
		return ctrl.Result{}, nil
	}

	// Ensure the gRPC server is running with the configured address.
	if err := r.ensureServer(ctx, hub); err != nil {
		log.Error(err, "failed to ensure gRPC server")
		return ctrl.Result{}, err
	}

	// Update CaptureHub status with aggregated spoke information.
	if err := r.updateStatus(ctx, hub); err != nil {
		log.Error(err, "failed to update CaptureHub status")
		return ctrl.Result{}, err
	}

	// Spoke registrations, heartbeats, and replay reports change the
	// in-memory registry without touching the CR, so refresh the status
	// periodically rather than only on CR edits.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// authoritativeHub selects which CaptureHub the singleton gRPC server
// follows when several exist: the oldest by creation time, name as
// tiebreak.
func authoritativeHub(items []capturev1alpha1.CaptureHub) *capturev1alpha1.CaptureHub {
	var active *capturev1alpha1.CaptureHub
	for i := range items {
		h := &items[i]
		if active == nil ||
			h.CreationTimestamp.Time.Before(active.CreationTimestamp.Time) ||
			(h.CreationTimestamp.Time.Equal(active.CreationTimestamp.Time) && h.Name < active.Name) {
			active = h
		}
	}
	return active
}

// ensureServer starts or reconfigures the gRPC server, with TLS/mTLS from
// the CaptureHub spec's certificate secret when configured.
func (r *CaptureHubReconciler) ensureServer(ctx context.Context, hub *capturev1alpha1.CaptureHub) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	address := hub.Spec.GRPCAddress
	if address == "" {
		address = ":9443"
	}

	creds, tlsSecret, err := r.buildCredentials(ctx, hub)
	if err != nil {
		return err
	}

	// If the server is already running with the right address and TLS
	// source, nothing to do. Certificate *rotation* within the same
	// secret is not hot-reloaded yet; change the secret name to force a
	// server restart.
	if r.server != nil && r.server.address == address && r.tlsSecret == tlsSecret {
		return nil
	}

	// Stop existing server on reconfiguration.
	if r.server != nil {
		r.Log.Info("Stopping existing gRPC server for reconfiguration")
		r.cancel()
		r.server.Stop()
		r.server = nil
		r.cancel = nil
	}

	if creds != nil {
		r.Log.Info("Starting gRPC server with TLS", "address", address,
			"secret", tlsSecret, "mTLS", hubRequiresMTLS(hub))
		r.server = NewServerWithTLS(address, creds)
	} else {
		r.Log.Info("Starting gRPC server (plaintext)", "address", address)
		r.server = NewServer(address)
	}
	r.tlsSecret = tlsSecret

	serverCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go func() {
		if err := r.server.Start(serverCtx); err != nil {
			r.Log.Error(err, "gRPC server stopped with error")
		}
	}()

	return nil
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// buildCredentials loads TLS credentials from the CaptureHub's certificate
// secret. Returns nil credentials for plaintext configurations.
func (r *CaptureHubReconciler) buildCredentials(ctx context.Context, hub *capturev1alpha1.CaptureHub) (credentials.TransportCredentials, string, error) {
	if hub.Spec.TLS == nil {
		return nil, "", nil
	}

	ref := hub.Spec.TLS.CertSecretRef
	namespace := hubSecretNamespace(ref)

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: string(ref.Name)}
	if err := r.Get(ctx, key, &secret); err != nil {
		return nil, "", fmt.Errorf("load TLS secret %s: %w", key, err)
	}

	creds, err := BuildServerCredentials(secret.Data, hubRequiresMTLS(hub))
	if err != nil {
		return nil, "", fmt.Errorf("TLS secret %s: %w", key, err)
	}
	return creds, key.String(), nil
}

// hubSecretNamespace resolves the secret namespace for the cluster-scoped
// CaptureHub: explicit ref namespace, else the hub controller's own
// namespace (POD_NAMESPACE), else "default".
func hubSecretNamespace(ref gwapiv1.SecretObjectReference) string {
	if ref.Namespace != nil && *ref.Namespace != "" {
		return string(*ref.Namespace)
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return "default"
}

func hubRequiresMTLS(hub *capturev1alpha1.CaptureHub) bool {
	return hub.Spec.Authentication != nil &&
		hub.Spec.Authentication.Type == capturev1alpha1.HubAuthenticationTypeMTLS
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

// updateStatus writes aggregated spoke data to the CaptureHub CR status.
func (r *CaptureHubReconciler) updateStatus(ctx context.Context, hub *capturev1alpha1.CaptureHub) error {
	r.mu.Lock()
	srv := r.server
	r.mu.Unlock()

	if srv == nil {
		return fmt.Errorf("gRPC server not running")
	}

	// Publish the authoritative load-test list for heartbeat responses so
	// spokes can garbage-collect orphaned shards. A failed list keeps the
	// previous value and marks it incomplete rather than telling spokes
	// "nothing exists".
	var loadTests capturev1alpha1.CaptureLoadTestList
	if err := r.List(ctx, &loadTests); err != nil {
		r.Log.Error(err, "failed to list CaptureLoadTests for heartbeat GC hints")
		srv.SetActiveLoadTests(nil, false)
	} else {
		keys := make([]*hubv1.LoadTestKey, 0, len(loadTests.Items))
		for i := range loadTests.Items {
			keys = append(keys, &hubv1.LoadTestKey{
				Namespace: loadTests.Items[i].Namespace,
				Name:      loadTests.Items[i].Name,
			})
		}
		srv.SetActiveLoadTests(keys, true)
	}

	hub.Status.ConnectedSpokes = srv.ConnectedSpokeCount()
	hub.Status.ActiveCaptures = srv.ActiveCaptureCount()
	hub.Status.ActiveReplays = srv.ActiveReplayCount()

	spokeSnapshots := srv.SpokeStatuses()
	hub.Status.Spokes = make([]capturev1alpha1.CaptureHubSpokeStatus, len(spokeSnapshots))
	for i, ss := range spokeSnapshots {
		t := metav1.NewTime(ss.LastHeartbeat)
		hub.Status.Spokes[i] = capturev1alpha1.CaptureHubSpokeStatus{
			Name:           ss.Name,
			Cell:           ss.Cell,
			LastHeartbeat:  &t,
			ActiveCaptures: ss.ActiveCaptures,
			ActiveReplays:  ss.ActiveReplays,
		}
	}

	cellSnapshots := srv.CellStatuses()
	hub.Status.Cells = make([]capturev1alpha1.CaptureHubCellStatus, len(cellSnapshots))
	for i, cs := range cellSnapshots {
		hub.Status.Cells[i] = capturev1alpha1.CaptureHubCellStatus{
			Name:            cs.Name,
			ConnectedSpokes: cs.ConnectedSpokes,
			TotalSpokes:     cs.TotalSpokes,
			ActiveCaptures:  cs.ActiveCaptures,
			ActiveReplays:   cs.ActiveReplays,
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
