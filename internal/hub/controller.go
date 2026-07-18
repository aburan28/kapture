package hub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

	// DirectiveBufferSize overrides the per-spoke directive buffer on
	// servers this reconciler starts; zero keeps the default.
	DirectiveBufferSize int

	mu          sync.Mutex
	server      *Server
	cancel      context.CancelFunc
	tlsSecret   string // "namespace/name" of the TLS secret in use, "" = plaintext
	tlsMTLS     bool
	tlsDataHash string
	dynamicTLS  *DynamicServerTLS
}

// SetupWithManager registers the reconciler with the controller-runtime
// manager. The secret watch drives in-place certificate rotation: an
// update to the referenced TLS secret re-reconciles the CaptureHub.
func (r *CaptureHubReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&capturev1alpha1.CaptureHub{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.hubsReferencingSecret)).
		Complete(r)
}

// hubsReferencingSecret maps a Secret event to the CaptureHubs whose
// spec.tls.certSecretRef points at it.
func (r *CaptureHubReconciler) hubsReferencingSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var hubs capturev1alpha1.CaptureHubList
	if err := r.List(ctx, &hubs); err != nil {
		r.Log.Error(err, "failed to list CaptureHubs for secret mapping")
		return nil
	}
	var requests []reconcile.Request
	for i := range hubs.Items {
		hub := &hubs.Items[i]
		if hub.Spec.TLS == nil {
			continue
		}
		ref := hub.Spec.TLS.CertSecretRef
		if string(ref.Name) == obj.GetName() && hubSecretNamespace(ref) == obj.GetNamespace() {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: hub.Name},
			})
		}
	}
	return requests
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

	// Surface which CR the server follows: extra CaptureHubs get an
	// explicit NotAuthoritative condition instead of silently doing
	// nothing.
	for i := range hubs.Items {
		other := &hubs.Items[i]
		if other.Name == hub.Name {
			continue
		}
		if err := r.markNotAuthoritative(ctx, other, hub.Name); err != nil {
			log.Error(err, "failed to update non-authoritative CaptureHub status", "capturehub", other.Name)
		}
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
// the CaptureHub spec's certificate secret when configured. Certificate
// rotation within the same secret is applied in place, without restarting
// the listener or disconnecting spokes; only address or TLS-mode changes
// restart the server.
func (r *CaptureHubReconciler) ensureServer(ctx context.Context, hub *capturev1alpha1.CaptureHub) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	address := hub.Spec.GRPCAddress
	if address == "" {
		address = ":9443"
	}

	secretData, tlsSecret, err := r.loadTLSSecret(ctx, hub)
	if err != nil {
		return err
	}
	requireMTLS := hubRequiresMTLS(hub)
	dataHash := hashTLSData(secretData)

	if r.server != nil && r.server.address == address &&
		r.tlsSecret == tlsSecret && r.tlsMTLS == requireMTLS {
		// Listener configuration unchanged; rotate certificates in place
		// when the secret content changed.
		if r.dynamicTLS != nil && dataHash != r.tlsDataHash {
			if err := r.dynamicTLS.Update(secretData, requireMTLS); err != nil {
				return fmt.Errorf("rotate TLS from secret %s: %w", tlsSecret, err)
			}
			r.tlsDataHash = dataHash
			r.Log.Info("Rotated hub TLS certificates in place", "secret", tlsSecret)
		}
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

	if secretData != nil {
		dynamicTLS, err := NewDynamicServerTLS(secretData, requireMTLS)
		if err != nil {
			return fmt.Errorf("TLS secret %s: %w", tlsSecret, err)
		}
		r.Log.Info("Starting gRPC server with TLS", "address", address,
			"secret", tlsSecret, "mTLS", requireMTLS)
		r.server = NewServerWithTLS(address, dynamicTLS.Credentials())
		r.dynamicTLS = dynamicTLS
	} else {
		r.Log.Info("Starting gRPC server (plaintext)", "address", address)
		r.server = NewServer(address)
		r.dynamicTLS = nil
	}
	r.server.SetDirectiveBufferSize(r.DirectiveBufferSize)
	r.tlsSecret = tlsSecret
	r.tlsMTLS = requireMTLS
	r.tlsDataHash = dataHash

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

// loadTLSSecret fetches the CaptureHub's certificate secret data. Returns
// nil data for plaintext configurations.
func (r *CaptureHubReconciler) loadTLSSecret(ctx context.Context, hub *capturev1alpha1.CaptureHub) (map[string][]byte, string, error) {
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
	return secret.Data, key.String(), nil
}

// hashTLSData fingerprints the TLS-relevant secret keys so rotation is
// detected by content, not by resource version churn.
func hashTLSData(secretData map[string][]byte) string {
	if secretData == nil {
		return ""
	}
	h := sha256.New()
	for _, key := range []string{TLSCertKey, TLSKeyKey, TLSCAKey} {
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write(secretData[key])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
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

// markNotAuthoritative records on an extra CaptureHub that the singleton
// gRPC server ignores it in favour of the authoritative CR.
func (r *CaptureHubReconciler) markNotAuthoritative(ctx context.Context, hub *capturev1alpha1.CaptureHub, activeName string) error {
	changed := meta.SetStatusCondition(&hub.Status.Conditions, metav1.Condition{
		Type:               capturev1alpha1.CaptureHubConditionActive,
		Status:             metav1.ConditionFalse,
		Reason:             "NotAuthoritative",
		Message:            fmt.Sprintf("gRPC server follows CaptureHub %q (oldest wins); this resource is ignored", activeName),
		ObservedGeneration: hub.Generation,
	})
	if !changed {
		return nil
	}
	return r.Status().Update(ctx, hub)
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

	meta.SetStatusCondition(&hub.Status.Conditions, metav1.Condition{
		Type:               capturev1alpha1.CaptureHubConditionActive,
		Status:             metav1.ConditionTrue,
		Reason:             "Authoritative",
		Message:            "gRPC server follows this CaptureHub",
		ObservedGeneration: hub.Generation,
	})

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
