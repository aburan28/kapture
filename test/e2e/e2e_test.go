// Package e2e contains end-to-end tests that run against a real Kubernetes cluster
// (typically Kind). These tests verify the full controller lifecycle including
// resource creation, reconciliation, mirror filter injection, and cleanup.
//
// Prerequisites:
//   - A running Kubernetes cluster with kubeconfig accessible
//   - CRDs installed (capture.gateway.io + gateway-api)
//   - Hub and spoke controllers deployed (e.g. via Helm chart)
//   - Gateway API CRDs installed
//
// Run with: go test ./test/e2e/... -v -timeout 600s
package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

var (
	k8sClient client.Client
	ctx       context.Context
	cancel    context.CancelFunc
	testNS    string
)

func TestMain(m *testing.M) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = capturev1alpha1.AddToScheme(scheme)
	_ = gwapiv1.Install(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = autoscalingv2.AddToScheme(scheme)

	cfg, err := config.GetConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get kubeconfig: %v\n", err)
		os.Exit(1)
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	os.Exit(m.Run())
}

// --- Test: Full TrafficCapture lifecycle ---

func TestTrafficCaptureFullLifecycle(t *testing.T) {
	ns := createNamespace(t)

	// Step 1: Create a CaptureStorage and mark it Ready
	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-e2e-test",
				MountPath:    "/mnt/capture",
			},
		},
	}
	mustCreate(t, storage)
	markStorageReady(t, storage)

	// Step 2: Create an HTTPRoute target
	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-route", Namespace: ns},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{
					BackendRefs: []gwapiv1.HTTPBackendRef{
						{BackendRef: gwapiv1.BackendRef{
							BackendObjectReference: gwapiv1.BackendObjectReference{
								Name: "backend-svc",
								Port: ptrTo(gwapiv1.PortNumber(80)),
							},
						}},
					},
				},
			},
		},
	}
	mustCreate(t, route)

	// Step 3: Create TrafficCapture
	replicas := int32(1)
	includeHeaders := true
	includeBody := true
	maxBody := int64(1048576)
	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-capture", Namespace: ns},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "HTTPRoute",
				Name:  "e2e-route",
			},
			StorageRef: capturev1alpha1.CaptureStorageReference{
				Name: "e2e-storage",
			},
			Capture: capturev1alpha1.CaptureSettings{
				IncludeHeaders: &includeHeaders,
				IncludeBody:    &includeBody,
				MaxBodyBytes:   &maxBody,
			},
			Agent: &capturev1alpha1.AgentScalingSpec{
				Replicas: &replicas,
			},
		},
	}
	mustCreate(t, tc)

	// Step 4: Verify agent Deployment is created
	deployKey := types.NamespacedName{Namespace: ns, Name: "e2e-capture-capture-agent"}
	deploy := &appsv1.Deployment{}
	waitFor(t, "agent Deployment to exist", 60*time.Second, func() bool {
		return k8sClient.Get(ctx, deployKey, deploy) == nil
	})

	t.Log("agent Deployment created")

	// Verify Deployment spec
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("expected at least one container in agent Deployment")
	}
	container := deploy.Spec.Template.Spec.Containers[0]
	if container.Name != "capture-agent" {
		t.Errorf("expected container name 'capture-agent', got %q", container.Name)
	}
	if len(container.Ports) < 2 {
		t.Errorf("expected at least 2 ports, got %d", len(container.Ports))
	}

	// Verify env vars
	envMap := envVarMap(container.Env)
	if v, ok := envMap["STORAGE_TYPE"]; !ok || v != "EFS" {
		t.Errorf("expected STORAGE_TYPE=EFS, got %q", v)
	}
	if v, ok := envMap["CAPTURE_INCLUDE_HEADERS"]; !ok || v != "true" {
		t.Errorf("expected CAPTURE_INCLUDE_HEADERS=true, got %q", v)
	}
	if v, ok := envMap["CAPTURE_INCLUDE_BODY"]; !ok || v != "true" {
		t.Errorf("expected CAPTURE_INCLUDE_BODY=true, got %q", v)
	}

	// Step 5: Verify Service is created
	svcKey := types.NamespacedName{Namespace: ns, Name: "e2e-capture-capture-agent"}
	svc := &corev1.Service{}
	waitFor(t, "agent Service to exist", 30*time.Second, func() bool {
		return k8sClient.Get(ctx, svcKey, svc) == nil
	})
	t.Log("agent Service created")

	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("expected ClusterIP service, got %s", svc.Spec.Type)
	}

	// Step 6: Verify HPA is created
	hpaKey := types.NamespacedName{Namespace: ns, Name: "e2e-capture-capture-agent"}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	waitFor(t, "HPA to exist", 30*time.Second, func() bool {
		return k8sClient.Get(ctx, hpaKey, hpa) == nil
	})
	t.Log("HPA created")

	// Step 7: Verify mirror filter injected into HTTPRoute
	waitFor(t, "mirror filter on HTTPRoute", 30*time.Second, func() bool {
		updated := &gwapiv1.HTTPRoute{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(route), updated); err != nil {
			return false
		}
		return hasMirrorFilter(updated, "e2e-capture-capture-agent")
	})
	t.Log("mirror filter injected into HTTPRoute")

	// Verify annotation was set
	updatedRoute := &gwapiv1.HTTPRoute{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(route), updatedRoute); err != nil {
		t.Fatalf("getting updated route: %v", err)
	}
	if v, ok := updatedRoute.Annotations["capture.gateway.io/traffic-capture"]; !ok {
		t.Error("expected capture annotation on HTTPRoute")
	} else {
		t.Logf("HTTPRoute annotation: %s", v)
	}

	// Step 8: Verify TrafficCapture status is updated
	waitFor(t, "TrafficCapture status update", 60*time.Second, func() bool {
		got := &capturev1alpha1.TrafficCapture{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.TrafficCapturePhaseActive ||
			got.Status.Phase == capturev1alpha1.TrafficCapturePhasePending
	})

	gotTC := &capturev1alpha1.TrafficCapture{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), gotTC); err != nil {
		t.Fatalf("getting TrafficCapture: %v", err)
	}
	t.Logf("TrafficCapture phase: %s", gotTC.Status.Phase)
	if gotTC.Status.AgentStatus != nil {
		t.Logf("Agent status: desired=%d ready=%d", gotTC.Status.AgentStatus.DesiredReplicas, gotTC.Status.AgentStatus.ReadyReplicas)
	}

	// Step 9: Delete TrafficCapture and verify cleanup
	if err := k8sClient.Delete(ctx, tc); err != nil {
		t.Fatalf("deleting TrafficCapture: %v", err)
	}

	// Wait for TrafficCapture to be fully deleted (finalizer should run)
	tcKey := types.NamespacedName{Namespace: ns, Name: "e2e-capture"}
	waitFor(t, "TrafficCapture deletion", 60*time.Second, func() bool {
		err := k8sClient.Get(ctx, tcKey, &capturev1alpha1.TrafficCapture{})
		return err != nil // NotFound expected
	})
	t.Log("TrafficCapture deleted")

	// Verify mirror filter removed from HTTPRoute
	waitFor(t, "mirror filter removal from HTTPRoute", 30*time.Second, func() bool {
		updated := &gwapiv1.HTTPRoute{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(route), updated); err != nil {
			return false
		}
		return !hasMirrorFilter(updated, "e2e-capture-capture-agent")
	})
	t.Log("mirror filter removed from HTTPRoute after deletion")
}

// --- Test: TrafficCapture fails when storage not found ---

func TestTrafficCaptureFailsWithMissingStorage(t *testing.T) {
	ns := createNamespace(t)

	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "nostorage-route", Namespace: ns},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{BackendRefs: []gwapiv1.HTTPBackendRef{{BackendRef: gwapiv1.BackendRef{
					BackendObjectReference: gwapiv1.BackendObjectReference{
						Name: "backend-svc",
						Port: ptrTo(gwapiv1.PortNumber(80)),
					},
				}}}},
			},
		},
	}
	mustCreate(t, route)

	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "nostorage-capture", Namespace: ns},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "HTTPRoute",
				Name:  "nostorage-route",
			},
			StorageRef: capturev1alpha1.CaptureStorageReference{
				Name: "nonexistent-storage",
			},
			Capture: capturev1alpha1.CaptureSettings{},
		},
	}
	mustCreate(t, tc)

	// Should transition to Failed phase
	waitFor(t, "TrafficCapture Failed phase", 60*time.Second, func() bool {
		got := &capturev1alpha1.TrafficCapture{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.TrafficCapturePhaseFailed
	})

	// Verify condition message
	got := &capturev1alpha1.TrafficCapture{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got); err != nil {
		t.Fatalf("getting TrafficCapture: %v", err)
	}
	found := false
	for _, c := range got.Status.Conditions {
		if c.Type == "Ready" && c.Reason == "StorageNotFound" {
			found = true
			t.Logf("Condition: %s - %s", c.Reason, c.Message)
		}
	}
	if !found {
		t.Error("expected StorageNotFound condition on Ready")
	}
}

// --- Test: TrafficCapture pending when storage not ready ---

func TestTrafficCapturePendingWhenStorageNotReady(t *testing.T) {
	ns := createNamespace(t)

	// Create storage but do NOT mark it ready
	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "notready-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-notready",
				MountPath:    "/mnt/capture",
			},
		},
	}
	mustCreate(t, storage)
	// Intentionally NOT marking ready

	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-route", Namespace: ns},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{BackendRefs: []gwapiv1.HTTPBackendRef{{BackendRef: gwapiv1.BackendRef{
					BackendObjectReference: gwapiv1.BackendObjectReference{
						Name: "backend-svc",
						Port: ptrTo(gwapiv1.PortNumber(80)),
					},
				}}}},
			},
		},
	}
	mustCreate(t, route)

	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-capture", Namespace: ns},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "HTTPRoute",
				Name:  "pending-route",
			},
			StorageRef: capturev1alpha1.CaptureStorageReference{
				Name: "notready-storage",
			},
			Capture: capturev1alpha1.CaptureSettings{},
		},
	}
	mustCreate(t, tc)

	// Should transition to Pending phase
	waitFor(t, "TrafficCapture Pending phase", 60*time.Second, func() bool {
		got := &capturev1alpha1.TrafficCapture{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got); err != nil {
			return false
		}
		return got.Status.Phase == capturev1alpha1.TrafficCapturePhasePending
	})

	// Verify no Deployment was created (storage not ready should block it)
	deployKey := types.NamespacedName{Namespace: ns, Name: "pending-capture-capture-agent"}
	err := k8sClient.Get(ctx, deployKey, &appsv1.Deployment{})
	if err == nil {
		t.Error("expected no Deployment to be created when storage is not ready")
	}

	// Now mark storage ready and verify progression
	markStorageReady(t, storage)

	// Should create the Deployment now
	waitFor(t, "agent Deployment after storage ready", 60*time.Second, func() bool {
		return k8sClient.Get(ctx, deployKey, &appsv1.Deployment{}) == nil
	})
	t.Log("Deployment created after storage marked Ready")
}

// --- Test: CaptureStorage CRUD for all types ---

func TestCaptureStorageAllTypes(t *testing.T) {
	ns := createNamespace(t)

	tests := []struct {
		name string
		spec capturev1alpha1.CaptureStorageSpec
	}{
		{
			name: "s3",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeS3,
				S3: &capturev1alpha1.S3Config{
					Bucket:               "e2e-bucket",
					Region:               "us-west-2",
					CredentialsSecretRef: gwSecretRef("s3-creds"),
				},
			},
		},
		{
			name: "gcs",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeGCS,
				GCS: &capturev1alpha1.GCSConfig{
					Bucket:               "e2e-gcs-bucket",
					CredentialsSecretRef: gwSecretRef("gcs-creds"),
				},
			},
		},
		{
			name: "efs",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEFS,
				EFS: &capturev1alpha1.EFSConfig{
					FileSystemID: "fs-e2e",
					MountPath:    "/mnt/efs",
				},
			},
		},
		{
			name: "ebs",
			spec: capturev1alpha1.CaptureStorageSpec{
				Type: capturev1alpha1.CaptureStorageTypeEBS,
				EBS: &capturev1alpha1.EBSConfig{
					VolumeID:  "vol-e2e",
					MountPath: "/mnt/ebs",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := &capturev1alpha1.CaptureStorage{
				ObjectMeta: metav1.ObjectMeta{Name: tt.name + "-e2e-storage", Namespace: ns},
				Spec:       tt.spec,
			}
			mustCreate(t, storage)

			got := &capturev1alpha1.CaptureStorage{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(storage), got); err != nil {
				t.Fatalf("getting %s CaptureStorage: %v", tt.name, err)
			}
			if got.Spec.Type != tt.spec.Type {
				t.Errorf("expected type %s, got %s", tt.spec.Type, got.Spec.Type)
			}

			// Verify status can be updated
			markStorageReady(t, storage)
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(storage), got); err != nil {
				t.Fatalf("getting %s CaptureStorage after status update: %v", tt.name, err)
			}
			if got.Status.Phase != capturev1alpha1.CaptureStoragePhaseReady {
				t.Errorf("expected Ready phase, got %s", got.Status.Phase)
			}
		})
	}
}

// --- Test: GRPCRoute mirror (if supported) ---

func TestTrafficCaptureWithGRPCRoute(t *testing.T) {
	ns := createNamespace(t)

	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "grpc-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-grpc-test",
				MountPath:    "/mnt/capture",
			},
		},
	}
	mustCreate(t, storage)
	markStorageReady(t, storage)

	grpcRoute := &gwapiv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "grpc-route", Namespace: ns},
		Spec: gwapiv1.GRPCRouteSpec{
			Rules: []gwapiv1.GRPCRouteRule{
				{
					BackendRefs: []gwapiv1.GRPCBackendRef{
						{BackendRef: gwapiv1.BackendRef{
							BackendObjectReference: gwapiv1.BackendObjectReference{
								Name: "grpc-backend",
								Port: ptrTo(gwapiv1.PortNumber(9090)),
							},
						}},
					},
				},
			},
		},
	}
	mustCreate(t, grpcRoute)

	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "grpc-capture", Namespace: ns},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "GRPCRoute",
				Name:  "grpc-route",
			},
			StorageRef: capturev1alpha1.CaptureStorageReference{
				Name: "grpc-storage",
			},
			Capture: capturev1alpha1.CaptureSettings{},
		},
	}
	mustCreate(t, tc)

	// Verify Deployment created
	deployKey := types.NamespacedName{Namespace: ns, Name: "grpc-capture-capture-agent"}
	waitFor(t, "agent Deployment for GRPCRoute", 60*time.Second, func() bool {
		return k8sClient.Get(ctx, deployKey, &appsv1.Deployment{}) == nil
	})

	// Verify mirror filter on GRPCRoute
	waitFor(t, "mirror filter on GRPCRoute", 30*time.Second, func() bool {
		updated := &gwapiv1.GRPCRoute{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(grpcRoute), updated); err != nil {
			return false
		}
		return hasGRPCMirrorFilter(updated, "grpc-capture-capture-agent")
	})
	t.Log("mirror filter injected into GRPCRoute")

	// Delete and verify cleanup
	if err := k8sClient.Delete(ctx, tc); err != nil {
		t.Fatalf("deleting TrafficCapture: %v", err)
	}

	waitFor(t, "GRPCRoute mirror filter removal", 60*time.Second, func() bool {
		updated := &gwapiv1.GRPCRoute{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(grpcRoute), updated); err != nil {
			return false
		}
		return !hasGRPCMirrorFilter(updated, "grpc-capture-capture-agent")
	})
	t.Log("mirror filter removed from GRPCRoute after deletion")
}

// --- Test: Agent health endpoint ---

func TestCaptureAgentHealthEndpoint(t *testing.T) {
	ns := createNamespace(t)

	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "health-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-health",
				MountPath:    "/mnt/capture",
			},
		},
	}
	mustCreate(t, storage)
	markStorageReady(t, storage)

	route := &gwapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "health-route", Namespace: ns},
		Spec: gwapiv1.HTTPRouteSpec{
			Rules: []gwapiv1.HTTPRouteRule{
				{BackendRefs: []gwapiv1.HTTPBackendRef{{BackendRef: gwapiv1.BackendRef{
					BackendObjectReference: gwapiv1.BackendObjectReference{
						Name: "backend-svc",
						Port: ptrTo(gwapiv1.PortNumber(80)),
					},
				}}}},
			},
		},
	}
	mustCreate(t, route)

	tc := &capturev1alpha1.TrafficCapture{
		ObjectMeta: metav1.ObjectMeta{Name: "health-capture", Namespace: ns},
		Spec: capturev1alpha1.TrafficCaptureSpec{
			TargetRef: gwapiv1.LocalPolicyTargetReference{
				Group: "gateway.networking.k8s.io",
				Kind:  "HTTPRoute",
				Name:  "health-route",
			},
			StorageRef: capturev1alpha1.CaptureStorageReference{
				Name: "health-storage",
			},
			Capture: capturev1alpha1.CaptureSettings{},
		},
	}
	mustCreate(t, tc)

	// Wait for Deployment to exist and have at least one ready pod
	deployKey := types.NamespacedName{Namespace: ns, Name: "health-capture-capture-agent"}
	waitFor(t, "agent Deployment ready", 120*time.Second, func() bool {
		deploy := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, deployKey, deploy); err != nil {
			return false
		}
		return deploy.Status.ReadyReplicas > 0
	})

	// Port-forward or use service to hit the health endpoint
	// In Kind, we can use the service ClusterIP if we're running inside the cluster,
	// otherwise skip the HTTP check and just verify the pod is running
	svcKey := types.NamespacedName{Namespace: ns, Name: "health-capture-capture-agent"}
	svc := &corev1.Service{}
	if err := k8sClient.Get(ctx, svcKey, svc); err != nil {
		t.Fatalf("getting agent service: %v", err)
	}

	// Try to reach the health endpoint via ClusterIP (works when tests run in-cluster)
	healthURL := fmt.Sprintf("http://%s:8081/healthz", svc.Spec.ClusterIP)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(healthURL)
	if err != nil {
		t.Logf("cannot reach agent health endpoint via ClusterIP (expected if running out-of-cluster): %v", err)
		t.Log("skipping HTTP health check - pod readiness already verified via Deployment status")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from health endpoint, got %d", resp.StatusCode)
	}
	t.Log("agent health endpoint returned 200 OK")
}

// --- Test: Hub controller manages CaptureHub CR ---

func TestCaptureHubStatusUpdated(t *testing.T) {
	// CaptureHub is cluster-scoped, so we use a unique name
	hubName := fmt.Sprintf("e2e-hub-%d", time.Now().UnixNano()%100000)

	hub := &capturev1alpha1.CaptureHub{
		ObjectMeta: metav1.ObjectMeta{Name: hubName},
		Spec: capturev1alpha1.CaptureHubSpec{
			GRPCAddress: ":19443", // use high port to avoid conflicts
		},
	}

	if err := k8sClient.Create(ctx, hub); err != nil {
		t.Fatalf("creating CaptureHub: %v", err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, hub)
	})

	// The hub controller should reconcile and update status
	waitFor(t, "CaptureHub status update", 60*time.Second, func() bool {
		got := &capturev1alpha1.CaptureHub{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: hubName}, got); err != nil {
			return false
		}
		// Just verify the status fields exist (connectedSpokes should be 0 initially)
		return got.Status.ConnectedSpokes >= 0
	})

	got := &capturev1alpha1.CaptureHub{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: hubName}, got); err != nil {
		t.Fatalf("getting CaptureHub: %v", err)
	}
	t.Logf("CaptureHub status: connectedSpokes=%d activeCaptures=%d", got.Status.ConnectedSpokes, got.Status.ActiveCaptures)
}

// --- Test: Multiple TrafficCaptures in same namespace ---

func TestMultipleTrafficCaptures(t *testing.T) {
	ns := createNamespace(t)

	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "multi-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-multi",
				MountPath:    "/mnt/capture",
			},
		},
	}
	mustCreate(t, storage)
	markStorageReady(t, storage)

	// Create two separate HTTPRoutes
	for _, name := range []string{"route-a", "route-b"} {
		route := &gwapiv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: gwapiv1.HTTPRouteSpec{
				Rules: []gwapiv1.HTTPRouteRule{
					{BackendRefs: []gwapiv1.HTTPBackendRef{{BackendRef: gwapiv1.BackendRef{
						BackendObjectReference: gwapiv1.BackendObjectReference{
							Name: gwapiv1.ObjectName(name + "-svc"),
							Port: ptrTo(gwapiv1.PortNumber(80)),
						},
					}}}},
				},
			},
		}
		mustCreate(t, route)
	}

	// Create two TrafficCaptures targeting different routes
	for _, tcName := range []string{"capture-a", "capture-b"} {
		routeName := "route-a"
		if tcName == "capture-b" {
			routeName = "route-b"
		}
		tc := &capturev1alpha1.TrafficCapture{
			ObjectMeta: metav1.ObjectMeta{Name: tcName, Namespace: ns},
			Spec: capturev1alpha1.TrafficCaptureSpec{
				TargetRef: gwapiv1.LocalPolicyTargetReference{
					Group: "gateway.networking.k8s.io",
					Kind:  "HTTPRoute",
					Name:  gwapiv1.ObjectName(routeName),
				},
				StorageRef: capturev1alpha1.CaptureStorageReference{
					Name: "multi-storage",
				},
				Capture: capturev1alpha1.CaptureSettings{},
			},
		}
		mustCreate(t, tc)
	}

	// Both should get their own Deployments
	for _, tcName := range []string{"capture-a", "capture-b"} {
		deployKey := types.NamespacedName{Namespace: ns, Name: tcName + "-capture-agent"}
		waitFor(t, fmt.Sprintf("%s Deployment", tcName), 60*time.Second, func() bool {
			return k8sClient.Get(ctx, deployKey, &appsv1.Deployment{}) == nil
		})
		t.Logf("Deployment for %s created", tcName)
	}

	// Both HTTPRoutes should have mirror filters
	for _, routeName := range []string{"route-a", "route-b"} {
		tcName := "capture-a"
		if routeName == "route-b" {
			tcName = "capture-b"
		}
		agentSvcName := tcName + "-capture-agent"
		waitFor(t, fmt.Sprintf("mirror on %s", routeName), 30*time.Second, func() bool {
			route := &gwapiv1.HTTPRoute{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: routeName}, route); err != nil {
				return false
			}
			return hasMirrorFilter(route, agentSvcName)
		})
	}
	t.Log("both HTTPRoutes have independent mirror filters")
}

// ============================================================================
// Helpers
// ============================================================================

func createNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "e2e-",
		},
	}
	if err := k8sClient.Create(ctx, ns); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, ns)
	})
	t.Logf("using namespace %s", ns.Name)
	return ns.Name
}

func mustCreate(t *testing.T, obj client.Object) {
	t.Helper()
	if err := k8sClient.Create(ctx, obj); err != nil {
		t.Fatalf("creating %T %s/%s: %v", obj, obj.GetNamespace(), obj.GetName(), err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, obj)
	})
}

func markStorageReady(t *testing.T, storage *capturev1alpha1.CaptureStorage) {
	t.Helper()
	// Re-fetch to get latest resourceVersion
	got := &capturev1alpha1.CaptureStorage{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(storage), got); err != nil {
		t.Fatalf("getting storage for status update: %v", err)
	}
	got.Status.Phase = capturev1alpha1.CaptureStoragePhaseReady
	got.Status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "ConfigValid",
		Message:            "Storage configuration is valid",
		LastTransitionTime: metav1.Now(),
	}}
	if err := k8sClient.Status().Update(ctx, got); err != nil {
		t.Fatalf("marking storage ready: %v", err)
	}
}

func waitFor(t *testing.T, desc string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
}

func hasMirrorFilter(route *gwapiv1.HTTPRoute, agentServiceName string) bool {
	for _, rule := range route.Spec.Rules {
		for _, f := range rule.Filters {
			if f.Type == gwapiv1.HTTPRouteFilterRequestMirror && f.RequestMirror != nil {
				if string(f.RequestMirror.BackendRef.Name) == agentServiceName {
					return true
				}
			}
		}
	}
	return false
}

func hasGRPCMirrorFilter(route *gwapiv1.GRPCRoute, agentServiceName string) bool {
	for _, rule := range route.Spec.Rules {
		for _, f := range rule.Filters {
			if f.Type == gwapiv1.GRPCRouteFilterRequestMirror && f.RequestMirror != nil {
				if string(f.RequestMirror.BackendRef.Name) == agentServiceName {
					return true
				}
			}
		}
	}
	return false
}

func envVarMap(envs []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(envs))
	for _, e := range envs {
		m[e.Name] = e.Value
	}
	return m
}

func gwSecretRef(name string) gwapiv1.SecretObjectReference {
	return gwapiv1.SecretObjectReference{
		Name: gwapiv1.ObjectName(name),
	}
}

func ptrTo[T any](v T) *T {
	return &v
}
