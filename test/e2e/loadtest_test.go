// End-to-end tests for multi-cell distributed load testing.
//
// These tests require the full hub-spoke wiring that the e2e workflow sets
// up: a CaptureHub CR (named by E2E_CAPTUREHUB_NAME) whose gRPC server the
// deployed spoke has registered with, the spoke registered under the cell
// named by E2E_SPOKE_CELL, and the replay-engine image available in the
// cluster. When the CaptureHub CR is absent the tests skip, so the suite
// still passes against a plain controller deployment.
//
// The sharding test is the load-bearing one: it seeds a synthetic capture
// into a PVC, fans a CaptureLoadTest out into multiple shards on the spoke,
// and verifies through an HTTP sink's request counter that every captured
// request was replayed exactly once across all shards — duplicates would
// push the counter above the seeded count, dropped shards would leave it
// below.
package e2e

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

const (
	// seedRequestCount is how many synthetic requests the sharding test
	// seeds and expects to see replayed exactly once.
	seedRequestCount = 120

	// loadTestWorkers is the shard fan-out on the (single) e2e spoke.
	loadTestWorkers = 3
)

func e2eHubName() string {
	if v := os.Getenv("E2E_CAPTUREHUB_NAME"); v != "" {
		return v
	}
	return "e2e-loadtest-hub"
}

func e2eSpokeCell() string {
	if v := os.Getenv("E2E_SPOKE_CELL"); v != "" {
		return v
	}
	return "cell-e2e"
}

func e2eAgentImage() string {
	if v := os.Getenv("E2E_AGENT_IMAGE"); v != "" {
		return v
	}
	return "kapture/agent:e2e"
}

// requireConnectedSpoke skips the test when the load-test environment is
// not deployed, and fails when it is deployed but the spoke never
// registers with the hub.
func requireConnectedSpoke(t *testing.T) {
	t.Helper()

	hub := &capturev1alpha1.CaptureHub{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: e2eHubName()}, hub); err != nil {
		t.Skipf("CaptureHub %q not found; load-test e2e environment not deployed: %v", e2eHubName(), err)
	}

	waitFor(t, "spoke to register with hub", 180*time.Second, func() bool {
		got := &capturev1alpha1.CaptureHub{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: e2eHubName()}, got); err != nil {
			return false
		}
		return got.Status.ConnectedSpokes >= 1
	})
}

// --- Test: hub reports cell topology ---

func TestCaptureHubReportsCellTopology(t *testing.T) {
	requireConnectedSpoke(t)

	waitFor(t, "cell aggregate in CaptureHub status", 120*time.Second, func() bool {
		hub := &capturev1alpha1.CaptureHub{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: e2eHubName()}, hub); err != nil {
			return false
		}
		for _, cell := range hub.Status.Cells {
			if cell.Name == e2eSpokeCell() && cell.ConnectedSpokes >= 1 {
				return true
			}
		}
		return false
	})

	hub := &capturev1alpha1.CaptureHub{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: e2eHubName()}, hub); err != nil {
		t.Fatalf("getting CaptureHub: %v", err)
	}
	t.Logf("CaptureHub cells: %+v", hub.Status.Cells)

	// The spoke entry itself must carry its cell.
	found := false
	for _, spoke := range hub.Status.Spokes {
		if spoke.Cell == e2eSpokeCell() {
			found = true
		}
	}
	if !found {
		t.Errorf("no spoke reports cell %q in CaptureHub status: %+v", e2eSpokeCell(), hub.Status.Spokes)
	}
}

// --- Test: full distributed load test with sharding verification ---

func TestCaptureLoadTestShardingE2E(t *testing.T) {
	requireConnectedSpoke(t)
	ns := createNamespace(t)

	// Step 1: PVC that carries the seeded capture data into the replay
	// jobs. BuildReplayJob mounts the claim named "capture-data" for
	// filesystem-backed storage.
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "capture-data", Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("64Mi")},
			},
		},
	}
	mustCreate(t, pvc)

	// Step 2: seed a synthetic capture. The replay engine reads
	// {mountPath}/{captureID}/**/*.jsonl.gz where captureID is
	// "<namespace>/<sourceCapture>".
	seedJSONL := buildSeedJSONL(seedRequestCount)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-data", Namespace: ns},
		Data:       map[string]string{"requests.jsonl": seedJSONL},
	}
	mustCreate(t, cm)

	captureDir := fmt.Sprintf("/data/%s/orders/2026-01-01", ns)
	seeder := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "seeder", Namespace: ns},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "seed",
				Image:   "busybox:1.36",
				Command: []string{"sh", "-c", fmt.Sprintf("mkdir -p %q && gzip -c /seed/requests.jsonl > %q && ls -la %q", captureDir, captureDir+"/seed-000001.jsonl.gz", captureDir)},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "capture-data", MountPath: "/data"},
					{Name: "seed", MountPath: "/seed"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "capture-data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "capture-data"},
				}},
				{Name: "seed", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "seed-data"},
					},
				}},
			},
		},
	}
	mustCreate(t, seeder)
	waitFor(t, "seeder pod to complete", 120*time.Second, func() bool {
		pod := &corev1.Pod{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(seeder), pod); err != nil {
			return false
		}
		return pod.Status.Phase == corev1.PodSucceeded
	})
	t.Logf("seeded %d requests into PVC", seedRequestCount)

	// Step 3: deploy the sink target. The capture agent accepts any HTTP
	// request and exposes capture_agent_requests_total on its health port,
	// which is exactly the exactly-once counter this test needs.
	deploySink(t, ns)

	// Step 4: storage + load test resources.
	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "loadtest-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-e2e-loadtest",
				MountPath:    "/captures",
			},
		},
	}
	mustCreate(t, storage)
	markStorageReady(t, storage)

	workers := int32(loadTestWorkers)
	concurrency := int32(5)
	lt := &capturev1alpha1.CaptureLoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "sharding-e2e", Namespace: ns},
		Spec: capturev1alpha1.CaptureLoadTestSpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: "loadtest-storage"},
			Target: capturev1alpha1.ReplayTarget{
				Host: fmt.Sprintf("sink.%s.svc.cluster.local", ns),
				Port: ptrTo(int32(8080)),
			},
			Rate: &capturev1alpha1.ReplayRateConfig{
				Mode: capturev1alpha1.ReplayRateModeUnlimited,
			},
			Distribution: &capturev1alpha1.LoadTestDistribution{
				Cells:                []string{e2eSpokeCell()},
				WorkersPerSpoke:      &workers,
				ConcurrencyPerWorker: &concurrency,
			},
		},
	}
	mustCreate(t, lt)

	// Step 5: control-plane sharding — the spoke must materialise exactly
	// one TrafficReplay per shard with disjoint, dense shard indexes.
	var shards capturev1alpha1.TrafficReplayList
	waitFor(t, "shard TrafficReplays to be created", 120*time.Second, func() bool {
		if err := k8sClient.List(ctx, &shards, client.InNamespace(ns),
			client.MatchingLabels{"capture.gateway.io/load-test": "sharding-e2e"}); err != nil {
			return false
		}
		return len(shards.Items) == loadTestWorkers
	})

	seenIndexes := map[int32]bool{}
	for _, shard := range shards.Items {
		if shard.Spec.Shard == nil {
			t.Fatalf("shard %s has no shard spec", shard.Name)
		}
		if shard.Spec.Shard.Count != loadTestWorkers {
			t.Errorf("shard %s count = %d, want %d", shard.Name, shard.Spec.Shard.Count, loadTestWorkers)
		}
		if seenIndexes[shard.Spec.Shard.Index] {
			t.Errorf("duplicate shard index %d", shard.Spec.Shard.Index)
		}
		seenIndexes[shard.Spec.Shard.Index] = true
		if shard.Spec.LoadTestRef == nil || shard.Spec.LoadTestRef.Name != "sharding-e2e" {
			t.Errorf("shard %s missing loadTestRef", shard.Name)
		}
	}
	for i := int32(0); i < loadTestWorkers; i++ {
		if !seenIndexes[i] {
			t.Errorf("shard index %d never assigned", i)
		}
	}
	t.Logf("%d shards created with disjoint indexes", len(shards.Items))

	// Step 6: the run must complete end to end.
	waitFor(t, "CaptureLoadTest to complete", 300*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), got); err != nil {
			return false
		}
		if got.Status.Phase == capturev1alpha1.CaptureLoadTestPhaseFailed ||
			got.Status.Phase == capturev1alpha1.CaptureLoadTestPhaseAborted {
			t.Fatalf("load test reached %s: %+v", got.Status.Phase, got.Status)
		}
		return got.Status.Phase == capturev1alpha1.CaptureLoadTestPhaseCompleted
	})

	final := &capturev1alpha1.CaptureLoadTest{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), final); err != nil {
		t.Fatalf("getting final load test: %v", err)
	}
	t.Logf("load test status: shards=%d sent=%d failed=%d filtered=%d cells=%+v",
		final.Status.TotalShards, final.Status.SentRequests, final.Status.FailedRequests,
		final.Status.FilteredRequests, final.Status.Cells)

	// Step 7: aggregate accounting — disjoint + exhaustive sharding means
	// the per-shard sent counts sum to exactly the seeded request count.
	if final.Status.TotalShards != loadTestWorkers {
		t.Errorf("totalShards = %d, want %d", final.Status.TotalShards, loadTestWorkers)
	}
	if final.Status.SentRequests != seedRequestCount {
		t.Errorf("aggregate sentRequests = %d, want %d (duplicate or dropped shard slices)",
			final.Status.SentRequests, seedRequestCount)
	}
	if final.Status.FailedRequests != 0 {
		t.Errorf("failedRequests = %d, want 0", final.Status.FailedRequests)
	}
	if final.Status.CompletedShards != loadTestWorkers {
		t.Errorf("completedShards = %d, want %d", final.Status.CompletedShards, loadTestWorkers)
	}
	cellFound := false
	for _, cell := range final.Status.Cells {
		if cell.Name == e2eSpokeCell() && cell.SentRequests == seedRequestCount {
			cellFound = true
		}
	}
	if !cellFound {
		t.Errorf("cell rollup missing or wrong: %+v", final.Status.Cells)
	}

	// Per-shard: every shard completed, sent a non-empty disjoint slice,
	// and the slices sum to the whole capture.
	var shardSum int64
	if err := k8sClient.List(ctx, &shards, client.InNamespace(ns),
		client.MatchingLabels{"capture.gateway.io/load-test": "sharding-e2e"}); err != nil {
		t.Fatalf("listing shards: %v", err)
	}
	for _, shard := range shards.Items {
		if shard.Status.Phase != capturev1alpha1.TrafficReplayPhaseCompleted {
			t.Errorf("shard %s phase = %s, want Completed", shard.Name, shard.Status.Phase)
		}
		if shard.Status.SentRequests == 0 {
			t.Errorf("shard %s sent no requests; hash distribution or shard filter broken", shard.Name)
		}
		shardSum += shard.Status.SentRequests
		t.Logf("shard %s: sent=%d failed=%d p99=%v", shard.Name,
			shard.Status.SentRequests, shard.Status.FailedRequests, shard.Status.P99Latency)
	}
	if shardSum != seedRequestCount {
		t.Errorf("sum of shard sentRequests = %d, want %d", shardSum, seedRequestCount)
	}

	// Step 8: data-plane exactly-once — the sink must have received every
	// seeded request exactly once across all shards.
	var sinkTotal int64
	waitFor(t, "sink metrics to reflect replayed requests", 60*time.Second, func() bool {
		total, err := readSinkRequestTotal(ns)
		if err != nil {
			t.Logf("reading sink metrics: %v", err)
			return false
		}
		sinkTotal = total
		return total >= seedRequestCount
	})
	if sinkTotal != seedRequestCount {
		t.Errorf("sink received %d requests, want exactly %d (>%d means duplicate shard slices)",
			sinkTotal, seedRequestCount, seedRequestCount)
	}
	t.Logf("sink received exactly %d requests across %d shards", sinkTotal, loadTestWorkers)

	// Step 9: deletion must fan STOP out and clean up every shard.
	if err := k8sClient.Delete(ctx, lt); err != nil {
		t.Fatalf("deleting load test: %v", err)
	}
	waitFor(t, "load test deletion", 120*time.Second, func() bool {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), &capturev1alpha1.CaptureLoadTest{})
		return err != nil
	})
	waitFor(t, "shard TrafficReplays cleanup", 120*time.Second, func() bool {
		var remaining capturev1alpha1.TrafficReplayList
		if err := k8sClient.List(ctx, &remaining, client.InNamespace(ns),
			client.MatchingLabels{"capture.gateway.io/load-test": "sharding-e2e"}); err != nil {
			return false
		}
		return len(remaining.Items) == 0
	})
	t.Log("all shard TrafficReplays cleaned up after deletion")
}

// --- Test: load test targeting an empty cell stays Pending ---

func TestCaptureLoadTestNoEligibleCellStaysPending(t *testing.T) {
	requireConnectedSpoke(t)
	ns := createNamespace(t)

	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-cell-storage", Namespace: ns},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS:  &capturev1alpha1.EFSConfig{FileSystemID: "fs-x", MountPath: "/captures"},
		},
	}
	mustCreate(t, storage)
	markStorageReady(t, storage)

	lt := &capturev1alpha1.CaptureLoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-cell", Namespace: ns},
		Spec: capturev1alpha1.CaptureLoadTestSpec{
			SourceRef:  capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef: capturev1alpha1.CaptureStorageReference{Name: "empty-cell-storage"},
			Target:     capturev1alpha1.ReplayTarget{Host: "nowhere.invalid"},
			Distribution: &capturev1alpha1.LoadTestDistribution{
				Cells: []string{"e2e-cell-that-does-not-exist"},
			},
		},
	}
	mustCreate(t, lt)

	waitFor(t, "Pending phase with NoEligibleSpokes", 120*time.Second, func() bool {
		got := &capturev1alpha1.CaptureLoadTest{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), got); err != nil {
			return false
		}
		if got.Status.Phase != capturev1alpha1.CaptureLoadTestPhasePending {
			return false
		}
		for _, cond := range got.Status.Conditions {
			if cond.Type == capturev1alpha1.LoadTestConditionSpokesAssigned &&
				cond.Status == metav1.ConditionFalse && cond.Reason == "NoEligibleSpokes" {
				return true
			}
		}
		return false
	})

	// No shards may have been created anywhere.
	var shards capturev1alpha1.TrafficReplayList
	if err := k8sClient.List(ctx, &shards, client.InNamespace(ns)); err != nil {
		t.Fatalf("listing shards: %v", err)
	}
	if len(shards.Items) != 0 {
		t.Errorf("load test with no eligible spokes created %d shards", len(shards.Items))
	}

	// Deletion of an undistributed load test must not wedge on the finalizer.
	if err := k8sClient.Delete(ctx, lt); err != nil {
		t.Fatalf("deleting load test: %v", err)
	}
	waitFor(t, "undistributed load test deletion", 60*time.Second, func() bool {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), &capturev1alpha1.CaptureLoadTest{})
		return err != nil
	})
}

// ============================================================================
// Helpers
// ============================================================================

// buildSeedJSONL produces n CapturedRequest JSON lines with unique IDs and
// staggered timestamps.
func buildSeedJSONL(n int) string {
	var buf bytes.Buffer
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * 50 * time.Millisecond)
		fmt.Fprintf(&buf,
			`{"id":"e2e-req-%04d","timestamp":%q,"method":"GET","path":"/replay/%d","protocol":"HTTP","metadata":{"host":"orders.e2e.local"}}`+"\n",
			i, ts.Format(time.RFC3339Nano), i)
	}
	return buf.String()
}

// deploySink deploys a capture agent as the replay target plus a Service
// named "sink" exposing HTTP (8080) and health/metrics (8081).
func deploySink(t *testing.T, ns string) {
	t.Helper()

	labels := map[string]string{"app": "e2e-sink"}
	replicas := int32(1)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "sink", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "sink",
						Image: e2eAgentImage(),
						Env: []corev1.EnvVar{
							{Name: "CAPTURE_ID", Value: "e2e-sink"},
							{Name: "STORAGE_TYPE", Value: "efs"},
							{Name: "EFS_MOUNT_PATH", Value: "/tmp/sink"},
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							}},
						},
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 8080},
							{Name: "health", ContainerPort: 8081},
						},
					}},
				},
			},
		},
	}
	mustCreate(t, deploy)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "sink", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8080, TargetPort: intstr.FromString("http")},
				{Name: "health", Port: 8081, TargetPort: intstr.FromString("health")},
			},
		},
	}
	mustCreate(t, svc)

	waitFor(t, "sink to become ready", 120*time.Second, func() bool {
		got := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(deploy), got); err != nil {
			return false
		}
		return got.Status.ReadyReplicas > 0
	})
}

var requestsTotalRe = regexp.MustCompile(`(?m)^capture_agent_requests_total (\d+)$`)

// readSinkRequestTotal scrapes capture_agent_requests_total from the sink
// through the API server's service proxy, which works from out-of-cluster
// test runners.
func readSinkRequestTotal(ns string) (int64, error) {
	raw, err := clientset.CoreV1().Services(ns).
		ProxyGet("http", "sink", "8081", "/metrics", nil).
		DoRaw(ctx)
	if err != nil {
		return 0, fmt.Errorf("proxying sink metrics: %w", err)
	}
	m := requestsTotalRe.FindSubmatch(raw)
	if m == nil {
		return 0, fmt.Errorf("capture_agent_requests_total not found in metrics:\n%s", strings.TrimSpace(string(raw)))
	}
	return strconv.ParseInt(string(m[1]), 10, 64)
}
