package spoke

import (
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
)

func makeShardReplay() *capturev1alpha1.TrafficReplay {
	return &capturev1alpha1.TrafficReplay{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lt-shard-1",
			Namespace: "default",
			Labels: map[string]string{
				LabelLoadTest:   "lt",
				LabelShardIndex: "1",
			},
		},
		Spec: capturev1alpha1.TrafficReplaySpec{
			SourceRef:   capturev1alpha1.TrafficCaptureReference{Name: "orders"},
			StorageRef:  capturev1alpha1.CaptureStorageReference{Name: gwapiv1.ObjectName("test-storage")},
			Target:      capturev1alpha1.ReplayTarget{Host: "staging.internal"},
			Shard:       &capturev1alpha1.ReplayShardSpec{Index: 1, Count: 4},
			Concurrency: i32(25),
			LoadTestRef: &capturev1alpha1.LoadTestReference{Name: "lt"},
		},
	}
}

func argValue(t *testing.T, args []string, flag string) string {
	t.Helper()
	idx := slices.Index(args, flag)
	if idx < 0 || idx+1 >= len(args) {
		t.Fatalf("flag %s not found in args: %v", flag, args)
	}
	return args[idx+1]
}

func TestBuildReplayJob_S3Shard(t *testing.T) {
	tr := makeShardReplay()
	storage := makeS3Storage()

	job, err := BuildReplayJob(tr, storage)
	if err != nil {
		t.Fatalf("BuildReplayJob: %v", err)
	}

	if job.Name != "lt-shard-1-replay" || job.Namespace != "default" {
		t.Errorf("job identity wrong: %s/%s", job.Namespace, job.Name)
	}
	if job.Labels[LabelLoadTest] != "lt" || job.Labels[LabelShardIndex] != "1" {
		t.Errorf("job labels missing load test metadata: %v", job.Labels)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	if got := argValue(t, args, "--capture-id"); got != "default/orders" {
		t.Errorf("--capture-id = %q", got)
	}
	if got := argValue(t, args, "--storage-type"); got != "s3" {
		t.Errorf("--storage-type = %q", got)
	}
	if got := argValue(t, args, "--bucket"); got != "my-bucket" {
		t.Errorf("--bucket = %q", got)
	}
	if got := argValue(t, args, "--target-url"); got != "http://staging.internal:80" {
		t.Errorf("--target-url = %q", got)
	}
	if got := argValue(t, args, "--shard-index"); got != "1" {
		t.Errorf("--shard-index = %q", got)
	}
	if got := argValue(t, args, "--shard-count"); got != "4" {
		t.Errorf("--shard-count = %q", got)
	}
	if got := argValue(t, args, "--concurrency"); got != "25" {
		t.Errorf("--concurrency = %q", got)
	}
	if got := argValue(t, args, "--summary-path"); got != "/dev/termination-log" {
		t.Errorf("--summary-path = %q", got)
	}
	// Default rate mode preserves recorded timing.
	if got := argValue(t, args, "--rate-mode"); got != "original" {
		t.Errorf("--rate-mode = %q, want original", got)
	}
}

func TestBuildReplayJob_TLSAndConstantRate(t *testing.T) {
	tr := makeShardReplay()
	tr.Spec.Target.TLS = bl(true)
	tr.Spec.Target.Port = i32(8443)
	tr.Spec.Rate = &capturev1alpha1.ReplayRateConfig{
		Mode:              capturev1alpha1.ReplayRateModeConstant,
		RequestsPerSecond: i32(250),
	}
	tr.Spec.Filters = &capturev1alpha1.ReplayFilters{
		PathPrefix: sp("/api"),
		Methods:    []string{"GET", "POST"},
		Limit:      i64(1000),
	}

	job, err := BuildReplayJob(tr, makeS3Storage())
	if err != nil {
		t.Fatalf("BuildReplayJob: %v", err)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	if got := argValue(t, args, "--target-url"); got != "https://staging.internal:8443" {
		t.Errorf("--target-url = %q", got)
	}
	if got := argValue(t, args, "--rate-mode"); got != "constant" {
		t.Errorf("--rate-mode = %q", got)
	}
	if got := argValue(t, args, "--rate-per-second"); got != "250" {
		t.Errorf("--rate-per-second = %q", got)
	}
	if got := argValue(t, args, "--methods"); got != "GET,POST" {
		t.Errorf("--methods = %q", got)
	}
	if got := argValue(t, args, "--limit"); got != "1000" {
		t.Errorf("--limit = %q", got)
	}
}

func TestBuildReplayJob_ExternalEngineArgs(t *testing.T) {
	tr := makeShardReplay()
	tr.Spec.Engine = &capturev1alpha1.ReplayEngineSpec{
		Name:   "k6",
		Config: &runtime.RawExtension{Raw: []byte(`{"vus":200}`)},
	}
	t.Setenv("REPLAY_PLUGIN_DIR", "/plugins")

	job, err := BuildReplayJob(tr, makeS3Storage())
	if err != nil {
		t.Fatalf("BuildReplayJob: %v", err)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	if got := argValue(t, args, "--engine"); got != "k6" {
		t.Errorf("--engine = %q", got)
	}
	if got := argValue(t, args, "--engine-config"); got != `{"vus":200}` {
		t.Errorf("--engine-config = %q", got)
	}

	env := job.Spec.Template.Spec.Containers[0].Env
	if len(env) != 1 || env[0].Name != "REPLAY_PLUGIN_DIR" || env[0].Value != "/plugins" {
		t.Errorf("plugin dir env not passed through: %+v", env)
	}
}

func TestBuildReplayJob_BuiltinEngineOmitsEngineFlag(t *testing.T) {
	tr := makeShardReplay()
	tr.Spec.Engine = &capturev1alpha1.ReplayEngineSpec{Name: "builtin"}

	job, err := BuildReplayJob(tr, makeS3Storage())
	if err != nil {
		t.Fatalf("BuildReplayJob: %v", err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	if slices.Contains(args, "--engine") {
		t.Errorf("builtin engine should not pass --engine: %v", args)
	}
}

func TestBuildReplayJob_EFSMountsVolume(t *testing.T) {
	tr := makeShardReplay()
	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storage", Namespace: "default"},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypeEFS,
			EFS: &capturev1alpha1.EFSConfig{
				FileSystemID: "fs-123",
				MountPath:    "/captures",
			},
		},
	}

	job, err := BuildReplayJob(tr, storage)
	if err != nil {
		t.Fatalf("BuildReplayJob: %v", err)
	}

	args := job.Spec.Template.Spec.Containers[0].Args
	if got := argValue(t, args, "--mount-path"); got != "/captures" {
		t.Errorf("--mount-path = %q", got)
	}
	if len(job.Spec.Template.Spec.Volumes) != 1 {
		t.Fatalf("EFS job has %d volumes, want 1", len(job.Spec.Template.Spec.Volumes))
	}
	mounts := job.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 1 || mounts[0].MountPath != "/captures" || !mounts[0].ReadOnly {
		t.Errorf("volume mount wrong: %+v", mounts)
	}
}

func TestBuildReplayJob_RejectsPluginStorage(t *testing.T) {
	tr := makeShardReplay()
	storage := &capturev1alpha1.CaptureStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "test-storage", Namespace: "default"},
		Spec: capturev1alpha1.CaptureStorageSpec{
			Type: capturev1alpha1.CaptureStorageTypePlugin,
		},
	}
	if _, err := BuildReplayJob(tr, storage); err == nil {
		t.Error("BuildReplayJob with Plugin storage succeeded, want error")
	}
}

func TestReplaySummaryFromStatus(t *testing.T) {
	tr := makeShardReplay()
	tr.Status.Phase = capturev1alpha1.TrafficReplayPhaseRunning
	tr.Status.SentRequests = 1234
	tr.Status.FailedRequests = 5
	rps := "88.5"
	tr.Status.AchievedRPS = &rps
	p99 := "250ms"
	tr.Status.P99Latency = &p99

	summary := ReplaySummaryFromStatus(tr)
	if summary == nil {
		t.Fatal("summary is nil for load-test replay")
	}
	if summary.LoadTestName != "lt" || summary.LoadTestNamespace != "default" {
		t.Errorf("identity wrong: %+v", summary)
	}
	if summary.ShardIndex != 1 || summary.ShardCount != 4 {
		t.Errorf("shard wrong: %+v", summary)
	}
	if summary.SentRequests != 1234 || summary.FailedRequests != 5 {
		t.Errorf("counts wrong: %+v", summary)
	}
	if summary.AchievedRps != 88.5 {
		t.Errorf("achievedRps = %v", summary.AchievedRps)
	}
	if summary.P99LatencyMs != 250 {
		t.Errorf("p99 = %d, want 250", summary.P99LatencyMs)
	}

	// Standalone replays (no loadTestRef) are not reported to the hub.
	standalone := makeShardReplay()
	standalone.Spec.LoadTestRef = nil
	if got := ReplaySummaryFromStatus(standalone); got != nil {
		t.Errorf("standalone replay produced summary: %+v", got)
	}
}

func TestReplayShardName(t *testing.T) {
	if got := ReplayShardName("lt", 3); got != "lt-shard-3" {
		t.Errorf("ReplayShardName = %q", got)
	}
	if !strings.HasPrefix(ReplayJobName(makeShardReplay()), "lt-shard-1") {
		t.Errorf("job name should derive from replay name")
	}
}
