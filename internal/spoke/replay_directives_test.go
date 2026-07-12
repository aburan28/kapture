package spoke

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

func directiveScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := capturev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func startDirective() *hubv1.ReplayDirective {
	return &hubv1.ReplayDirective{
		DirectiveId:       "default-lt-shard-1",
		Action:            hubv1.ReplayAction_REPLAY_ACTION_START,
		LoadTestName:      "lt",
		LoadTestNamespace: "default",
		Spec: &hubv1.ReplaySpec{
			SourceCapture:  "orders",
			StorageRefName: "s3-store",
			TargetHost:     "staging.internal",
			TargetPort:     8443,
			TargetTls:      true,
			Concurrency:    25,
			Shard:          &hubv1.ReplayShard{Index: 1, Count: 4},
			Rate: &hubv1.ReplayRate{
				Mode:              "Constant",
				RequestsPerSecond: 100,
			},
			Filters: &hubv1.ReplayDataFilters{
				PathPrefix: "/api",
				Methods:    []string{"GET", "POST"},
				Limit:      50000,
				StartTime:  timestamppb.Now(),
			},
		},
	}
}

func TestReplayDirectiveHandler_StartCreatesShard(t *testing.T) {
	scheme := directiveScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	h := &ReplayDirectiveHandler{Client: cl, Log: logr.Discard()}

	if err := h.Handle(context.Background(), startDirective()); err != nil {
		t.Fatalf("Handle(START): %v", err)
	}

	tr := &capturev1alpha1.TrafficReplay{}
	key := types.NamespacedName{Namespace: "default", Name: "lt-shard-1"}
	if err := cl.Get(context.Background(), key, tr); err != nil {
		t.Fatalf("shard TrafficReplay not created: %v", err)
	}

	if tr.Labels[LabelLoadTest] != "lt" || tr.Labels[LabelShardIndex] != "1" {
		t.Errorf("labels wrong: %v", tr.Labels)
	}
	if tr.Spec.SourceRef.Name != "orders" || string(tr.Spec.StorageRef.Name) != "s3-store" {
		t.Errorf("source/storage wrong: %+v", tr.Spec)
	}
	if tr.Spec.Target.Host != "staging.internal" || *tr.Spec.Target.Port != 8443 || !*tr.Spec.Target.TLS {
		t.Errorf("target wrong: %+v", tr.Spec.Target)
	}
	if tr.Spec.Shard == nil || tr.Spec.Shard.Index != 1 || tr.Spec.Shard.Count != 4 {
		t.Errorf("shard wrong: %+v", tr.Spec.Shard)
	}
	if tr.Spec.LoadTestRef == nil || tr.Spec.LoadTestRef.Name != "lt" {
		t.Errorf("loadTestRef wrong: %+v", tr.Spec.LoadTestRef)
	}
	if tr.Spec.Rate == nil || tr.Spec.Rate.Mode != capturev1alpha1.ReplayRateModeConstant || *tr.Spec.Rate.RequestsPerSecond != 100 {
		t.Errorf("rate wrong: %+v", tr.Spec.Rate)
	}
	if tr.Spec.Filters == nil || *tr.Spec.Filters.PathPrefix != "/api" || *tr.Spec.Filters.Limit != 50000 {
		t.Errorf("filters wrong: %+v", tr.Spec.Filters)
	}
	if *tr.Spec.Concurrency != 25 {
		t.Errorf("concurrency = %d, want 25", *tr.Spec.Concurrency)
	}
}

func TestReplayDirectiveHandler_StartIsIdempotent(t *testing.T) {
	scheme := directiveScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	h := &ReplayDirectiveHandler{Client: cl, Log: logr.Discard()}

	if err := h.Handle(context.Background(), startDirective()); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	if err := h.Handle(context.Background(), startDirective()); err != nil {
		t.Fatalf("second Handle (resend): %v", err)
	}

	list := &capturev1alpha1.TrafficReplayList{}
	if err := cl.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Errorf("resend created %d TrafficReplays, want 1", len(list.Items))
	}
}

func TestReplayDirectiveHandler_StopDeletesOnlyLoadTestShards(t *testing.T) {
	scheme := directiveScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	h := &ReplayDirectiveHandler{Client: cl, Log: logr.Discard()}

	// Create two shards of "lt" plus one unrelated replay.
	if err := h.Handle(context.Background(), startDirective()); err != nil {
		t.Fatal(err)
	}
	other := startDirective()
	other.Spec.Shard.Index = 2
	if err := h.Handle(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	unrelated := startDirective()
	unrelated.LoadTestName = "other-lt"
	if err := h.Handle(context.Background(), unrelated); err != nil {
		t.Fatal(err)
	}

	stop := &hubv1.ReplayDirective{
		DirectiveId:       "default-lt-stop",
		Action:            hubv1.ReplayAction_REPLAY_ACTION_STOP,
		LoadTestName:      "lt",
		LoadTestNamespace: "default",
	}
	if err := h.Handle(context.Background(), stop); err != nil {
		t.Fatalf("Handle(STOP): %v", err)
	}

	list := &capturev1alpha1.TrafficReplayList{}
	if err := cl.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("after stop %d TrafficReplays remain, want 1 (the unrelated load test)", len(list.Items))
	}
	if list.Items[0].Labels[LabelLoadTest] != "other-lt" {
		t.Errorf("wrong survivor: %v", list.Items[0].Labels)
	}
}

func TestReplayDirectiveHandler_StartRequiresSpec(t *testing.T) {
	scheme := directiveScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	h := &ReplayDirectiveHandler{Client: cl, Log: logr.Discard()}

	bad := &hubv1.ReplayDirective{
		DirectiveId:       "x",
		Action:            hubv1.ReplayAction_REPLAY_ACTION_START,
		LoadTestName:      "lt",
		LoadTestNamespace: "default",
	}
	if err := h.Handle(context.Background(), bad); err == nil {
		t.Error("Handle(START without spec) succeeded, want error")
	}
}
