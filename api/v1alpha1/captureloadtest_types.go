package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const KindCaptureLoadTest = "CaptureLoadTest"

// +kubebuilder:validation:Enum=Pending;Distributing;Running;Completed;Failed;Aborted
type CaptureLoadTestPhase string

const (
	CaptureLoadTestPhasePending      CaptureLoadTestPhase = "Pending"
	CaptureLoadTestPhaseDistributing CaptureLoadTestPhase = "Distributing"
	CaptureLoadTestPhaseRunning      CaptureLoadTestPhase = "Running"
	CaptureLoadTestPhaseCompleted    CaptureLoadTestPhase = "Completed"
	CaptureLoadTestPhaseFailed       CaptureLoadTestPhase = "Failed"
	CaptureLoadTestPhaseAborted      CaptureLoadTestPhase = "Aborted"
)

// Condition types set by the load test coordinator.
const (
	// LoadTestConditionSpokesAssigned indicates whether the hub found enough
	// connected spokes in the requested cells to run the test.
	LoadTestConditionSpokesAssigned = "SpokesAssigned"
	// LoadTestConditionAborted records why a run was aborted.
	LoadTestConditionAborted = "Aborted"
)

// LoadTestDistribution controls how replay work is fanned out across the
// hub's connected spokes.
type LoadTestDistribution struct {
	// Cells restricts the load test to spokes registered in these cells.
	// Empty means all connected spokes are eligible.
	// +optional
	Cells []string `json:"cells,omitempty"`

	// MaxSpokes caps how many spokes participate. Zero or unset means all
	// eligible spokes participate.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxSpokes *int32 `json:"maxSpokes,omitempty"`

	// WorkersPerSpoke is the number of replay shards each participating
	// spoke runs. Total shards = participating spokes * workersPerSpoke.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	WorkersPerSpoke *int32 `json:"workersPerSpoke,omitempty"`

	// ConcurrencyPerWorker is the number of parallel senders inside each
	// replay worker.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	ConcurrencyPerWorker *int32 `json:"concurrencyPerWorker,omitempty"`
}

// LoadTestAbortPolicy defines conditions under which the hub aborts an
// in-flight load test and stops all shards.
type LoadTestAbortPolicy struct {
	// MaxDuration aborts the run if it has been running longer than this.
	// +optional
	MaxDuration *metav1.Duration `json:"maxDuration,omitempty"`

	// ErrorPercent aborts the run when failed requests exceed this
	// percentage of sent requests (evaluated once at least
	// minSampleRequests requests have been sent).
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	ErrorPercent *int32 `json:"errorPercent,omitempty"`

	// MinSampleRequests is the minimum number of attempted requests before
	// ErrorPercent is evaluated. Defaults to 100.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinSampleRequests *int64 `json:"minSampleRequests,omitempty"`
}

// CaptureLoadTestSpec defines a distributed replay-based load test executed
// across spoke clusters coordinated by the hub.
type CaptureLoadTestSpec struct {
	// SourceRef references the TrafficCapture whose recorded traffic drives
	// the load test. Every participating spoke must be able to resolve the
	// referenced capture data through StorageRef.
	SourceRef TrafficCaptureReference `json:"sourceRef"`

	// StorageRef names the CaptureStorage (present on each participating
	// spoke, same name/namespace as this load test) to read captured data from.
	StorageRef CaptureStorageReference `json:"storageRef"`

	// Target defines where replayed traffic is sent.
	Target ReplayTarget `json:"target"`

	// Rate controls replay pacing. For Constant mode the configured
	// requestsPerSecond is the aggregate rate across all shards; the hub
	// divides it between workers. Defaults to OriginalTiming behaviour of
	// each shard's subset, which reproduces the recorded QPS in aggregate.
	// +optional
	Rate *ReplayRateConfig `json:"rate,omitempty"`

	// Filters narrows which captured requests are replayed. Limit applies
	// per shard.
	// +optional
	Filters *ReplayFilters `json:"filters,omitempty"`

	// Distribution controls cell targeting and shard fan-out.
	// +optional
	Distribution *LoadTestDistribution `json:"distribution,omitempty"`

	// Abort defines in-flight safety limits.
	// +optional
	Abort *LoadTestAbortPolicy `json:"abort,omitempty"`

	// Engine selects the replay engine every shard runs (builtin, k6,
	// ghz, or an installed plugin). Defaults to the builtin sender.
	// +optional
	Engine *ReplayEngineSpec `json:"engine,omitempty"`
}

// LoadTestCellStatus aggregates progress for one cell.
type LoadTestCellStatus struct {
	Name string `json:"name"`

	// +optional
	Spokes int32 `json:"spokes,omitempty"`

	// +optional
	Shards int32 `json:"shards,omitempty"`

	// +optional
	SentRequests int64 `json:"sentRequests,omitempty"`

	// +optional
	FailedRequests int64 `json:"failedRequests,omitempty"`
}

// LoadTestAssignment records which spoke runs which shards. The coordinator
// uses it to route stop directives and to detect orphaned shards.
type LoadTestAssignment struct {
	SpokeID string `json:"spokeID"`

	// +optional
	Cell string `json:"cell,omitempty"`

	// ShardIndexes lists the shard indexes assigned to this spoke.
	ShardIndexes []int32 `json:"shardIndexes"`
}

// CaptureLoadTestStatus defines the observed state of CaptureLoadTest.
type CaptureLoadTestStatus struct {
	// +optional
	Phase CaptureLoadTestPhase `json:"phase,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// TotalShards is the number of replay shards the capture was split into.
	// +optional
	TotalShards int32 `json:"totalShards,omitempty"`

	// AssignedSpokes is the number of spokes participating in the run.
	// +optional
	AssignedSpokes int32 `json:"assignedSpokes,omitempty"`

	// Assignments records the spoke → shard mapping decided at distribution
	// time.
	// +optional
	Assignments []LoadTestAssignment `json:"assignments,omitempty"`

	// Cells aggregates progress per cell.
	// +optional
	Cells []LoadTestCellStatus `json:"cells,omitempty"`

	// +optional
	TotalRequests int64 `json:"totalRequests,omitempty"`

	// +optional
	SentRequests int64 `json:"sentRequests,omitempty"`

	// +optional
	FailedRequests int64 `json:"failedRequests,omitempty"`

	// +optional
	FilteredRequests int64 `json:"filteredRequests,omitempty"`

	// CompletedShards is the number of shards that finished successfully.
	// +optional
	CompletedShards int32 `json:"completedShards,omitempty"`

	// FailedShards is the number of shards that terminated with an error.
	// +optional
	FailedShards int32 `json:"failedShards,omitempty"`

	// AchievedRPS is the aggregate observed send rate across all shards.
	// +optional
	AchievedRPS *string `json:"achievedRPS,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CaptureLoadTest defines a distributed load test that replays captured
// traffic from many spoke clusters at once, coordinated by the hub.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceRef.name`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.host`
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.status.totalShards`
// +kubebuilder:printcolumn:name="Sent",type=integer,JSONPath=`.status.sentRequests`
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.failedRequests`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CaptureLoadTest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CaptureLoadTestSpec   `json:"spec"`
	Status CaptureLoadTestStatus `json:"status,omitempty"`
}

// CaptureLoadTestList contains a list of CaptureLoadTest resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CaptureLoadTestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CaptureLoadTest `json:"items"`
}

func init() {
	localSchemeBuilder.Register(&CaptureLoadTest{}, &CaptureLoadTestList{})
}
