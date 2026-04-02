package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const KindTrafficReplay = "TrafficReplay"

// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
type TrafficReplayPhase string

const (
	TrafficReplayPhasePending   TrafficReplayPhase = "Pending"
	TrafficReplayPhaseRunning   TrafficReplayPhase = "Running"
	TrafficReplayPhaseCompleted TrafficReplayPhase = "Completed"
	TrafficReplayPhaseFailed    TrafficReplayPhase = "Failed"
)

// +kubebuilder:validation:Enum=Constant;OriginalTiming;Unlimited
type ReplayRateMode string

const (
	ReplayRateModeConstant       ReplayRateMode = "Constant"
	ReplayRateModeOriginalTiming ReplayRateMode = "OriginalTiming"
	ReplayRateModeUnlimited      ReplayRateMode = "Unlimited"
)

// TrafficCaptureReference references a TrafficCapture whose data should be replayed.
type TrafficCaptureReference struct {
	Name string `json:"name"`
}

// ReplayTarget defines the destination for replayed traffic.
type ReplayTarget struct {
	// Host is the target hostname or IP to replay traffic to.
	Host string `json:"host"`

	// Port is the target port. Defaults to 80 for HTTP, 443 for HTTPS.
	// +optional
	Port *int32 `json:"port,omitempty"`

	// +optional
	// +kubebuilder:default=false
	TLS *bool `json:"tls,omitempty"`
}

// ReplayRateConfig controls the pacing of replayed requests.
type ReplayRateConfig struct {
	// Mode controls how replay requests are paced.
	// +kubebuilder:default=Unlimited
	Mode ReplayRateMode `json:"mode"`

	// RequestsPerSecond is the target RPS when Mode is Constant.
	// +optional
	// +kubebuilder:validation:Minimum=1
	RequestsPerSecond *int32 `json:"requestsPerSecond,omitempty"`

	// TimeScale multiplies original inter-request delays when Mode is
	// OriginalTiming. 1.0 = real-time, 0.5 = 2x speed.
	// +optional
	TimeScale *string `json:"timeScale,omitempty"`
}

// ReplayFilters narrows which captured requests are replayed.
type ReplayFilters struct {
	// StartTime replays only requests captured at or after this time.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// EndTime replays only requests captured before this time.
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// PathPrefix filters to requests whose path starts with this prefix.
	// +optional
	PathPrefix *string `json:"pathPrefix,omitempty"`

	// Methods filters to specific HTTP methods.
	// +optional
	Methods []string `json:"methods,omitempty"`

	// Limit caps the total number of requests to replay. Zero means unlimited.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Limit *int64 `json:"limit,omitempty"`
}

// ReplayTransformRef references a named transformer plugin.
type ReplayTransformRef struct {
	// Name is the registered transformer plugin name (e.g., "header-rewriter").
	Name string `json:"name"`

	// Config is plugin-specific configuration.
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// TrafficReplaySpec defines the desired state of TrafficReplay.
type TrafficReplaySpec struct {
	// SourceRef references the TrafficCapture whose data should be replayed.
	SourceRef TrafficCaptureReference `json:"sourceRef"`

	// StorageRef references the CaptureStorage to read captured data from.
	StorageRef CaptureStorageReference `json:"storageRef"`

	// Target defines where to send replayed traffic.
	Target ReplayTarget `json:"target"`

	// Rate controls replay pacing. Optional, defaults to unlimited.
	// +optional
	Rate *ReplayRateConfig `json:"rate,omitempty"`

	// Filters narrows which captured requests are replayed.
	// +optional
	Filters *ReplayFilters `json:"filters,omitempty"`

	// Transforms are applied in order to each request before replay.
	// +optional
	Transforms []ReplayTransformRef `json:"transforms,omitempty"`

	// Concurrency is the number of parallel replay workers. Defaults to 1.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Concurrency *int32 `json:"concurrency,omitempty"`
}

// TrafficReplayStatus defines the observed state of TrafficReplay.
type TrafficReplayStatus struct {
	// +optional
	Phase TrafficReplayPhase `json:"phase,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// TotalRequests is the number of requests read from the source.
	// +optional
	TotalRequests int64 `json:"totalRequests,omitempty"`

	// SentRequests is the number of requests successfully sent to the target.
	// +optional
	SentRequests int64 `json:"sentRequests,omitempty"`

	// FailedRequests is the number of requests that failed to send.
	// +optional
	FailedRequests int64 `json:"failedRequests,omitempty"`

	// FilteredRequests is the number of requests dropped by filters or transforms.
	// +optional
	FilteredRequests int64 `json:"filteredRequests,omitempty"`

	// MeanLatency is the average round-trip time for replayed requests.
	// +optional
	MeanLatency *string `json:"meanLatency,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// TrafficReplay defines a traffic replay request.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceRef.name`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.host`
// +kubebuilder:printcolumn:name="Sent",type=integer,JSONPath=`.status.sentRequests`
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.failedRequests`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TrafficReplay struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrafficReplaySpec   `json:"spec"`
	Status TrafficReplayStatus `json:"status,omitempty"`
}

// TrafficReplayList contains a list of TrafficReplay resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TrafficReplayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TrafficReplay `json:"items"`
}

func init() {
	localSchemeBuilder.Register(&TrafficReplay{}, &TrafficReplayList{})
}
