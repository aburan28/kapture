package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const KindTrafficCapture = "TrafficCapture"

// +kubebuilder:validation:Enum=Mirror;EBPF
type CaptureBackend string

const (
	CaptureBackendMirror CaptureBackend = "Mirror"
	CaptureBackendEBPF   CaptureBackend = "EBPF"
)

// EBPFCaptureSettings controls a pod-scoped, node-local OpenSSL collector.
type EBPFCaptureSettings struct {
	// PID optionally selects one host PID belonging to the target pod.
	// +optional
	// +kubebuilder:validation:Minimum=1
	PID *int32 `json:"pid,omitempty"`
	// LibraryPath overrides OpenSSL discovery in /proc/PID/maps.
	// +optional
	// +kubebuilder:validation:Pattern=`^/.*`
	LibraryPath string `json:"libraryPath,omitempty"`
	// +optional
	// +kubebuilder:default=256
	// +kubebuilder:validation:Minimum=8
	// +kubebuilder:validation:Maximum=8192
	MapPages int32 `json:"mapPages,omitempty"`
	// +optional
	// +kubebuilder:default=134217728
	// +kubebuilder:validation:Minimum=1048576
	// +kubebuilder:validation:Maximum=2147483648
	MemoryBytes int64 `json:"memoryBytes,omitempty"`
	// +optional
	// +kubebuilder:default=33554432
	// +kubebuilder:validation:Minimum=1048576
	// +kubebuilder:validation:Maximum=2147483648
	HistoryBytes int64 `json:"historyBytes,omitempty"`
	// +optional
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$`
	History string `json:"history,omitempty"`
	// TriggerStatus keeps recent exchanges on the same connection when this
	// HTTP status is reached; zero exports every exchange. gRPC errors trigger too.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=599
	TriggerStatus int32 `json:"triggerStatus,omitempty"`
	// PCAP exports a labeled reconstructed plaintext PCAPNG, never a wire trace.
	// +optional
	// +kubebuilder:default=true
	PCAP *bool `json:"pcap,omitempty"`
}

// +kubebuilder:validation:Enum=Pending;Active;Completed;Failed
type TrafficCapturePhase string

const (
	TrafficCapturePhasePending   TrafficCapturePhase = "Pending"
	TrafficCapturePhaseActive    TrafficCapturePhase = "Active"
	TrafficCapturePhaseCompleted TrafficCapturePhase = "Completed"
	TrafficCapturePhaseFailed    TrafficCapturePhase = "Failed"
)

// CaptureStorageReference references a CaptureStorage in the same namespace.
type CaptureStorageReference struct {
	Name gwapiv1.ObjectName `json:"name"`
}

// HeaderMatch selects traffic by an exact header name/value pair.
type HeaderMatch struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// CaptureFilters narrows which traffic is mirrored.
type CaptureFilters struct {
	// +optional
	Headers []HeaderMatch `json:"headers,omitempty"`
	// +optional
	PathPrefix *string `json:"pathPrefix,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Percentage *int32 `json:"percentage,omitempty"`
}

// CaptureSettings controls how mirrored traffic is recorded.
type CaptureSettings struct {
	// +optional
	IncludeHeaders *bool `json:"includeHeaders,omitempty"`
	// +optional
	IncludeBody *bool `json:"includeBody,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxBodyBytes *int64 `json:"maxBodyBytes,omitempty"`
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	Duration *string `json:"duration,omitempty"`

	// RedactHeaders lists additional headers whose values are replaced
	// with "[REDACTED]" before captured requests are stored, on top of the
	// default credential set (Authorization, Proxy-Authorization, Cookie,
	// Set-Cookie, X-Api-Key, X-Auth-Token).
	// +optional
	RedactHeaders []string `json:"redactHeaders,omitempty"`

	// DisableHeaderRedaction turns off ALL header redaction, including the
	// default credential set. Captured credentials then become durable
	// storage data and are re-sent verbatim on replay — only use this when
	// the capture pipeline and storage are trusted with live secrets.
	// +optional
	DisableHeaderRedaction *bool `json:"disableHeaderRedaction,omitempty"`
}

// AgentScalingSpec configures capture agent scaling.
//
// +kubebuilder:validation:XValidation:rule="!has(self.minReplicas) || !has(self.maxReplicas) || self.minReplicas <= self.maxReplicas",message="minReplicas must not exceed maxReplicas"
type AgentScalingSpec struct {
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinReplicas *int32 `json:"minReplicas,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetCPU *int32 `json:"targetCPU,omitempty"`
}

// PayloadPluginRef references a named payload plugin to apply in the capture pipeline.
type PayloadPluginRef struct {
	// Name is the registered plugin name (e.g., "redact", "sample", "enrich").
	Name string `json:"name"`
	// +optional
	// +kubebuilder:validation:Enum=drop;skip;abort
	// +kubebuilder:default=drop
	OnError *string `json:"onError,omitempty"`
	// Config is plugin-specific configuration.
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// TrafficCaptureSpec defines the desired state of TrafficCapture.
//
// +kubebuilder:validation:XValidation:rule="!has(self.backend) || self.backend != 'EBPF' || (size(self.targetRef.group) == 0 && self.targetRef.kind == 'Pod')",message="EBPF requires a core Pod target in the capture namespace"
// +kubebuilder:validation:XValidation:rule="!has(self.ebpf) || (has(self.backend) && self.backend == 'EBPF')",message="ebpf settings require the EBPF backend"
// +kubebuilder:validation:XValidation:rule="!has(self.backend) || self.backend != 'EBPF' || !has(self.agent)",message="EBPF is node-local and does not support agent autoscaling"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.backend) || self.backend == oldSelf.backend",message="capture backend is immutable"
type TrafficCaptureSpec struct {
	// +optional
	// +kubebuilder:default=Mirror
	Backend CaptureBackend `json:"backend,omitempty"`
	// +optional
	EBPF       *EBPFCaptureSettings               `json:"ebpf,omitempty"`
	TargetRef  gwapiv1.LocalPolicyTargetReference `json:"targetRef"`
	StorageRef CaptureStorageReference            `json:"storageRef"`
	Capture    CaptureSettings                    `json:"capture"`
	// +optional
	Filters *CaptureFilters `json:"filters,omitempty"`
	// +optional
	Agent *AgentScalingSpec `json:"agent,omitempty"`
	// Plugins are payload processing plugins applied in order between
	// capture and storage. Each plugin can transform, filter, or observe
	// captured requests.
	// +optional
	Plugins []PayloadPluginRef `json:"plugins,omitempty"`
}

// CaptureAgentStatus summarizes the deployed capture agents.
type CaptureAgentStatus struct {
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
}

// TrafficCaptureStatus defines the observed state of TrafficCapture.
type TrafficCaptureStatus struct {
	// +optional
	Phase TrafficCapturePhase `json:"phase,omitempty"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	CapturedRequests int64 `json:"capturedRequests,omitempty"`
	// +optional
	BytesWritten *resource.Quantity `json:"bytesWritten,omitempty"`
	// +optional
	AgentStatus *CaptureAgentStatus `json:"agentStatus,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// TrafficCapture defines a traffic capture request.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetRef.name`
// +kubebuilder:printcolumn:name="Storage",type=string,JSONPath=`.spec.storageRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TrafficCapture struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrafficCaptureSpec   `json:"spec"`
	Status TrafficCaptureStatus `json:"status,omitempty"`
}

// TrafficCaptureList contains a list of TrafficCapture resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TrafficCaptureList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TrafficCapture `json:"items"`
}

func init() {
	localSchemeBuilder.Register(&TrafficCapture{}, &TrafficCaptureList{})
}
