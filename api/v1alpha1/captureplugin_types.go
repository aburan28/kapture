package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const KindCapturePlugin = "CapturePlugin"

// +kubebuilder:validation:Enum=Mirror;Standalone
type CapturePluginMode string

const (
	// CapturePluginModeMirror means the plugin receives mirrored traffic on
	// exposed ports. The controller injects a RequestMirror filter into the
	// target route, just like the built-in capture agent.
	CapturePluginModeMirror CapturePluginMode = "Mirror"

	// CapturePluginModeStandalone means the plugin uses its own capture
	// mechanism (eBPF, tcpdump, pcap, etc.). No mirror filter is injected.
	CapturePluginModeStandalone CapturePluginMode = "Standalone"
)

// +kubebuilder:validation:Enum=volume;stdout
type PluginOutputType string

const (
	PluginOutputTypeVolume PluginOutputType = "volume"
	PluginOutputTypeStdout PluginOutputType = "stdout"
)

// +kubebuilder:validation:Enum=jsonl;pcap
type PluginOutputFormat string

const (
	PluginOutputFormatJSONL PluginOutputFormat = "jsonl"
	PluginOutputFormatPCAP  PluginOutputFormat = "pcap"
)

// +kubebuilder:validation:Enum=Ready;Error
type CapturePluginPhase string

const (
	CapturePluginPhaseReady CapturePluginPhase = "Ready"
	CapturePluginPhaseError CapturePluginPhase = "Error"
)

// PluginContainerSpec defines the user-provided container that performs capture.
type PluginContainerSpec struct {
	Image string `json:"image"`
	// +optional
	Command []string `json:"command,omitempty"`
	// +optional
	Args []string `json:"args,omitempty"`
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`
}

// PluginPortSpec defines a port the plugin container exposes.
type PluginPortSpec struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	ContainerPort int32 `json:"containerPort"`
	// +optional
	// +kubebuilder:default=TCP
	Protocol corev1.Protocol `json:"protocol,omitempty"`
}

// PluginOutputSpec defines how the plugin emits captured data.
type PluginOutputSpec struct {
	// +kubebuilder:default=volume
	Type PluginOutputType `json:"type"`
	// Path inside the container where the plugin writes output files.
	// Required when type is "volume".
	// +optional
	Path string `json:"path,omitempty"`
	// +kubebuilder:default=jsonl
	Format PluginOutputFormat `json:"format"`
}

// PluginVolumeSpec defines additional volumes the plugin needs.
type PluginVolumeSpec struct {
	Name string `json:"name"`
	// +optional
	EmptyDir *corev1.EmptyDirVolumeSource `json:"emptyDir,omitempty"`
	// +optional
	HostPath *corev1.HostPathVolumeSource `json:"hostPath,omitempty"`
	// +optional
	ConfigMap *corev1.ConfigMapVolumeSource `json:"configMap,omitempty"`
	// +optional
	Secret *corev1.SecretVolumeSource `json:"secret,omitempty"`
	// +optional
	SizeLimit *resource.Quantity `json:"sizeLimit,omitempty"`
}

// CapturePluginSpec defines the desired state of CapturePlugin.
type CapturePluginSpec struct {
	Mode      CapturePluginMode   `json:"mode"`
	Container PluginContainerSpec `json:"container"`
	// +optional
	Ports []PluginPortSpec `json:"ports,omitempty"`
	Output PluginOutputSpec `json:"output"`
	// +optional
	Volumes []PluginVolumeSpec `json:"volumes,omitempty"`
}

// CapturePluginStatus defines the observed state of CapturePlugin.
type CapturePluginStatus struct {
	// +optional
	Phase CapturePluginPhase `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CapturePlugin defines a reusable, user-provided capture tool.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.container.image`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CapturePlugin struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CapturePluginSpec   `json:"spec"`
	Status CapturePluginStatus `json:"status,omitempty"`
}

// CapturePluginList contains a list of CapturePlugin resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CapturePluginList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CapturePlugin `json:"items"`
}

func init() {
	localSchemeBuilder.Register(&CapturePlugin{}, &CapturePluginList{})
}
