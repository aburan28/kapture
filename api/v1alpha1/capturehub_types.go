package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const KindCaptureHub = "CaptureHub"

// +kubebuilder:validation:Enum=mTLS
type HubAuthenticationType string

const HubAuthenticationTypeMTLS HubAuthenticationType = "mTLS"

// CaptureHubTLSConfig defines TLS settings for the hub gRPC server.
type CaptureHubTLSConfig struct {
	CertSecretRef gwapiv1.SecretObjectReference `json:"certSecretRef"`
}

// CaptureHubAuthentication defines hub authentication settings.
type CaptureHubAuthentication struct {
	Type HubAuthenticationType `json:"type"`
}

// CaptureHubSpec defines the desired state of CaptureHub.
type CaptureHubSpec struct {
	GRPCAddress string `json:"grpcAddress"`
	// +optional
	TLS *CaptureHubTLSConfig `json:"tls,omitempty"`
	// +optional
	Authentication *CaptureHubAuthentication `json:"authentication,omitempty"`
}

// CaptureHubSpokeStatus summarizes an attached spoke cluster.
type CaptureHubSpokeStatus struct {
	Name string `json:"name"`
	// +optional
	LastHeartbeat *metav1.Time `json:"lastHeartbeat,omitempty"`
	// +optional
	ActiveCaptures int32 `json:"activeCaptures,omitempty"`
}

// CaptureHubStatus defines the observed state of CaptureHub.
type CaptureHubStatus struct {
	// +optional
	ConnectedSpokes int32 `json:"connectedSpokes,omitempty"`
	// +optional
	ActiveCaptures int32 `json:"activeCaptures,omitempty"`
	// +optional
	Spokes []CaptureHubSpokeStatus `json:"spokes,omitempty"`
}

// CaptureHub defines hub-wide capture controller configuration.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ConnectedSpokes",type=integer,JSONPath=`.status.connectedSpokes`
// +kubebuilder:printcolumn:name="ActiveCaptures",type=integer,JSONPath=`.status.activeCaptures`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CaptureHub struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CaptureHubSpec   `json:"spec"`
	Status CaptureHubStatus `json:"status,omitempty"`
}

// CaptureHubList contains a list of CaptureHub resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CaptureHubList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CaptureHub `json:"items"`
}

func init() {
	localSchemeBuilder.Register(&CaptureHub{}, &CaptureHubList{})
}
