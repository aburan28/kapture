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
//
// +kubebuilder:validation:XValidation:rule="!has(self.authentication) || self.authentication.type != 'mTLS' || has(self.tls)",message="tls.certSecretRef is required when authentication.type is mTLS"
type CaptureHubSpec struct {
	// GRPCAddress is the listen address for the hub gRPC server, in Go
	// net.Listen form: ":9443" or "host:port".
	// +kubebuilder:validation:MinLength=2
	// +kubebuilder:validation:Pattern=`^.*:[0-9]+$`
	GRPCAddress string `json:"grpcAddress"`
	// +optional
	TLS *CaptureHubTLSConfig `json:"tls,omitempty"`
	// +optional
	Authentication *CaptureHubAuthentication `json:"authentication,omitempty"`
}

// CaptureHubSpokeStatus summarizes an attached spoke cluster.
type CaptureHubSpokeStatus struct {
	Name string `json:"name"`
	// Cell is the deployment cell the spoke registered under.
	// +optional
	Cell string `json:"cell,omitempty"`
	// +optional
	LastHeartbeat *metav1.Time `json:"lastHeartbeat,omitempty"`
	// +optional
	ActiveCaptures int32 `json:"activeCaptures,omitempty"`
	// +optional
	ActiveReplays int32 `json:"activeReplays,omitempty"`
}

// CaptureHubCellStatus aggregates the spokes registered under one cell.
type CaptureHubCellStatus struct {
	Name string `json:"name"`
	// +optional
	ConnectedSpokes int32 `json:"connectedSpokes,omitempty"`
	// +optional
	TotalSpokes int32 `json:"totalSpokes,omitempty"`
	// +optional
	ActiveCaptures int32 `json:"activeCaptures,omitempty"`
	// +optional
	ActiveReplays int32 `json:"activeReplays,omitempty"`
}

// CaptureHubConditionActive reports whether this CaptureHub drives the
// singleton gRPC server. When several CaptureHubs exist, only the oldest
// (name as tiebreak) is Active; the others carry reason NotAuthoritative.
const CaptureHubConditionActive = "Active"

// CaptureHubStatus defines the observed state of CaptureHub.
type CaptureHubStatus struct {
	// +optional
	ConnectedSpokes int32 `json:"connectedSpokes,omitempty"`
	// +optional
	ActiveCaptures int32 `json:"activeCaptures,omitempty"`
	// +optional
	ActiveReplays int32 `json:"activeReplays,omitempty"`
	// +optional
	Spokes []CaptureHubSpokeStatus `json:"spokes,omitempty"`
	// Cells aggregates connected spokes by their registered cell.
	// +optional
	Cells []CaptureHubCellStatus `json:"cells,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
