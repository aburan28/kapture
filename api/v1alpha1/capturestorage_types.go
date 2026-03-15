package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const KindCaptureStorage = "CaptureStorage"

// +kubebuilder:validation:Enum=S3;GCS;EFS;EBS
type CaptureStorageType string

const (
	CaptureStorageTypeS3  CaptureStorageType = "S3"
	CaptureStorageTypeGCS CaptureStorageType = "GCS"
	CaptureStorageTypeEFS CaptureStorageType = "EFS"
	CaptureStorageTypeEBS CaptureStorageType = "EBS"
)

// +kubebuilder:validation:Enum=Ready;Error
type CaptureStoragePhase string

const (
	CaptureStoragePhaseReady CaptureStoragePhase = "Ready"
	CaptureStoragePhaseError CaptureStoragePhase = "Error"
)

// RetentionPolicy defines common capture retention settings.
type RetentionPolicy struct {
	// +optional
	MaxAge *string `json:"maxAge,omitempty"`
	// +optional
	MaxSize *resource.Quantity `json:"maxSize,omitempty"`
}

// S3Config defines Amazon S3 storage settings.
type S3Config struct {
	Bucket string `json:"bucket"`
	Region string `json:"region"`
	// +optional
	Prefix               *string                       `json:"prefix,omitempty"`
	CredentialsSecretRef gwapiv1.SecretObjectReference `json:"credentialsSecretRef"`
}

// GCSConfig defines Google Cloud Storage settings.
type GCSConfig struct {
	Bucket string `json:"bucket"`
	// +optional
	Prefix               *string                       `json:"prefix,omitempty"`
	CredentialsSecretRef gwapiv1.SecretObjectReference `json:"credentialsSecretRef"`
}

// EFSConfig defines Amazon EFS storage settings.
type EFSConfig struct {
	FileSystemID string `json:"fileSystemID"`
	MountPath    string `json:"mountPath"`
	// +optional
	Directory *string `json:"directory,omitempty"`
}

// EBSConfig defines Amazon EBS storage settings.
type EBSConfig struct {
	VolumeID  string `json:"volumeID"`
	MountPath string `json:"mountPath"`
	// +optional
	Filesystem *string `json:"filesystem,omitempty"`
}

// CaptureStorageSpec defines the desired state of CaptureStorage.
type CaptureStorageSpec struct {
	Type CaptureStorageType `json:"type"`
	// +optional
	S3 *S3Config `json:"s3,omitempty"`
	// +optional
	GCS *GCSConfig `json:"gcs,omitempty"`
	// +optional
	EFS *EFSConfig `json:"efs,omitempty"`
	// +optional
	EBS *EBSConfig `json:"ebs,omitempty"`
	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`
}

// CaptureStorageStatus defines the observed state of CaptureStorage.
type CaptureStorageStatus struct {
	// +optional
	Phase CaptureStoragePhase `json:"phase,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CaptureStorage defines a reusable capture storage backend.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CaptureStorage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CaptureStorageSpec   `json:"spec"`
	Status CaptureStorageStatus `json:"status,omitempty"`
}

// CaptureStorageList contains a list of CaptureStorage resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CaptureStorageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CaptureStorage `json:"items"`
}

func init() {
	localSchemeBuilder.Register(&CaptureStorage{}, &CaptureStorageList{})
}
