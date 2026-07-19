package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const KindCaptureStorage = "CaptureStorage"

// +kubebuilder:validation:Enum=S3;GCS;EFS;EBS;RDS;Plugin
type CaptureStorageType string

const (
	CaptureStorageTypeS3     CaptureStorageType = "S3"
	CaptureStorageTypeGCS    CaptureStorageType = "GCS"
	CaptureStorageTypeEFS    CaptureStorageType = "EFS"
	CaptureStorageTypeEBS    CaptureStorageType = "EBS"
	CaptureStorageTypeRDS    CaptureStorageType = "RDS"
	CaptureStorageTypePlugin CaptureStorageType = "Plugin"
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
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	Bucket string `json:"bucket"`
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`
	// +optional
	Prefix               *string                       `json:"prefix,omitempty"`
	CredentialsSecretRef gwapiv1.SecretObjectReference `json:"credentialsSecretRef"`
}

// GCSConfig defines Google Cloud Storage settings.
type GCSConfig struct {
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=222
	Bucket string `json:"bucket"`
	// +optional
	Prefix               *string                       `json:"prefix,omitempty"`
	CredentialsSecretRef gwapiv1.SecretObjectReference `json:"credentialsSecretRef"`
}

// EFSConfig defines Amazon EFS storage settings.
type EFSConfig struct {
	// +kubebuilder:validation:MinLength=1
	FileSystemID string `json:"fileSystemID"`
	// +kubebuilder:validation:Pattern=`^/.*`
	MountPath string `json:"mountPath"`
	// +optional
	Directory *string `json:"directory,omitempty"`
}

// EBSConfig defines Amazon EBS storage settings.
type EBSConfig struct {
	// +kubebuilder:validation:MinLength=1
	VolumeID string `json:"volumeID"`
	// +kubebuilder:validation:Pattern=`^/.*`
	MountPath string `json:"mountPath"`
	// +optional
	Filesystem *string `json:"filesystem,omitempty"`
}

// RDSConfig stores captured requests as rows in a PostgreSQL-compatible
// database (Amazon RDS/Aurora or any Postgres). The agent creates the
// schema on first use; captured requests are queryable with SQL and
// replayable like any other backend.
type RDSConfig struct {
	// ConnectionSecretRef references a Secret whose "dsn" key holds the
	// PostgreSQL connection URL
	// (postgres://user:pass@host:5432/db?sslmode=require).
	ConnectionSecretRef gwapiv1.SecretObjectReference `json:"connectionSecretRef"`
	// Table optionally overrides the captured-requests table name.
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-zA-Z_][a-zA-Z0-9_]*$`
	Table *string `json:"table,omitempty"`
}

// PluginConfig defines Go plugin storage settings.
type PluginConfig struct {
	// +kubebuilder:validation:MinLength=1
	Path string `json:"path"`
	// +optional
	Symbol *string `json:"symbol,omitempty"`
	// +optional
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// CaptureStorageSpec defines the desired state of CaptureStorage.
//
// +kubebuilder:validation:XValidation:rule="self.type != 'S3' || has(self.s3)",message="s3 configuration is required when type is S3"
// +kubebuilder:validation:XValidation:rule="self.type != 'GCS' || has(self.gcs)",message="gcs configuration is required when type is GCS"
// +kubebuilder:validation:XValidation:rule="self.type != 'EFS' || has(self.efs)",message="efs configuration is required when type is EFS"
// +kubebuilder:validation:XValidation:rule="self.type != 'EBS' || has(self.ebs)",message="ebs configuration is required when type is EBS"
// +kubebuilder:validation:XValidation:rule="self.type != 'RDS' || has(self.rds)",message="rds configuration is required when type is RDS"
// +kubebuilder:validation:XValidation:rule="self.type != 'Plugin' || has(self.plugin)",message="plugin configuration is required when type is Plugin"
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
	RDS *RDSConfig `json:"rds,omitempty"`
	// +optional
	Plugin *PluginConfig `json:"plugin,omitempty"`
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
