/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ValkeyClusterSpec defines the desired state of ValkeyCluster
type ValkeyClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +kubebuilder:validation:Minimum=1
	// Size defines the number of shards in the valkey cluster
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Shards int32 `json:"shards,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// Size defines the number of replicas per shard in the cluster
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Replicas int32 `json:"replicas,omitempty"`
}

// ValkeyClusterStatus defines the observed state of ValkeyCluster
type ValkeyClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Conditions store the status conditions of the ValkeyCluster instances
	// +operator-sdk:csv:customresourcedefinitions:type=status
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ValkeyCluster is the Schema for the valkeyclusters API
type ValkeyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ValkeyClusterSpec   `json:"spec,omitempty"`
	Status ValkeyClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyCluster{}, &ValkeyClusterList{})
}
