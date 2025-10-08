/*
Copyright 2025 ztnp.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SymmetricPeer describes a set of pods or namespaces (same structure for From & To)
type SymmetricPeer struct {
	// NamespaceSelector selects namespaces by labels
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// PodSelector selects pods by labels within the selected namespaces
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`
	// +kubebuilder:validation:XValidation:rule="has(self.namespaceSelector) || has(self.podSelector)",message="at least one of namespaceSelector or podSelector must be set"
}

// NetworkPort defines one allowed port for the rule
type NetworkPort struct {

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int `json:"port"`

	// +kubebuilder:validation:Enum=TCP;UDP;SCTP
	// +kubebuilder:default=TCP
	// +optional
	Protocol corev1.Protocol `json:"protocol,omitempty"`
}

// SymmetricRuleSpec defines the desired state of SymmetricRule
type SymmetricRuleSpec struct {

	// From defines the source selector allowed to establish connection to the To selector.
	// combines NamespaceSelector and PodSelector.
	// +required
	From SymmetricPeer `json:"from"`

	// To defines the destination selector allowed to establish connection from the From selector.
	// combines NamespaceSelector and PodSelector.
	// +required
	To SymmetricPeer `json:"to"`

	// Ports defines the allowed ports for the rule.
	// Allows all ports if not specified
	// +optional
	Ports []NetworkPort `json:"ports,omitempty"`

	// Enforce defines if the rule should be enforced
	// false = dry-run mode
	// +kubebuilder:default=false
	// +optional
	Enforce bool `json:"enforce,omitempty"`

	// Prune defines if the resulting network policies should be pruned
	// +optional
	// +kubebuilder:default=true
	Prune bool `json:"prune,omitempty"`
}

// SymmetricRuleStatus defines the observed state of SymmetricRule.
type SymmetricRuleStatus struct {
	// True if the rule has been compiled and applied successfully
	Compiled bool `json:"compiled"`

	// Brief reason for the current state (DryRun, Success, Error, etc.)
	Reason string `json:"reason,omitempty"`

	// Detailed message explaining the state
	Message string `json:"message,omitempty"`

	// Some metrics for the compiled plan
	Metrics SymmetricRuleMetrics `json:"metrics,omitempty"`

	// Timestamp of the last update to the status
	LastUpdate metav1.Time `json:"lastUpdate,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type SymmetricRuleMetrics struct {
	IngressPeers      int `json:"ingressPeers,omitempty"`
	EgressPeers       int `json:"egressPeers,omitempty"`
	EstimatedPolicies int `json:"estimatedPolicies,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// SymmetricRule is the Schema for the symmetricrules API
// +kubebuilder:resource:shortName=sr;ztsr,scope=Cluster
// +kubebuilder:printcolumn:name="Compiled",type=boolean,JSONPath=`.status.compiled`,description="Applied"
// +kubebuilder:printcolumn:name="From",type=string,JSONPath=`.spec.from.podSelector.matchLabels.app`,description="Source app"
// +kubebuilder:printcolumn:name="To",type=string,JSONPath=`.spec.to.podSelector.matchLabels.app`,description="Target app"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`,description="Why"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type SymmetricRule struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of SymmetricRule
	// +required
	Spec SymmetricRuleSpec `json:"spec"`

	// status defines the observed state of SymmetricRule
	// +optional
	Status SymmetricRuleStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// SymmetricRuleList contains a list of SymmetricRule
type SymmetricRuleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SymmetricRule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SymmetricRule{}, &SymmetricRuleList{})
}
