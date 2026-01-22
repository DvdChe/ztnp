package v1alpha1

import (
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Rule is the contribution of a SymmetricRule to this RuleSet.
// A Rule = an opposite peer (+ ports) for THIS RuleSet.
// The identity of a Rule is "by" (name of the SR).
type Rule struct {
	// By = name of the SymmetricRule that originated this rule.
	// +kubebuilder:validation:MinLength=1
	By string `json:"by"`

	// Opposite peer (Ingress: authorized sources; Egress: authorized destinations).
	Peer netv1.NetworkPolicyPeer `json:"peer"`

	// Authorized ports for THIS peer.
	// Empty => all ports (ALL).
	// +optional
	Ports []NetworkPort `json:"ports,omitempty"`
}

// DirectionRules groups the rules of a direction.
// Semantic map by "by" to guarantee at most one rule per SR.
type DirectionRules struct {
	// +optional
	// +kubebuilder:validation:Type=array
	// +listType=map
	// +listMapKey=by
	Rules []Rule `json:"rules,omitempty"`
}

// AttachedTo (a.k.a. target) = pods targeted by THIS RuleSet.
// Since the RuleSet is namespaced, this selector applies within this namespace.
type AttachedTo struct {
	// Nil => all pods in the namespace.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`
}

// RuleSetSpec: desired state.
// 1 RuleSet = 1 target (attachedTo). The ingress/egress rules
// list the opposite peers and where they come from (by).
// +kubebuilder:resource:scope=Namespaced,shortName=rs
// +kubebuilder:printcolumn:name="Ing",type=integer,description="Ingress rules",JSONPath=`.status.ingressCount`
// +kubebuilder:printcolumn:name="Egr",type=integer,description="Egress rules",JSONPath=`.status.egressCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type RuleSetSpec struct {
	// attachedTo designates the pods targeted by THIS RuleSet (the target).
	AttachedTo AttachedTo `json:"attachedTo"`

	// Ingress rules (authorized sources to the target).
	// +optional
	Ingress DirectionRules `json:"ingress,omitempty"`

	// Egress rules (authorized destinations from the target).
	// +optional
	Egress DirectionRules `json:"egress,omitempty"`
}

// RuleSetStatus: observed state (counters + conditions).
type RuleSetStatus struct {
	// +optional
	IngressCount int `json:"ingressCount,omitempty"`
	// +optional
	EgressCount int `json:"egressCount,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type RuleSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RuleSetSpec   `json:"spec"`
	Status RuleSetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RuleSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RuleSet `json:"items"`
}
