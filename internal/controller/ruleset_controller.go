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

package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ztnpv1alpha1 "github.com/DvdChe/ztnp/api/v1alpha1"
)

const rulesetFinalizerName = "ztnp.io/ruleset-finalizer"

// RuleSetReconciler reconciles a RuleSet object
type RuleSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// RBAC permissions for the controller
// +kubebuilder:rbac:groups=ztnp.io,resources=rulesets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ztnp.io,resources=rulesets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ztnp.io,resources=rulesets/finalizers,verbs=update
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles RuleSet events and ensures NetworkPolicies are in sync.
func (r *RuleSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the RuleSet
	var rs ztnpv1alpha1.RuleSet
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, &rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("reconciling RuleSet", "namespace", rs.Namespace, "name", rs.Name)

	// Handle deletion
	if !rs.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&rs, rulesetFinalizerName) {
			if err := r.handleDelete(ctx, &rs); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(&rs, rulesetFinalizerName) {
		controllerutil.AddFinalizer(&rs, rulesetFinalizerName)
		if err := r.Update(ctx, &rs); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Build and apply the NetworkPolicy
	if err := r.ensureNetworkPolicy(ctx, &rs); err != nil {
		log.Error(err, "failed to ensure NetworkPolicy")
		return ctrl.Result{}, err
	}

	// Update status
	if err := r.updateStatus(ctx, &rs); err != nil {
		log.Error(err, "failed to update status")
	}

	log.Info("RuleSet reconciled",
		"namespace", rs.Namespace,
		"name", rs.Name,
		"ingressRules", len(rs.Spec.Ingress.Rules),
		"egressRules", len(rs.Spec.Egress.Rules))

	return ctrl.Result{}, nil
}

// handleDelete removes the NetworkPolicy and finalizer.
func (r *RuleSetReconciler) handleDelete(ctx context.Context, rs *ztnpv1alpha1.RuleSet) error {
	log := logf.FromContext(ctx)
	log.Info("cleaning up RuleSet", "namespace", rs.Namespace, "name", rs.Name)

	// Delete the associated NetworkPolicy
	np := &netv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: rs.Namespace,
			Name:      networkPolicyName(rs.Name),
		},
	}
	if err := r.Delete(ctx, np); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete NetworkPolicy: %w", err)
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(rs, rulesetFinalizerName)
	return r.Update(ctx, rs)
}

// ensureNetworkPolicy creates or updates the NetworkPolicy for this RuleSet.
func (r *RuleSetReconciler) ensureNetworkPolicy(ctx context.Context, rs *ztnpv1alpha1.RuleSet) error {
	np := &netv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: rs.Namespace,
			Name:      networkPolicyName(rs.Name),
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		// Set owner reference so NetworkPolicy is garbage collected with RuleSet
		if err := controllerutil.SetControllerReference(rs, np, r.Scheme); err != nil {
			return err
		}

		// Set labels
		if np.Labels == nil {
			np.Labels = make(map[string]string)
		}
		np.Labels["ztnp.io/managed"] = "true"
		np.Labels["ztnp.io/ruleset"] = rs.Name

		// Build the NetworkPolicy spec
		np.Spec = r.buildNetworkPolicySpec(rs)

		return nil
	})

	return err
}

// buildNetworkPolicySpec converts a RuleSet to a NetworkPolicy spec.
func (r *RuleSetReconciler) buildNetworkPolicySpec(rs *ztnpv1alpha1.RuleSet) netv1.NetworkPolicySpec {
	spec := netv1.NetworkPolicySpec{
		// Target pods (from attachedTo)
		PodSelector: metav1.LabelSelector{},
		PolicyTypes: []netv1.PolicyType{},
	}

	// Set pod selector if specified
	if rs.Spec.AttachedTo.PodSelector != nil {
		spec.PodSelector = *rs.Spec.AttachedTo.PodSelector
	}

	// Build ingress rules
	if len(rs.Spec.Ingress.Rules) > 0 {
		spec.PolicyTypes = append(spec.PolicyTypes, netv1.PolicyTypeIngress)
		spec.Ingress = r.buildIngressRules(rs.Spec.Ingress.Rules)
	}

	// Build egress rules
	if len(rs.Spec.Egress.Rules) > 0 {
		spec.PolicyTypes = append(spec.PolicyTypes, netv1.PolicyTypeEgress)
		spec.Egress = r.buildEgressRules(rs.Spec.Egress.Rules)
	}

	return spec
}

// buildIngressRules converts RuleSet ingress rules to NetworkPolicy ingress rules.
func (r *RuleSetReconciler) buildIngressRules(rules []ztnpv1alpha1.Rule) []netv1.NetworkPolicyIngressRule {
	npRules := make([]netv1.NetworkPolicyIngressRule, 0, len(rules))

	for _, rule := range rules {
		npRule := netv1.NetworkPolicyIngressRule{
			From:  []netv1.NetworkPolicyPeer{rule.Peer},
			Ports: convertPorts(rule.Ports),
		}
		npRules = append(npRules, npRule)
	}

	return npRules
}

// buildEgressRules converts RuleSet egress rules to NetworkPolicy egress rules.
func (r *RuleSetReconciler) buildEgressRules(rules []ztnpv1alpha1.Rule) []netv1.NetworkPolicyEgressRule {
	npRules := make([]netv1.NetworkPolicyEgressRule, 0, len(rules))

	for _, rule := range rules {
		npRule := netv1.NetworkPolicyEgressRule{
			To:    []netv1.NetworkPolicyPeer{rule.Peer},
			Ports: convertPorts(rule.Ports),
		}
		npRules = append(npRules, npRule)
	}

	return npRules
}

// convertPorts converts NetworkPort to NetworkPolicyPort.
func convertPorts(ports []ztnpv1alpha1.NetworkPort) []netv1.NetworkPolicyPort {
	if len(ports) == 0 {
		// Empty means all ports allowed
		return nil
	}

	npPorts := make([]netv1.NetworkPolicyPort, 0, len(ports))
	for _, p := range ports {
		port := intstr.FromInt(p.Port)
		protocol := p.Protocol
		if protocol == "" {
			protocol = corev1.ProtocolTCP
		}
		npPorts = append(npPorts, netv1.NetworkPolicyPort{
			Port:     &port,
			Protocol: &protocol,
		})
	}

	return npPorts
}

// networkPolicyName generates the NetworkPolicy name from the RuleSet name.
func networkPolicyName(rulesetName string) string {
	return fmt.Sprintf("ztnp-%s", rulesetName)
}

// updateStatus updates the RuleSet status with rule counts.
func (r *RuleSetReconciler) updateStatus(ctx context.Context, rs *ztnpv1alpha1.RuleSet) error {
	rs.Status.IngressCount = len(rs.Spec.Ingress.Rules)
	rs.Status.EgressCount = len(rs.Spec.Egress.Rules)

	return r.Status().Update(ctx, rs)
}

// SetupWithManager sets up the controller with the Manager.
func (r *RuleSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ztnpv1alpha1.RuleSet{}).
		Watches(
			&netv1.NetworkPolicy{},
			handler.EnqueueRequestsFromMapFunc(r.findRuleSetForNetworkPolicy),
		).
		Named("ruleset").
		Complete(r)
}

// findRuleSetForNetworkPolicy maps a NetworkPolicy event to the RuleSet that owns it.
// This ensures that when a NetworkPolicy is deleted, the owning RuleSet is re-reconciled to recreate it.
func (r *RuleSetReconciler) findRuleSetForNetworkPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	np := obj.(*netv1.NetworkPolicy)

	// Only handle NetworkPolicies managed by ztnp
	if np.Labels == nil || np.Labels["ztnp.io/managed"] != "true" {
		return nil
	}

	// Get the RuleSet name from the label
	rulesetName, ok := np.Labels["ztnp.io/ruleset"]
	if !ok {
		return nil
	}

	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Namespace: np.Namespace,
				Name:      rulesetName,
			},
		},
	}
}
