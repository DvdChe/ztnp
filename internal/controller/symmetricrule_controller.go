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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ztnpv1alpha1 "github.com/DvdChe/ztnp/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// finalizerName is used to ensure cleanup before the SymmetricRule is deleted.
const finalizerName = "ztnp.io/symmetricrule-finalizer"

// SymmetricRuleReconciler reconciles a SymmetricRule object.
// It creates RuleSets in target namespaces to enforce network policies.
type SymmetricRuleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// RBAC permissions for the controller
// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=ztnp.io,resources=rulesets,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles SymmetricRule events and ensures RuleSets are in sync.
func (r *SymmetricRuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Step 2: Fetch the SymmetricRule
	// SymmetricRule is cluster-scoped, so we only need the Name (no Namespace)
	var sr ztnpv1alpha1.SymmetricRule
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name}, &sr); err != nil {
		// If the resource is not found, it was deleted - nothing to do
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("reconciling SymmetricRule", "name", sr.Name)

	// Step 3: Handle deletion - check if the resource is being deleted
	if !sr.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&sr, finalizerName) {
			// Run cleanup logic before allowing deletion
			if err := r.handleDelete(ctx, &sr); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure our finalizer is present (so we can cleanup on deletion)
	if !controllerutil.ContainsFinalizer(&sr, finalizerName) {
		controllerutil.AddFinalizer(&sr, finalizerName)
		if err := r.Update(ctx, &sr); err != nil {
			return ctrl.Result{}, err
		}
		// Requeue to continue with the rest of the reconciliation
		return ctrl.Result{Requeue: true}, nil
	}

	// Step 5: Create/update RuleSets
	// Skip if not enforcing (dry-run mode)
	if !sr.Spec.Enforce {
		log.Info("dry-run mode, skipping RuleSet creation", "name", sr.Name)
		_ = r.updateStatus(ctx, &sr, false, "DryRun", "enforce=false, RuleSets not created")
		return ctrl.Result{}, nil
	}

	// Resolve "To" namespaces (where we create Ingress RuleSets)
	toNamespaces, err := r.listNamespaces(ctx, sr.Spec.To.NamespaceSelector)
	if err != nil {
		_ = r.updateStatus(ctx, &sr, false, "NamespaceResolveError", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to list 'to' namespaces: %w", err)
	}

	// Resolve "From" namespaces (where we create Egress RuleSets)
	fromNamespaces, err := r.listNamespaces(ctx, sr.Spec.From.NamespaceSelector)
	if err != nil {
		_ = r.updateStatus(ctx, &sr, false, "NamespaceResolveError", err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to list 'from' namespaces: %w", err)
	}

	// Create Ingress RuleSets in "To" namespaces
	// These allow traffic FROM the "From" peer TO the "To" peer
	for _, ns := range toNamespaces {
		if err := r.ensureIngressRuleSet(ctx, &sr, ns); err != nil {
			_ = r.updateStatus(ctx, &sr, false, "RuleSetError", fmt.Sprintf("ingress in %s: %v", ns, err))
			return ctrl.Result{}, fmt.Errorf("failed to ensure ingress RuleSet in %s: %w", ns, err)
		}
	}

	// Create Egress RuleSets in "From" namespaces
	// These allow traffic FROM the "From" peer TO the "To" peer
	for _, ns := range fromNamespaces {
		if err := r.ensureEgressRuleSet(ctx, &sr, ns); err != nil {
			_ = r.updateStatus(ctx, &sr, false, "RuleSetError", fmt.Sprintf("egress in %s: %v", ns, err))
			return ctrl.Result{}, fmt.Errorf("failed to ensure egress RuleSet in %s: %w", ns, err)
		}
	}

	// Success - update status
	message := fmt.Sprintf("RuleSets synchronized: %d ingress, %d egress", len(toNamespaces), len(fromNamespaces))
	if err := r.updateStatus(ctx, &sr, true, "Synchronized", message); err != nil {
		log.Error(err, "failed to update status")
	}

	log.Info("RuleSets synchronized",
		"name", sr.Name,
		"ingressNamespaces", len(toNamespaces),
		"egressNamespaces", len(fromNamespaces))

	return ctrl.Result{}, nil
}

// handleDelete removes all rules contributed by this SymmetricRule from RuleSets,
// then removes the finalizer to allow deletion.
func (r *SymmetricRuleReconciler) handleDelete(ctx context.Context, sr *ztnpv1alpha1.SymmetricRule) error {
	log := logf.FromContext(ctx)
	log.Info("cleaning up SymmetricRule", "name", sr.Name)

	// Clean up Ingress RuleSets in "To" namespaces
	toNamespaces, _ := r.listNamespaces(ctx, sr.Spec.To.NamespaceSelector)
	for _, ns := range toNamespaces {
		rsName := computeRuleSetName(ns, sr.Spec.To.PodSelector)
		if err := r.removeRuleFromRuleSet(ctx, ns, rsName, sr.Name, true); err != nil {
			return fmt.Errorf("failed to clean ingress RuleSet %s/%s: %w", ns, rsName, err)
		}
	}

	// Clean up Egress RuleSets in "From" namespaces
	fromNamespaces, _ := r.listNamespaces(ctx, sr.Spec.From.NamespaceSelector)
	for _, ns := range fromNamespaces {
		rsName := computeRuleSetName(ns, sr.Spec.From.PodSelector)
		if err := r.removeRuleFromRuleSet(ctx, ns, rsName, sr.Name, false); err != nil {
			return fmt.Errorf("failed to clean egress RuleSet %s/%s: %w", ns, rsName, err)
		}
	}

	log.Info("cleanup complete", "name", sr.Name)

	// Remove the finalizer to allow Kubernetes to delete the resource
	controllerutil.RemoveFinalizer(sr, finalizerName)
	return r.Update(ctx, sr)
}

// removeRuleFromRuleSet removes a rule from a RuleSet, and deletes the RuleSet if empty.
// isIngress indicates whether to remove from Ingress rules (true) or Egress rules (false).
func (r *SymmetricRuleReconciler) removeRuleFromRuleSet(ctx context.Context, namespace, name, ruleBy string, isIngress bool) error {
	var rs ztnpv1alpha1.RuleSet
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &rs)
	if err != nil {
		// RuleSet doesn't exist, nothing to clean
		return client.IgnoreNotFound(err)
	}

	// Remove the rule
	if isIngress {
		removeRule(&rs.Spec.Ingress.Rules, ruleBy)
	} else {
		removeRule(&rs.Spec.Egress.Rules, ruleBy)
	}

	// If RuleSet is now empty (no ingress and no egress rules), delete it
	if len(rs.Spec.Ingress.Rules) == 0 && len(rs.Spec.Egress.Rules) == 0 {
		return r.Delete(ctx, &rs)
	}

	// Otherwise, update the RuleSet
	return r.Update(ctx, &rs)
}

// removeRule removes a rule with the given "by" identifier from the rules list.
func removeRule(rules *[]ztnpv1alpha1.Rule, by string) {
	filtered := (*rules)[:0]
	for _, rule := range *rules {
		if rule.By != by {
			filtered = append(filtered, rule)
		}
	}
	*rules = filtered
}

// listNamespaces returns the names of namespaces matching the given label selector.
// If the selector is nil, all namespaces are returned.
func (r *SymmetricRuleReconciler) listNamespaces(ctx context.Context, sel *metav1.LabelSelector) ([]string, error) {
	// Convert LabelSelector to labels.Selector
	// If sel is nil, labels.Everything() matches all namespaces
	labelSel := labels.Everything()
	if sel != nil {
		var err error
		labelSel, err = metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return nil, err
		}
	}

	// List namespaces matching the selector
	var nsList corev1.NamespaceList
	if err := r.List(ctx, &nsList, client.MatchingLabelsSelector{Selector: labelSel}); err != nil {
		return nil, err
	}

	// Extract namespace names
	names := make([]string, len(nsList.Items))
	for i, ns := range nsList.Items {
		names[i] = ns.Name
	}
	return names, nil
}

// ensureIngressRuleSet creates or updates a RuleSet in the given namespace
// to allow ingress traffic from the "From" peer.
func (r *SymmetricRuleReconciler) ensureIngressRuleSet(ctx context.Context, sr *ztnpv1alpha1.SymmetricRule, namespace string) error {
	// Compute a stable name for the RuleSet based on namespace and pod selector
	rsName := computeRuleSetName(namespace, sr.Spec.To.PodSelector)

	rs := &ztnpv1alpha1.RuleSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      rsName,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, rs, func() error {
		// Set labels for identification
		if rs.Labels == nil {
			rs.Labels = make(map[string]string)
		}
		rs.Labels["ztnp.io/managed"] = "true"

		// Set the target pods (attachedTo)
		rs.Spec.AttachedTo.PodSelector = sr.Spec.To.PodSelector

		// Build the peer that is allowed to send traffic
		peer := netv1.NetworkPolicyPeer{
			NamespaceSelector: sr.Spec.From.NamespaceSelector,
			PodSelector:       sr.Spec.From.PodSelector,
		}

		// Upsert the rule (identified by the SymmetricRule name)
		upsertRule(&rs.Spec.Ingress.Rules, sr.Name, peer, sr.Spec.Ports)

		return nil
	})

	return err
}

// ensureEgressRuleSet creates or updates a RuleSet in the given namespace
// to allow egress traffic to the "To" peer.
func (r *SymmetricRuleReconciler) ensureEgressRuleSet(ctx context.Context, sr *ztnpv1alpha1.SymmetricRule, namespace string) error {
	// Compute a stable name for the RuleSet based on namespace and pod selector
	rsName := computeRuleSetName(namespace, sr.Spec.From.PodSelector)

	rs := &ztnpv1alpha1.RuleSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      rsName,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, rs, func() error {
		// Set labels for identification
		if rs.Labels == nil {
			rs.Labels = make(map[string]string)
		}
		rs.Labels["ztnp.io/managed"] = "true"

		// Set the target pods (attachedTo)
		rs.Spec.AttachedTo.PodSelector = sr.Spec.From.PodSelector

		// Build the peer that traffic is allowed to go to
		peer := netv1.NetworkPolicyPeer{
			NamespaceSelector: sr.Spec.To.NamespaceSelector,
			PodSelector:       sr.Spec.To.PodSelector,
		}

		// Upsert the rule (identified by the SymmetricRule name)
		upsertRule(&rs.Spec.Egress.Rules, sr.Name, peer, sr.Spec.Ports)

		return nil
	})

	return err
}

// computeRuleSetName generates a stable, deterministic name for a RuleSet.
// The name is based on the namespace and pod selector to ensure uniqueness.
func computeRuleSetName(namespace string, podSelector *metav1.LabelSelector) string {
	log := logf.FromContext(context.Background())
	// Create a string representation of the selector
	selectorStr := ""
	if podSelector != nil {
		selectorStr = fmt.Sprintf("%v", podSelector)
	}
	prefixName := ""
	for k, v := range podSelector.MatchLabels {
		prefixName += fmt.Sprintf("%s-%s", k, v)
	}
	log.Info("computing RuleSet name", "namespace", namespace, "podSelector", podSelector)
	log.Info("prefixName", "prefixName", prefixName)
	// Hash to keep the name short and valid
	input := fmt.Sprintf("%s/%s", namespace, selectorStr)
	hash := sha256.Sum256([]byte(input))
	shortHash := hex.EncodeToString(hash[:])[:12]

	return fmt.Sprintf("%s-%s", prefixName, shortHash)
}

// upsertRule adds or updates a rule in the rules list.
// Rules are identified by the "By" field (the SymmetricRule name).
func upsertRule(rules *[]ztnpv1alpha1.Rule, by string, peer netv1.NetworkPolicyPeer, ports []ztnpv1alpha1.NetworkPort) {
	// Look for existing rule from this SymmetricRule
	for i := range *rules {
		if (*rules)[i].By == by {
			// Update existing rule
			(*rules)[i].Peer = peer
			(*rules)[i].Ports = ports
			return
		}
	}

	// Add new rule
	*rules = append(*rules, ztnpv1alpha1.Rule{
		By:    by,
		Peer:  peer,
		Ports: ports,
	})
}

// updateStatus updates the SymmetricRule status with the given values.
// It uses the status subresource to avoid conflicts with spec updates.
func (r *SymmetricRuleReconciler) updateStatus(ctx context.Context, sr *ztnpv1alpha1.SymmetricRule, compiled bool, reason, message string) error {
	sr.Status.Compiled = compiled
	sr.Status.Reason = reason
	sr.Status.Message = message
	sr.Status.LastUpdate = metav1.Now()

	return r.Status().Update(ctx, sr)
}

// SetupWithManager registers the controller with the manager.
func (r *SymmetricRuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ztnpv1alpha1.SymmetricRule{}).
		Watches(
			&ztnpv1alpha1.RuleSet{},
			handler.EnqueueRequestsFromMapFunc(r.findSymmetricRulesForRuleSet),
		).
		Complete(r)
}

// findSymmetricRulesForRuleSet maps a RuleSet event to the SymmetricRules that should be reconciled.
// This ensures that when a RuleSet is deleted, the owning SymmetricRules are re-reconciled to recreate it.
func (r *SymmetricRuleReconciler) findSymmetricRulesForRuleSet(ctx context.Context, obj client.Object) []reconcile.Request {
	rs := obj.(*ztnpv1alpha1.RuleSet)

	// Collect unique SymmetricRule names from the "by" field in rules
	srNames := make(map[string]struct{})
	for _, rule := range rs.Spec.Ingress.Rules {
		srNames[rule.By] = struct{}{}
	}
	for _, rule := range rs.Spec.Egress.Rules {
		srNames[rule.By] = struct{}{}
	}

	// Build reconcile requests for each SymmetricRule
	requests := make([]reconcile.Request, 0, len(srNames))
	for name := range srNames {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name}, // cluster-scoped, no namespace
		})
	}

	return requests
}
