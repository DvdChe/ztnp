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

// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules,verbs=get;list;watch
// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ztnpv1alpha1 "github.com/DvdChe/ztnp/api/v1alpha1"
)

// SymmetricRuleReconciler reconciles a SymmetricRule object
type SymmetricRuleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules,verbs=get;list;watch
// +kubebuilder:rbac:groups=ztnp.io,resources=symmetricrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces;pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the SymmetricRule object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *SymmetricRuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	var sr ztnpv1alpha1.SymmetricRule
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name}, &sr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	fromCount, err := r.countPods(ctx, sr.Spec.From)
	if err != nil {
		return r.setStatus(ctx, &sr, false, "ValidationError", err.Error(), 0, 0)
	}
	toCount, err := r.countPods(ctx, sr.Spec.To)
	if err != nil {
		return r.setStatus(ctx, &sr, false, "ValidationError", err.Error(), 0, 0)
	}

	if fromCount == 0 {
		return r.setStatus(ctx, &sr, false, "EmptySelection(from)", "no pods match 'from' selectors", 0, toCount)
	}
	if toCount == 0 {
		return r.setStatus(ctx, &sr, false, "EmptySelection(to)", "no pods match 'to' selectors", fromCount, 0)
	}

	estimatedPolicies := int(2)

	reason, msg := "DryRun", "Dry-run: no NetworkPolicies will be created"
	if sr.Spec.Enforce {
		reason = "Planned"
		msg = "Enforce requested, but compilation not implemented yet"
	}
	logf.FromContext(ctx).Info("computed plan",
		"name", sr.Name, "fromPods", fromCount, "toPods", toCount, "reason", reason)

	if sr.Spec.Enforce {
		// 1) The target namespaces
		toNS, err := r.namespacesFor(ctx, sr.Spec.To.NamespaceSelector)
		if err != nil {
			return r.setStatus(ctx, &sr, false, "NamespaceResolveError(to)", err.Error(), fromCount, toCount)
		}
		// 2) The source namespaces
		fromNS, err := r.namespacesFor(ctx, sr.Spec.From.NamespaceSelector)
		if err != nil {
			return r.setStatus(ctx, &sr, false, "NamespaceResolveError(from)", err.Error(), fromCount, toCount)
		}

		// 3) Apply ingress in each target ns
		for _, ns := range toNS {
			np := renderIngressNP(ns, &sr)
			op, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
				mutateNetworkPolicy(np, renderIngressNP(ns, &sr))
				return nil
			})
			if err != nil {
				return r.setStatus(ctx, &sr, false, "ApplyError(ingress)", err.Error(), fromCount, toCount)
			}
			logf.FromContext(ctx).Info("ingress policy reconciled", "namespace", ns, "op", op)
		}

		// 4) Apply egress in each source ns
		for _, ns := range fromNS {
			np := renderEgressNP(ns, &sr)
			op, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
				mutateNetworkPolicy(np, renderEgressNP(ns, &sr))
				return nil
			})
			if err != nil {
				return r.setStatus(ctx, &sr, false, "ApplyError(ingress)", err.Error(), fromCount, toCount)
			}
			logf.FromContext(ctx).Info("ingress policy reconciled", "namespace", ns, "op", op)
		}

		// 5) Success
		return r.setStatus(ctx, &sr, true, "Success", "NetworkPolicies applied", fromCount, toCount, 2)
	}

	return r.setStatus(ctx, &sr, false, reason, msg, int(fromCount), int(toCount), estimatedPolicies)
}

func (r *SymmetricRuleReconciler) countPods(ctx context.Context, peer ztnpv1alpha1.SymmetricPeer) (int, error) {
	// 1) Résoudre les namespaces
	nsSel := labels.Everything()
	if peer.NamespaceSelector != nil {
		var err error
		nsSel, err = metav1.LabelSelectorAsSelector(peer.NamespaceSelector)
		if err != nil {
			return 0, err
		}
	}

	var nsList corev1.NamespaceList
	if err := r.List(ctx, &nsList, &client.ListOptions{LabelSelector: nsSel}); err != nil {
		return 0, err
	}

	// 2) Préparer le selector de pods
	podSel := labels.Everything()
	if peer.PodSelector != nil {
		var err error
		podSel, err = metav1.LabelSelectorAsSelector(peer.PodSelector)
		if err != nil {
			return 0, err
		}
	}

	// 3) Lister les pods dans chaque namespace retenu
	total := 0
	for _, ns := range nsList.Items {
		var pods corev1.PodList
		if err := r.List(ctx, &pods, &client.ListOptions{
			Namespace:     ns.Name,
			LabelSelector: podSel,
		}); err != nil {
			return 0, err
		}
		total += len(pods.Items)
	}
	return total, nil
}

func (r *SymmetricRuleReconciler) setStatus(
	ctx context.Context,
	sr *ztnpv1alpha1.SymmetricRule,
	compiled bool,
	reason, msg string,
	fromCount, toCount int,
	est ...int,
) (ctrl.Result, error) {
	estPolicies := int(2)
	if len(est) > 0 {
		estPolicies = est[0]
	}

	base := sr.DeepCopy()

	sr.Status.Compiled = compiled
	sr.Status.Reason = reason
	sr.Status.Message = msg
	sr.Status.Metrics.IngressPeers = toCount
	sr.Status.Metrics.EgressPeers = fromCount
	sr.Status.Metrics.EstimatedPolicies = estPolicies
	sr.Status.LastUpdate = metav1.NewTime(time.Now())

	if err := r.Status().Patch(ctx, sr, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *SymmetricRuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ztnpv1alpha1.SymmetricRule{}).
		Complete(r)
}

func renderIngressNP(ns string, sr *ztnpv1alpha1.SymmetricRule) *netv1.NetworkPolicy {
	np := &netv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("ztnp-%s-ingress", sr.Name),
			Namespace: ns,
			Labels: map[string]string{
				"ztnp.io/managed":   "true",
				"ztnp.io/symrule":   sr.Name,
				"ztnp.io/direction": "ingress",
			},
		},
		Spec: netv1.NetworkPolicySpec{
			PodSelector: lsOrEmpty(sr.Spec.To.PodSelector),
			PolicyTypes: []netv1.PolicyType{netv1.PolicyTypeIngress},
		},
	}
	// from peer (ns + pods)
	ip := netv1.NetworkPolicyIngressRule{}
	ip.From = []netv1.NetworkPolicyPeer{{
		NamespaceSelector: sr.Spec.From.NamespaceSelector,
		PodSelector:       sr.Spec.From.PodSelector,
	}}
	// ports (omit if empty => all)
	if len(sr.Spec.Ports) > 0 {
		for _, p := range sr.Spec.Ports {
			proto := corev1.ProtocolTCP
			if p.Protocol != "" {
				proto = p.Protocol
			}
			ip.Ports = append(ip.Ports, netv1.NetworkPolicyPort{
				Protocol: &proto,
				Port:     &intstr.IntOrString{Type: intstr.Int, IntVal: int32(p.Port)},
			})
		}
	}
	np.Spec.Ingress = []netv1.NetworkPolicyIngressRule{ip}
	return np
}

func renderEgressNP(ns string, sr *ztnpv1alpha1.SymmetricRule) *netv1.NetworkPolicy {
	np := &netv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("ztnp-%s-egress", sr.Name),
			Namespace: ns,
			Labels: map[string]string{
				"ztnp.io/managed":   "true",
				"ztnp.io/symrule":   sr.Name,
				"ztnp.io/direction": "egress",
			},
		},
		Spec: netv1.NetworkPolicySpec{
			PodSelector: lsOrEmpty(sr.Spec.From.PodSelector),
			PolicyTypes: []netv1.PolicyType{netv1.PolicyTypeEgress},
		},
	}
	er := netv1.NetworkPolicyEgressRule{}
	er.To = []netv1.NetworkPolicyPeer{{
		NamespaceSelector: sr.Spec.To.NamespaceSelector,
		PodSelector:       sr.Spec.To.PodSelector,
	}}
	if len(sr.Spec.Ports) > 0 {
		for _, p := range sr.Spec.Ports {
			proto := corev1.ProtocolTCP
			if p.Protocol != "" {
				proto = p.Protocol
			}
			er.Ports = append(er.Ports, netv1.NetworkPolicyPort{
				Protocol: &proto,
				Port:     &intstr.IntOrString{Type: intstr.Int, IntVal: int32(p.Port)},
			})
		}
	}
	np.Spec.Egress = []netv1.NetworkPolicyEgressRule{er}
	return np
}

func lsOrEmpty(ls *metav1.LabelSelector) metav1.LabelSelector {
	if ls != nil {
		return *ls
	}
	return metav1.LabelSelector{} // empty selector => all pods in the namespace
}

// helper pour résoudre les listes de namespaces à partir d'un NamespaceSelector
func (r *SymmetricRuleReconciler) namespacesFor(ctx context.Context, sel *metav1.LabelSelector) ([]string, error) {
	s := labels.Everything()
	if sel != nil {
		var err error
		s, err = metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return nil, err
		}
	}
	var nsList corev1.NamespaceList
	if err := r.List(ctx, &nsList, &client.ListOptions{LabelSelector: s}); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		out = append(out, ns.Name)
	}
	return out, nil
}

// mutateNetworkPolicy applique le "desired state" sur l'objet existant (np).
// - Remplace .Spec entièrement (idempotent, simple)
// - Met à jour les labels gérés ztnp.io/* en préservant les autres
func mutateNetworkPolicy(existing, desired *netv1.NetworkPolicy) {
	// 1) Labels : purge des labels gérés pour éviter les artefacts
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	for k := range existing.Labels {
		if strings.HasPrefix(k, "ztnp.io/") {
			delete(existing.Labels, k)
		}
	}
	// puis recopie des labels désirés (inclut ztnp.io/*)
	for k, v := range desired.Labels {
		existing.Labels[k] = v
	}

	// 2) Spec : remplacement complet
	existing.Spec = desired.Spec

	// (Annotations/OwnerRefs non touchés volontairement)
}
