/*
Copyright The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

// nodeStatusChangedPredicate maps NodeReadinessRules to Nodes.
func nodeStatusChangedPredicate(ctx context.Context) predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			log := ctrl.LoggerFrom(ctx)
			oldNode := e.ObjectOld.(*corev1.Node)
			newNode := e.ObjectNew.(*corev1.Node)

			conditionsChanged := !conditionsEqual(oldNode.Status.Conditions, newNode.Status.Conditions)
			taintsChanged := !taintsEqual(oldNode.Spec.Taints, newNode.Spec.Taints)
			labelsChanged := !labelsEqual(oldNode.Labels, newNode.Labels)

			shouldReconcile := conditionsChanged || taintsChanged || labelsChanged

			if shouldReconcile {
				log.V(4).Info("Processing node update event",
					"node", newNode.Name,
					"conditionsChanged", conditionsChanged,
					"taintsChanged", taintsChanged,
					"labelsChanged", labelsChanged)
			}

			return shouldReconcile
		},
		CreateFunc: func(e event.CreateEvent) bool {
			log := ctrl.LoggerFrom(ctx)
			node := e.Object.(*corev1.Node)
			log.V(4).Info("Processing node create event", "node", node.GetName())
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool { return true },
	}
}

// conditionsEqual checks if two condition slices are equal.
func conditionsEqual(a, b []corev1.NodeCondition) bool {
	if len(a) != len(b) {
		return false
	}

	// Create map for quick lookup
	aMap := make(map[corev1.NodeConditionType]corev1.ConditionStatus)
	for _, cond := range a {
		aMap[cond.Type] = cond.Status
	}

	for _, cond := range b {
		if status, exists := aMap[cond.Type]; !exists || status != cond.Status {
			return false
		}
	}

	return true
}

// taintsEqual checks if two taint slices are equal.
func taintsEqual(a, b []corev1.Taint) bool {
	if len(a) != len(b) {
		return false
	}

	// Create map for quick lookup
	aMap := make(map[string]corev1.Taint)
	for _, taint := range a {
		key := taint.Key + string(taint.Effect)
		aMap[key] = taint
	}

	for _, taint := range b {
		key := taint.Key + string(taint.Effect)
		oldTaint, exists := aMap[key]
		if !exists || oldTaint.Value != taint.Value {
			return false
		}
	}

	return true
}

// labelsEqual checks if two label maps are equal.
func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}

	return true
}

// nodeSelectorChanged checks if nodeSelector has changed.
func nodeSelectorChanged(current, previous *metav1.LabelSelector) bool {
	// Both nil - no change
	if current == nil && previous == nil {
		return false
	}

	// One is nil, other is not - changed
	if (current == nil) != (previous == nil) {
		return true
	}

	// Compare matchLabels
	if !stringMapEqual(current.MatchLabels, previous.MatchLabels) {
		return true
	}

	// Compare matchExpressions
	if len(current.MatchExpressions) != len(previous.MatchExpressions) {
		return true
	}

	// Create maps for comparison
	currentExprs := make(map[string]metav1.LabelSelectorRequirement)
	for _, expr := range current.MatchExpressions {
		key := fmt.Sprintf("%s-%s-%v", expr.Key, expr.Operator, expr.Values)
		currentExprs[key] = expr
	}

	previousExprs := make(map[string]metav1.LabelSelectorRequirement)
	for _, expr := range previous.MatchExpressions {
		key := fmt.Sprintf("%s-%s-%v", expr.Key, expr.Operator, expr.Values)
		previousExprs[key] = expr
	}

	for key := range currentExprs {
		if _, exists := previousExprs[key]; !exists {
			return true
		}
	}

	return false
}

// stringMapEqual checks if two string maps are equal.
func stringMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}

	return true
}

func ensureFinalizer(ctx context.Context, c client.Client, rule *readinessv1alpha1.NodeReadinessRule, finalizer string) (finalizerAdded bool, err error) {
	// Finalizers can only be added when the deletionTimestamp is not set.
	if !rule.GetDeletionTimestamp().IsZero() {
		return false, nil
	}

	if controllerutil.ContainsFinalizer(rule, finalizer) {
		return false, nil
	}

	patch := client.MergeFrom(rule.DeepCopy())

	controllerutil.AddFinalizer(rule, finalizer)

	err = c.Patch(ctx, rule, patch)
	if err != nil {
		return false, err
	}

	return true, nil
}
