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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	readinessv1alpha1 "sigs.k8s.io/node-readiness-controller/api/v1alpha1"
)

const (
	// finalizerName is the finalizer added to NodeReadinessRule to ensure cleanup.
	finalizerName = "readiness.node.x-k8s.io/cleanup-taints"
)

// ReadinessController manages node taints based on readiness gate rules.
type ReadinessController struct {
	client.Client
	Scheme *runtime.Scheme

	// Cache for efficient rule lookup
	ruleCacheMutex sync.RWMutex
	ruleCache      map[string]*readinessv1alpha1.NodeReadinessRule // ruleName -> rule

	// Global dry run mode (emergency off-switch)
	globalDryRun bool
}

// NewReadinessGateController creates a new controller.
func NewReadinessGateController(mgr ctrl.Manager) *ReadinessController {
	return &ReadinessController{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		ruleCache: make(map[string]*readinessv1alpha1.NodeReadinessRule),
	}
}

// RuleReconciler handles NodeReadinessRule reconciliation.
type RuleReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Controller *ReadinessController
}

// +kubebuilder:rbac:groups=readiness.node.x-k8s.io,resources=nodereadinessrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=readiness.node.x-k8s.io,resources=nodereadinessrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=readiness.node.x-k8s.io,resources=nodereadinessrules/finalizers,verbs=update

// SetupWithManager sets up the controller with the Manager.
func (r *RuleReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("nodereadiness-controller").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		For(&readinessv1alpha1.NodeReadinessRule{}).
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.mapNodeToRules),
			builder.WithPredicates(nodeStatusChangedPredicate(ctx))).
		Complete(r)
}

func (r *RuleReconciler) mapNodeToRules(ctx context.Context, obj client.Object) []reconcile.Request {
	log := ctrl.LoggerFrom(ctx)

	node, ok := obj.(*corev1.Node)
	if !ok {
		log.V(4).Info("Expected Node", "type", fmt.Sprintf("%T", obj))
		return nil
	}
	log.V(4).Info("Processing node event", "node", node.GetName())

	var rules readinessv1alpha1.NodeReadinessRuleList
	if err := r.List(ctx, &rules); err != nil {
		log.V(4).Error(err, "failed to list NodeReadinessRules")
		return nil
	}

	var requests []reconcile.Request
	for _, rule := range rules.Items {
		// If the rule's selector matches this node, add it to the queue
		if r.Controller.ruleAppliesTo(ctx, &rule, node) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: rule.Name},
			})
		}
	}
	return requests
}

func (r *RuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, retErr error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling rule", "rule", req.Name)

	// Fetch the rule
	rule := &readinessv1alpha1.NodeReadinessRule{}
	if err := r.Get(ctx, req.NamespacedName, rule); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Rule deleted, remove from cache
			log.Info("Rule not found, removing from cache", "rule", req.Name)
			r.Controller.removeRuleFromCache(ctx, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	ctx = ctrl.LoggerInto(ctx, ctrl.LoggerFrom(ctx).WithValues("Rule", klog.KRef(rule.Namespace, rule.Name)))

	// Add finalizer first if not set to avoid the race condition between init and delete.
	if finalizerAdded, err := ensureFinalizer(ctx, r.Client, rule, finalizerName); err != nil {
		log.Error(err, "failed to add finalizer")
		return ctrl.Result{}, err
	} else if finalizerAdded {
		log.Info("Finalizer added to the rule")
		return ctrl.Result{}, nil
	}

	patchObject := client.MergeFrom(rule.DeepCopy())
	defer func() {
		if err := r.Patch(ctx, rule, patchObject); err != nil {
			log.Error(err, "Failed to patch Rule", "rule", rule.Name)
			retErr = kerrors.NewAggregate([]error{retErr, err})
			return
		}
	}()

	// Handle deletion reconciliation loop.
	if !rule.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, rule)
	}

	// Detect nodeSelector changes and cleanup old nodes
	cachedRule := r.Controller.getCachedRule(rule.Name)
	if cachedRule != nil && nodeSelectorChanged(rule.Spec.NodeSelector, cachedRule.Spec.NodeSelector) {
		log.Info("NodeSelector changed, cleaning up nodes from old selector", "rule", rule.Name)
		if err := r.Controller.cleanupNodesAfterSelectorChange(ctx, cachedRule, rule); err != nil {
			log.Error(err, "Failed to cleanup nodes after selector change", "rule", rule.Name)
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	}

	// Update rule cache (after cleanup)
	r.Controller.updateRuleCache(ctx, rule)

	// Handle dry run
	if rule.Spec.DryRun {
		if err := r.Controller.processDryRun(ctx, rule); err != nil {
			log.Error(err, "Failed to process dry run", "rule", rule.Name)
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	} else {
		// Clear previous dry run results
		rule.Status.DryRunResults = readinessv1alpha1.DryRunResults{}

		// Process all applicable nodes for this rule
		if err := r.Controller.processAllNodesForRule(ctx, rule); err != nil {
			log.Error(err, "Failed to process nodes for rule", "rule", rule.Name)
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	}

	// Update rule status
	if err := r.Controller.updateRuleStatus(ctx, rule); err != nil {
		log.Error(err, "Failed to update rule status", "rule", rule.Name)
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Clean up status for deleted nodes
	if err := r.Controller.cleanupDeletedNodes(ctx, rule); err != nil {
		log.Error(err, "Failed to clean up deleted nodes", "rule", rule.Name)
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	return ctrl.Result{}, nil
}

func (r *RuleReconciler) reconcileDelete(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	// Rule is being deleted, clean up taints before removing finalizer
	log.Info("Cleaning up taints for deleted rule", "rule", rule.Name)
	if err := r.Controller.cleanupTaintsForRule(ctx, rule); err != nil {
		log.Error(err, "Failed to cleanup taints for rule", "rule", rule.Name)
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Remove from cache after successful cleanup
	r.Controller.removeRuleFromCache(ctx, rule.Name)

	// Remove finalizer
	controllerutil.RemoveFinalizer(rule, finalizerName)

	return ctrl.Result{}, nil
}

// getConditionStatus gets the status of a condition on a node.
func (r *ReadinessController) getConditionStatus(node *corev1.Node, conditionType string) corev1.ConditionStatus {
	for _, condition := range node.Status.Conditions {
		if string(condition.Type) == conditionType {
			return condition.Status
		}
	}
	return corev1.ConditionUnknown
}

// hasTaintBySpec checks if a node has a specific taint.
func (r *ReadinessController) hasTaintBySpec(node *corev1.Node, taintSpec corev1.Taint) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintSpec.Key && taint.Effect == taintSpec.Effect {
			return true
		}
	}
	return false
}

// addTaintBySpec adds a taint to a node.
func (r *ReadinessController) addTaintBySpec(ctx context.Context, node *corev1.Node, taintSpec corev1.Taint) error {
	patch := client.StrategicMergeFrom(node.DeepCopy())
	node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
		Key:    taintSpec.Key,
		Value:  taintSpec.Value,
		Effect: taintSpec.Effect,
	})
	return r.Patch(ctx, node, patch)
}

// removeTaintBySpec removes a taint from a node.
func (r *ReadinessController) removeTaintBySpec(ctx context.Context, node *corev1.Node, taintSpec corev1.Taint) error {
	patch := client.StrategicMergeFrom(node.DeepCopy())
	var newTaints []corev1.Taint
	for _, taint := range node.Spec.Taints {
		if taint.Key != taintSpec.Key || taint.Effect != taintSpec.Effect {
			newTaints = append(newTaints, taint)
		}
	}
	node.Spec.Taints = newTaints
	return r.Patch(ctx, node, patch)
}

// Bootstrap completion tracking.
func (r *ReadinessController) isBootstrapCompleted(nodeName, ruleName string) bool {
	// Check node annotation
	node := &corev1.Node{}
	if err := r.Get(context.TODO(), client.ObjectKey{Name: nodeName}, node); err != nil {
		return false
	}

	annotationKey := fmt.Sprintf("readiness.k8s.io/bootstrap-completed-%s", ruleName)
	_, exists := node.Annotations[annotationKey]
	return exists
}

func (r *ReadinessController) markBootstrapCompleted(ctx context.Context, nodeName, ruleName string) {
	log := ctrl.LoggerFrom(ctx)

	annotationKey := fmt.Sprintf("readiness.k8s.io/bootstrap-completed-%s", ruleName)

	// retry to handle conflict with concurrent node updates
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return err
		}

		// Check if already marked to avoid unnecessary updates
		if node.Annotations != nil {
			if _, exists := node.Annotations[annotationKey]; exists {
				return nil
			}
		}

		// Initialize annotations if nil
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}

		node.Annotations[annotationKey] = "true"
		return r.Update(ctx, node)
	})

	if err != nil {
		log.Error(err, "Failed to mark bootstrap completed", "node", nodeName, "rule", ruleName)
	} else {
		log.Info("Marked bootstrap completed", "node", nodeName, "rule", ruleName)
	}
}

// cleanupDeletedNodes removes status entries for nodes that no longer exist.
func (r *ReadinessController) cleanupDeletedNodes(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return err
	}

	existingNodes := make(map[string]bool)
	for _, node := range nodeList.Items {
		existingNodes[node.Name] = true
	}

	// Filter out deleted nodes
	var newNodeEvaluations []readinessv1alpha1.NodeEvaluation
	for _, evaluation := range rule.Status.NodeEvaluations {
		if existingNodes[evaluation.NodeName] {
			newNodeEvaluations = append(newNodeEvaluations, evaluation)
		}
	}

	if len(newNodeEvaluations) == len(rule.Status.NodeEvaluations) {
		log.V(4).Info("No deleted nodes to clean up", "rule", rule.Name)
		return nil
	}

	log.V(4).Info("Cleaning up deleted nodes from rule status",
		"rule", rule.Name,
		"before", len(rule.Status.NodeEvaluations),
		"after", len(newNodeEvaluations))

	// Use retry on conflict to update status to avoid race conditions from node updates
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &readinessv1alpha1.NodeReadinessRule{}
		if err := r.Get(ctx, client.ObjectKey{Name: rule.Name}, fresh); err != nil {
			return err
		}

		var freshNodeEvaluations []readinessv1alpha1.NodeEvaluation
		for _, evaluation := range fresh.Status.NodeEvaluations {
			if existingNodes[evaluation.NodeName] {
				freshNodeEvaluations = append(freshNodeEvaluations, evaluation)
			}
		}

		if len(freshNodeEvaluations) == len(fresh.Status.NodeEvaluations) {
			return nil
		}

		fresh.Status.NodeEvaluations = freshNodeEvaluations
		return r.Status().Update(ctx, fresh)
	})
}

// processAllNodesForRule processes all nodes when a rule changes.
func (r *ReadinessController) processAllNodesForRule(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return err
	}

	log.Info("Processing all nodes for rule", "rule", rule.Name, "totalNodes", len(nodeList.Items))

	var appliedNodes []string
	for _, node := range nodeList.Items {
		if r.ruleAppliesTo(ctx, rule, &node) {
			appliedNodes = append(appliedNodes, node.Name)

			if r.isBootstrapCompleted(node.Name, rule.Name) && rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly {
				log.Info("Skipping bootstrap-only rule - already completed",
					"node", node.Name, "rule", rule.Name)
				continue
			}

			log.Info("Processing node for rule", "rule", rule.Name, "node", node.Name)
			if err := r.evaluateRuleForNode(ctx, rule, &node); err != nil {
				// Log error but continue with other nodes
				log.Error(err, "Failed to evaluate node for rule", "rule", rule.Name, "node", node.Name)
				r.recordNodeFailure(rule, node.Name, "EvaluationError", err.Error())
			}
		}
	}

	// Update status
	rule.Status.ObservedGeneration = rule.Generation
	rule.Status.AppliedNodes = appliedNodes

	log.Info("Completed processing nodes for rule", "rule", rule.Name, "processedCount", len(appliedNodes))
	return nil
}

// evaluateRuleForNode evaluates a single rule against a single node.
func (r *ReadinessController) evaluateRuleForNode(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, node *corev1.Node) error {
	log := ctrl.LoggerFrom(ctx)

	// Evaluate all conditions (ALL logic)
	allConditionsSatisfied := true
	conditionResults := make([]readinessv1alpha1.ConditionEvaluationResult, 0, len(rule.Spec.Conditions))

	for _, condReq := range rule.Spec.Conditions {
		currentStatus := r.getConditionStatus(node, condReq.Type)
		satisfied := currentStatus == condReq.RequiredStatus
		missing := currentStatus == corev1.ConditionUnknown

		if !satisfied {
			allConditionsSatisfied = false
		}

		conditionResults = append(conditionResults, readinessv1alpha1.ConditionEvaluationResult{
			Type:           condReq.Type,
			CurrentStatus:  currentStatus,
			RequiredStatus: condReq.RequiredStatus,
			Satisfied:      satisfied,
			Missing:        missing,
		})

		log.V(1).Info("Condition evaluation", "node", node.Name, "rule", rule.Name,
			"conditionType", condReq.Type, "current", currentStatus, "required", condReq.RequiredStatus,
			"satisfied", satisfied, "missing", missing)
	}

	// Determine taint action
	shouldRemoveTaint := allConditionsSatisfied
	currentlyHasTaint := r.hasTaintBySpec(node, rule.Spec.Taint)

	log.Info("Evaluation result", "node", node.Name, "rule", rule.Name,
		"allConditionsSatisfied", allConditionsSatisfied, "hasTaint", currentlyHasTaint)

	var err error

	switch {
	case shouldRemoveTaint && currentlyHasTaint:
		log.Info("Removing taint", "node", node.Name, "rule", rule.Name, "taint", rule.Spec.Taint.Key)

		if err = r.removeTaintBySpec(ctx, node, rule.Spec.Taint); err != nil {
			return fmt.Errorf("failed to remove taint: %w", err)
		}

		// Mark bootstrap completed if bootstrap-only mode
		if rule.Spec.EnforcementMode == readinessv1alpha1.EnforcementModeBootstrapOnly {
			r.markBootstrapCompleted(ctx, node.Name, rule.Name)
		}

	case !shouldRemoveTaint && !currentlyHasTaint:
		log.Info("Adding taint", "node", node.Name, "rule", rule.Name, "taint", rule.Spec.Taint.Key)

		if err = r.addTaintBySpec(ctx, node, rule.Spec.Taint); err != nil {
			return fmt.Errorf("failed to add taint: %w", err)
		}

	default:
		log.Info("No taint action needed", "node", node.Name, "rule", rule.Name,
			"shouldRemove", shouldRemoveTaint, "hasTaint", currentlyHasTaint)
	}

	// Determine observed taint status after any actions
	var taintStatus readinessv1alpha1.TaintStatus
	if r.hasTaintBySpec(node, rule.Spec.Taint) {
		taintStatus = readinessv1alpha1.TaintStatusPresent
	} else {
		taintStatus = readinessv1alpha1.TaintStatusAbsent
	}

	// Update evaluation status
	r.updateNodeEvaluationStatus(rule, node.Name, conditionResults, taintStatus)

	return nil
}

// updateNodeEvaluationStatus updates the evaluation status for a specific node.
func (r *ReadinessController) updateNodeEvaluationStatus(
	rule *readinessv1alpha1.NodeReadinessRule,
	nodeName string,
	conditionResults []readinessv1alpha1.ConditionEvaluationResult,
	taintStatus readinessv1alpha1.TaintStatus,
) {
	// Find existing evaluation or create new
	var nodeEval *readinessv1alpha1.NodeEvaluation
	for i := range rule.Status.NodeEvaluations {
		if rule.Status.NodeEvaluations[i].NodeName == nodeName {
			nodeEval = &rule.Status.NodeEvaluations[i]
			break
		}
	}

	if nodeEval == nil {
		rule.Status.NodeEvaluations = append(rule.Status.NodeEvaluations, readinessv1alpha1.NodeEvaluation{
			NodeName: nodeName,
		})
		nodeEval = &rule.Status.NodeEvaluations[len(rule.Status.NodeEvaluations)-1]
	}

	// Update evaluation
	nodeEval.ConditionResults = conditionResults
	nodeEval.TaintStatus = taintStatus
	nodeEval.LastEvaluationTime = metav1.Now()
}

// ruleAppliesTo checks if a rule applies to a node.
func (r *ReadinessController) ruleAppliesTo(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule, node *corev1.Node) bool {
	log := ctrl.LoggerFrom(ctx)

	if rule.Spec.NodeSelector == nil {
		return true
	}

	selector, err := metav1.LabelSelectorAsSelector(rule.Spec.NodeSelector)
	if err != nil {
		log.Error(err, "Invalid node selector for rule", "rule", rule.Name)
		return false
	}

	return selector.Matches(labels.Set(node.Labels))
}

// updateRuleCache updates the rule cache.
func (r *ReadinessController) updateRuleCache(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) {
	log := ctrl.LoggerFrom(ctx)
	r.ruleCacheMutex.Lock()
	defer r.ruleCacheMutex.Unlock()

	ruleCopy := rule.DeepCopy()
	r.ruleCache[rule.Name] = ruleCopy
	log.V(4).Info("Updated rule cache",
		"rule", rule.Name,
		"totalRules", len(r.ruleCache),
		"resourceVersion", ruleCopy.ResourceVersion)
}

// getCachedRule retrieves a rule from cache.
func (r *ReadinessController) getCachedRule(ruleName string) *readinessv1alpha1.NodeReadinessRule {
	r.ruleCacheMutex.RLock()
	defer r.ruleCacheMutex.RUnlock()

	rule, exists := r.ruleCache[ruleName]
	if !exists {
		return nil
	}
	return rule.DeepCopy()
}

// removeRuleFromCache removes a rule from cache.
func (r *ReadinessController) removeRuleFromCache(ctx context.Context, ruleName string) {
	log := ctrl.LoggerFrom(ctx)
	r.ruleCacheMutex.Lock()
	defer r.ruleCacheMutex.Unlock()

	delete(r.ruleCache, ruleName)
	log.Info("Removed rule from cache", "rule", ruleName, "totalRules", len(r.ruleCache))
}

// updateRuleStatus updates the status of a NodeReadinessRule.
func (r *ReadinessController) updateRuleStatus(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	log.V(1).Info("Updating rule status",
		"rule", rule.Name,
		"nodeEvaluations", len(rule.Status.NodeEvaluations),
		"appliedNodes", len(rule.Status.AppliedNodes))

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestRule := &readinessv1alpha1.NodeReadinessRule{}
		if err := r.Get(ctx, client.ObjectKey{Name: rule.Name}, latestRule); err != nil {
			return err
		}

		// Merge our status updates into fresh version
		// This ensures we're updating based on the latest resourceVersion
		latestRule.Status.NodeEvaluations = rule.Status.NodeEvaluations
		latestRule.Status.AppliedNodes = rule.Status.AppliedNodes
		latestRule.Status.FailedNodes = rule.Status.FailedNodes
		latestRule.Status.ObservedGeneration = rule.Status.ObservedGeneration
		latestRule.Status.DryRunResults = rule.Status.DryRunResults

		if err := r.Status().Update(ctx, latestRule); err != nil {
			log.V(1).Info("Status update conflict, will retry",
				"rule", rule.Name,
				"error", err.Error())
			return err
		}

		log.V(1).Info("Successfully updated rule status", "rule", rule.Name)
		return nil
	})
}

// processDryRun processes dry run for a rule.
func (r *ReadinessController) processDryRun(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) error {
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return err
	}

	var affectedNodes, taintsToAdd, taintsToRemove, riskyOps int32
	var summaryParts []string

	for _, node := range nodeList.Items {
		if !r.ruleAppliesTo(ctx, rule, &node) {
			continue
		}

		affectedNodes++

		// Simulate rule evaluation
		allConditionsSatisfied := true
		missingConditions := 0

		for _, condReq := range rule.Spec.Conditions {
			currentStatus := r.getConditionStatus(&node, condReq.Type)
			if currentStatus == corev1.ConditionUnknown {
				missingConditions++
			}
			if currentStatus != condReq.RequiredStatus {
				allConditionsSatisfied = false
			}
		}

		shouldRemoveTaint := allConditionsSatisfied
		currentlyHasTaint := r.hasTaintBySpec(&node, rule.Spec.Taint)

		if shouldRemoveTaint && currentlyHasTaint {
			taintsToRemove++
		} else if !shouldRemoveTaint && !currentlyHasTaint {
			taintsToAdd++
		}

		if missingConditions > 0 {
			riskyOps++
		}
	}

	// Build summary
	if taintsToAdd > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("would add %d taints", taintsToAdd))
	}
	if taintsToRemove > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("would remove %d taints", taintsToRemove))
	}
	if riskyOps > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("%d nodes have missing conditions", riskyOps))
	}

	summary := "No changes needed"
	if len(summaryParts) > 0 {
		summary = strings.Join(summaryParts, ", ")
	}

	// Update rule status with dry run results
	rule.Status.DryRunResults = readinessv1alpha1.DryRunResults{
		AffectedNodes:   &affectedNodes,
		TaintsToAdd:     &taintsToAdd,
		TaintsToRemove:  &taintsToRemove,
		RiskyOperations: &riskyOps,
		Summary:         summary,
	}

	return nil
}

// SetGlobalDryRun sets the global dry run mode (emergency off-switch).
func (r *ReadinessController) SetGlobalDryRun(dryRun bool) {
	r.globalDryRun = dryRun
}

// cleanupTaintsForRule removes taints managed by this rule from all applicable nodes.
func (r *ReadinessController) cleanupTaintsForRule(ctx context.Context, rule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	// Get all nodes that this rule applies to
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	var errors []string
	for _, node := range nodeList.Items {
		if !r.ruleAppliesTo(ctx, rule, &node) {
			continue
		}

		// Check if node has the taint managed by this rule
		if r.hasTaintBySpec(&node, rule.Spec.Taint) {
			log.Info("Removing taint from node during rule cleanup",
				"node", node.Name,
				"rule", rule.Name,
				"taint", rule.Spec.Taint.Key)

			if err := r.removeTaintBySpec(ctx, &node, rule.Spec.Taint); err != nil {
				errors = append(errors, fmt.Sprintf("node %s: %v", node.Name, err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to cleanup taints on some nodes: %s", strings.Join(errors, "; "))
	}

	return nil
}

// cleanupNodesAfterSelectorChange cleans up nodes that matched old selector but not new one.
func (r *ReadinessController) cleanupNodesAfterSelectorChange(ctx context.Context, oldRule, newRule *readinessv1alpha1.NodeReadinessRule) error {
	log := ctrl.LoggerFrom(ctx)

	// Get all nodes
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	// Build old selector
	var oldSelector labels.Selector
	var err error
	if oldRule.Spec.NodeSelector != nil {
		oldSelector, err = metav1.LabelSelectorAsSelector(oldRule.Spec.NodeSelector)
		if err != nil {
			return fmt.Errorf("failed to parse old node selector: %w", err)
		}
	}

	// Clean up nodes that matched old selector but not new selector
	var errors []string
	for _, node := range nodeList.Items {
		// Check if node matched old selector
		matchedOld := false
		if oldSelector == nil {
			// nil selector matches all nodes
			matchedOld = true
		} else {
			matchedOld = oldSelector.Matches(labels.Set(node.Labels))
		}

		// Check if node matches new selector (use newRule for current evaluation)
		matchesNew := r.ruleAppliesTo(ctx, newRule, &node)

		// If node matched old but not new, clean up the taint
		if matchedOld && !matchesNew {
			if r.hasTaintBySpec(&node, newRule.Spec.Taint) {
				log.Info("Removing taint from node that no longer matches selector",
					"node", node.Name,
					"rule", newRule.Name,
					"taint", newRule.Spec.Taint.Key)

				if err := r.removeTaintBySpec(ctx, &node, newRule.Spec.Taint); err != nil {
					errors = append(errors, fmt.Sprintf("node %s: %v", node.Name, err))
				}
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to cleanup taints on some nodes: %s", strings.Join(errors, "; "))
	}

	return nil
}

// recordNodeFailure records a failure for a specific node.
func (r *ReadinessController) recordNodeFailure(
	rule *readinessv1alpha1.NodeReadinessRule,
	nodeName, reason, message string,
) {
	// Remove any existing failure for this node
	var failedNodes []readinessv1alpha1.NodeFailure
	for _, failure := range rule.Status.FailedNodes {
		if failure.NodeName != nodeName {
			failedNodes = append(failedNodes, failure)
		}
	}

	// Add new failure
	failedNodes = append(failedNodes, readinessv1alpha1.NodeFailure{
		NodeName:           nodeName,
		Reason:             reason,
		Message:            message,
		LastEvaluationTime: metav1.Now(),
	})

	rule.Status.FailedNodes = failedNodes
}
