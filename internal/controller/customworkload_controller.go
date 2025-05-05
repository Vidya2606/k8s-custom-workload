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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	appsv1 "github.com/vidya2606/k8s-custom-workload/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sort"
)

// CustomWorkloadReconciler reconciles a CustomWorkload object
type CustomWorkloadReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.example.com,resources=customworkloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.example.com,resources=customworkloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.example.com,resources=customworkloads/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CustomWorkload object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *CustomWorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Triggered Reconcile")

	// Find the CustomWorkload instance
	workload, err := r.findCustomWorkload(ctx, req, log)
	if err != nil {
		return ctrl.Result{Requeue: false}, err
	}

	// List all pods owned by this workload
	childPods, err := r.findChildPods(ctx, req, log)
	if err != nil {
		return ctrl.Result{Requeue: false}, err
	}

	// Calculate hash of the latest template
	templateHash, err := calculateTemplateHash(workload.Spec.Template)
	if err != nil {
		log.Error(err, "Failed to calculate template hash")
		return ctrl.Result{Requeue: true}, err
	}

	// Manage Pods: create/delete based on replicas, partition, and hash
	if err := r.managePods(ctx, &workload, childPods.Items, templateHash, log); err != nil {
		log.Error(err, "Failed to manage pods")
		return ctrl.Result{Requeue: true}, err
	}

	return ctrl.Result{}, nil
}

func (r *CustomWorkloadReconciler) findCustomWorkload(ctx context.Context, req ctrl.Request, log logr.Logger) (appsv1.CustomWorkload, error) {
	var workload appsv1.CustomWorkload
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		if errors.IsNotFound(err) {
			log.Error(err, "CustomWorkload not found")
		}
		return workload, err
	}
	return workload, nil
}

func (r *CustomWorkloadReconciler) findChildPods(ctx context.Context, req ctrl.Request, log logr.Logger) (corev1.PodList, error) {
	var childPods corev1.PodList
	selector := client.MatchingLabels{"customworkload": req.Name}
	if err := r.List(ctx, &childPods, client.InNamespace(req.Namespace), selector); err != nil {
		log.Error(err, "Failed to list child Pods")
		return childPods, err
	}
	return childPods, nil
}

func (r *CustomWorkloadReconciler) managePods(ctx context.Context, workload *appsv1.CustomWorkload, allPods []corev1.Pod, newHash string, log logr.Logger) error {
	desired := int(workload.Spec.Replicas)
	partition := min(int(workload.Spec.Partition), desired)
	desiredNew := desired - partition

	var newPods, oldPods []corev1.Pod
	var oldHash string

	for _, pod := range allPods {
		if pod.DeletionTimestamp != nil {
			// skip terminating pods entirely
			continue
		}

		hash := pod.Labels["template-hash"]
		if hash == newHash {
			newPods = append(newPods, pod)
		} else {
			oldPods = append(oldPods, pod)
			if oldHash == "" && hash != "" {
				oldHash = hash
			}
		}
	}

	log.Info("Managing Pods", "desired", desired, "allPods", len(allPods), "partition", partition, "desiredNew", desiredNew, "newCount", len(newPods), "oldCount", len(oldPods), "newHash", newHash, "oldHash", oldHash)

	// Initial deployment
	if len(allPods) == 0 {
		log.Info("Initial deployment: creating all pods", "count", desired)
		return r.createPods(ctx, workload, desired, newHash)
	}

	// No template change, scale up/down only
	if oldHash == "" || oldHash == newHash {
		return r.handleScalingOnly(ctx, workload, allPods, newHash, desired, log)
	}
	if len(newPods) == 0 && allPodsHaveHash(oldPods, newHash) {
		return r.handleScalingOnly(ctx, workload, allPods, newHash, desired, log)
	}

	// Template change, perform partitioned rolling update
	return r.handleRollingUpdate(ctx, workload, newPods, oldPods, newHash, desiredNew, partition, log)
}

func (r *CustomWorkloadReconciler) handleRollingUpdate(ctx context.Context, workload *appsv1.CustomWorkload, newPods, oldPods []corev1.Pod, newHash string, desiredNew, partition int, log logr.Logger) error {
	// Filter out terminating pods
	liveNewPods := filterLivePods(newPods)
	liveOldPods := filterLivePods(oldPods)

	currentTotal := len(liveNewPods) + len(liveOldPods)
	desiredTotal := desiredNew + partition

	log.Info("Rolling update state", "liveNewPods", len(liveNewPods), "liveOldPods", len(liveOldPods), "desiredNew", desiredNew, "partition", partition, "currentTotal", currentTotal, "desiredTotal", desiredTotal)

	// Create new pods only if needed
	if len(liveNewPods) < desiredNew {
		toCreate := desiredNew - len(liveNewPods)
		log.Info("Rolling update: creating new pods", "toCreate", toCreate)
		return r.createPods(ctx, workload, toCreate, newHash)
	}

	// Only delete old pods beyond the partition if we already have enough new pods
	if len(liveOldPods) > partition {
		toDelete := len(liveOldPods) - partition
		log.Info("Rolling update: deleting old pods beyond partition", "toDelete", toDelete)
		return r.deletePods(ctx, liveOldPods[:toDelete], toDelete, newHash)
	}

	// If total exceeds desired, scale down oldest pods
	if currentTotal > desiredTotal {
		toDelete := currentTotal - desiredTotal
		log.Info("Rolling update: scaling down excess pods", "toDelete", toDelete)

		allLivePods := append([]corev1.Pod{}, liveNewPods...)
		allLivePods = append(allLivePods, liveOldPods...)

		return r.deletePods(ctx, allLivePods, toDelete, newHash)
	}

	log.Info("Rolling update step completed")
	return nil
}

func filterLivePods(pods []corev1.Pod) []corev1.Pod {
	var result []corev1.Pod
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			result = append(result, pod)
		}
	}
	return result
}

func (r *CustomWorkloadReconciler) handleScalingOnly(ctx context.Context, workload *appsv1.CustomWorkload, allPods []corev1.Pod, templateHash string, desired int, log logr.Logger) error {
	var livePods []corev1.Pod
	for _, pod := range allPods {
		if pod.DeletionTimestamp == nil {
			livePods = append(livePods, pod)
		}
	}

	existing := len(livePods)
	if existing < desired {
		toCreate := desired - existing
		log.Info("Scaling up: creating new pods", "count", toCreate)
		return r.createPods(ctx, workload, toCreate, templateHash)
	}

	if existing > desired {
		toDelete := existing - desired
		log.Info("Scaling down: deleting excess pods", "count", toDelete)
		return r.deletePods(ctx, livePods, toDelete, "")
	}
	log.Info("No scaling needed. Desired count met")
	return nil
}

func (r *CustomWorkloadReconciler) createPods(ctx context.Context, workload *appsv1.CustomWorkload, podsToCreate int, templateHash string) error {
	for i := 0; i < podsToCreate; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: workload.Name + "-",
				Namespace:    workload.Namespace,
				Labels: map[string]string{
					"customworkload": workload.Name,
					"template-hash":  templateHash,
				},
			},
			Spec: workload.Spec.Template.Spec,
		}
		if err := controllerutil.SetControllerReference(workload, pod, r.Scheme); err != nil {
			return err
		}

		// triggers a POST /api/v1/namespaces/default/pods
		if err := r.Create(ctx, pod); err != nil {
			fmt.Println("Failed to create Pod:", err)
			return err
		}
		fmt.Println("Created Pod:", pod.Name)
	}
	return nil
}

func (r *CustomWorkloadReconciler) deletePods(ctx context.Context, childPods []corev1.Pod, podsToDelete int, preferredHash string) error {
	// Filter out terminating pods
	var livePods []corev1.Pod
	for _, pod := range childPods {
		if pod.DeletionTimestamp == nil {
			livePods = append(livePods, pod)
		}
	}

	// Prioritize deletion of pods not matching the preferred hash (i.e., delete old-hash pods first)
	if preferredHash != "" {
		sort.SliceStable(livePods, func(i, j int) bool {
			iPreferred := livePods[i].Labels["template-hash"] == preferredHash
			jPreferred := livePods[j].Labels["template-hash"] == preferredHash
			if iPreferred != jPreferred {
				return !iPreferred // false < true → keep preferredHash pods last
			}
			// delete oldest first
			return livePods[i].CreationTimestamp.Before(&livePods[j].CreationTimestamp)
		})
	} else {
		// sort by age (oldest first)
		sort.SliceStable(livePods, func(i, j int) bool {
			return livePods[i].CreationTimestamp.Before(&livePods[j].CreationTimestamp)
		})
	}

	toDelete := livePods[:podsToDelete]

	for _, pod := range toDelete {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			// triggers a DELETE /api/v1/namespaces/default/pods/<name>
			if err := r.Delete(ctx, &pod); err != nil {
				fmt.Println("Failed to delete pod:", pod.Name, "error:", err)
				return err
			}
			fmt.Println("Deleted pod:", pod.Name)
		}
	}

	return nil
}

func calculateTemplateHash(spec corev1.PodTemplateSpec) (string, error) {
	bytes, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(bytes)
	fullHash := hex.EncodeToString(hash[:])
	return fullHash[:10], nil
}

func allPodsHaveHash(pods []corev1.Pod, hash string) bool {
	for _, pod := range pods {
		if pod.Labels["template-hash"] != hash {
			return false
		}
	}
	return true
}

// SetupWithManager sets up the controller with the Manager.
func (r *CustomWorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.CustomWorkload{}).
		Owns(&corev1.Pod{}).
		Named("customworkload").
		Complete(r)
}
