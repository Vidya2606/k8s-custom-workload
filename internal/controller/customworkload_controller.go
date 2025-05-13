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
	customreplicasetv1 "github.com/vidya2606/k8s-custom-workload/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sort"
	"time"
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

func (r *CustomWorkloadReconciler) findCustomWorkload(ctx context.Context, req ctrl.Request, log logr.Logger) (customreplicasetv1.CustomWorkload, error) {
	var workload customreplicasetv1.CustomWorkload
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

func (r *CustomWorkloadReconciler) managePods(ctx context.Context, workload *customreplicasetv1.CustomWorkload, allPods []corev1.Pod, newHash string, log logr.Logger) error {
	desired := int(workload.Spec.Replicas)
	partition := min(int(workload.Spec.Partition), desired)
	desiredNew := desired - partition

	var livePods, newPods, oldPods []corev1.Pod
	hashCounts := map[string][]corev1.Pod{}

	// Group live pods and organize by template-hash
	for _, pod := range allPods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		livePods = append(livePods, pod)
		hash := pod.Labels["template-hash"]
		hashCounts[hash] = append(hashCounts[hash], pod)
	}

	// Determine the stableHash (the most common existing hash, the version which is applied to all pods)
	stableHash := ""
	maxCount := 0
	for hash, pods := range hashCounts {
		if hash == newHash {
			continue // skip new hash
		}
		if len(pods) > maxCount {
			stableHash = hash
			maxCount = len(pods)
		}
	}

	// Categorize live pods into new (matching newHash) and old
	for _, pod := range livePods {
		if pod.Labels["template-hash"] == newHash {
			newPods = append(newPods, pod)
		} else {
			oldPods = append(oldPods, pod)
		}
	}

	log.Info("Managing Pods", "desired", desired, "livePods", len(livePods), "partition", partition,
		"desiredNew", desiredNew, "newCount", len(newPods), "oldCount", len(oldPods), "newHash", newHash, "stableHash", stableHash)

	// Initial deployment — no pods yet
	if len(livePods) == 0 {
		log.Info("Initial deployment: creating all pods", "count", desired)
		return r.createPods(ctx, workload, desired, newHash)
	}

	// All pods already on newHash (no rollout, only scaling)
	if len(newPods) == len(livePods) {
		return r.handleScalingOnly(ctx, workload, livePods, newHash, desired, log)
	}

	// State already matches partition goal — no action needed
	if len(newPods) == desiredNew && len(oldPods) == partition {
		log.Info("Pod state matches desired partition — no rollout needed")
		return nil
	}

	// Template change, perform partitioned rolling update
	return r.handleRollingUpdate(ctx, workload, newPods, oldPods, newHash, desiredNew, partition, stableHash, log)
}

func (r *CustomWorkloadReconciler) handleScalingOnly(ctx context.Context, workload *customreplicasetv1.CustomWorkload, allPods []corev1.Pod, templateHash string, desired int, log logr.Logger) error {
	//var livePods []corev1.Pod
	//for _, pod := range allPods {
	//	if pod.DeletionTimestamp == nil {
	//		livePods = append(livePods, pod)
	//	}
	//}
	livePods := filterLivePods(allPods)

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

//func (r *CustomWorkloadReconciler) handleRollingUpdate(ctx context.Context, workload *customreplicasetv1.CustomWorkload, newPods, oldPods []corev1.Pod, newHash string, desiredNew int, partition int, stableHash string, log logr.Logger) error {
//	liveNewPods := filterLivePods(newPods)
//	liveOldPods := filterLivePods(oldPods)
//
//	currentTotal := len(liveNewPods) + len(liveOldPods)
//	desiredTotal := desiredNew + partition
//
//	log.Info("Rolling update state", "liveNewPods", len(liveNewPods), "liveOldPods", len(liveOldPods),
//		"desiredNew", desiredNew, "partition", partition, "currentTotal", currentTotal, "desiredTotal", desiredTotal, "stableHash", stableHash)
//
//	// First, delete any excess new pods if partition increased
//	if len(liveNewPods) > desiredNew {
//		toDelete := len(liveNewPods) - desiredNew
//		log.Info("Rolling update: deleting excess new pods due to partition increase", "toDelete", toDelete)
//		return r.deletePods(ctx, liveNewPods[:toDelete], toDelete, newHash)
//	}
//
//	// Decide what hash to use for new pod
//	createHash := newHash
//	if len(liveNewPods) >= desiredNew && len(liveOldPods) < partition {
//		// rollback or partition increased: create old (stable) hash pods
//		createHash = stableHash
//		log.Info("Using stableHash for pod creation (rollback or partition increase)", "createHash", stableHash)
//	}
//
//	// Calculate how many new pods to create (based on current hash group counts)
//	toCreate := 0
//	if createHash == stableHash {
//		toCreate = partition - len(liveOldPods)
//	} else {
//		toCreate = desiredNew - len(liveNewPods)
//	}
//
//	if toCreate > 0 {
//		log.Info("Rolling update: creating new pods", "toCreate", toCreate, "usingHash", createHash)
//		return r.createPods(ctx, workload, toCreate, createHash)
//	}
//
//	// Create pods if needed
//	if currentTotal < desiredTotal {
//		toCreate := desiredTotal - currentTotal
//		log.Info("Rolling update: creating new pods", "toCreate", toCreate)
//		return r.createPods(ctx, workload, toCreate, createHash)
//	}
//
//	// Delete old pods beyond the partition (roll forward)
//	if len(liveOldPods) > partition {
//		toDelete := len(liveOldPods) - partition
//		log.Info("Rolling update: deleting old pods beyond partition", "toDelete", toDelete)
//		return r.deletePods(ctx, liveOldPods[:toDelete], toDelete, newHash)
//	}
//
//	log.Info("Rolling update step completed")
//	return nil
//}

func (r *CustomWorkloadReconciler) handleRollingUpdate(
	ctx context.Context,
	workload *customreplicasetv1.CustomWorkload,
	newPods, oldPods []corev1.Pod,
	newHash string,
	desiredNew int,
	partition int,
	stableHash string,
	log logr.Logger,
) error {
	liveNewPods := filterLivePods(newPods)
	liveOldPods := filterLivePods(oldPods)

	currentTotal := len(liveNewPods) + len(liveOldPods)
	desiredTotal := desiredNew + partition

	log.Info("Rolling update state",
		"liveNewPods", len(liveNewPods),
		"liveOldPods", len(liveOldPods),
		"desiredNew", desiredNew,
		"partition", partition,
		"currentTotal", currentTotal,
		"desiredTotal", desiredTotal,
		"stableHash", stableHash)

	// Decide correct pod hash to create
	// rollback or partition increase → use stableHash
	createHash := newHash
	if len(liveNewPods) >= desiredNew && stableHash != "" && stableHash != newHash {
		createHash = stableHash
		log.Info("Using stableHash for pod creation (rollback or partition increase)", "createHash", stableHash)
	}

	// Create missing pods
	if currentTotal < desiredTotal {
		toCreate := desiredTotal - currentTotal
		log.Info("Rolling update: creating new pods", "toCreate", toCreate)
		return r.createPods(ctx, workload, toCreate, createHash)
	}

	// If we have enough total pods, but wrong versions (rollback case),
	// delete wrong hash pods to make room for stableHash
	if len(liveNewPods) > desiredNew {
		toDelete := len(liveNewPods) - desiredNew
		log.Info("Rolling update: deleting excess new pods to rollback", "toDelete", toDelete)
		return r.deletePods(ctx, liveNewPods[:toDelete], toDelete, stableHash)
	}

	// Delete old pods beyond partition (forward rollout)
	if len(liveOldPods) > partition {
		toDelete := len(liveOldPods) - partition
		log.Info("Rolling update: deleting old pods beyond partition", "toDelete", toDelete)
		return r.deletePods(ctx, liveOldPods[:toDelete], toDelete, newHash)
	}

	log.Info("Rolling update step completed — no further action")
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

func (r *CustomWorkloadReconciler) createPods(ctx context.Context, workload *customreplicasetv1.CustomWorkload, podsToCreate int, templateHash string) error {
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

//func (r *CustomWorkloadReconciler) ensureRevisionHistory(ctx context.Context, workload *customreplicasetv1.CustomWorkload, newHash string, log logr.Logger) (*appsv1.ControllerRevision, error) {
//	return nil, nil
//}

func calculateTemplateHash(spec corev1.PodTemplateSpec) (string, error) {
	bytes, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(bytes)
	fullHash := hex.EncodeToString(hash[:])
	return fullHash[:10], nil
}

func convertPodTemplateToJson(template corev1.PodTemplateSpec) ([]byte, error) {
	spec, err := json.Marshal(template)
	if err != nil {
		fmt.Println("Failed to convert PodTemplateSpec to JSON")
		return nil, err
	}
	return spec, nil
}

func hashControllerRevisionData(data runtime.RawExtension) (string, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Failed to convert ControllerRevision data to JSON")
		return "", err
	}
	hash := sha256.Sum256(jsonData)
	return fmt.Sprintf("%x", hash), nil
}

func (r *CustomWorkloadReconciler) manageControllerRevisionHistory(ctx context.Context, workload *customreplicasetv1.CustomWorkload) (*appsv1.ControllerRevision, error) {
	jsonSpec, err := convertPodTemplateToJson(workload.Spec.Template)
	if err != nil {
		return nil, err
	}

	newRevisionData := runtime.RawExtension{Raw: jsonSpec}
	newRevisionHash, err := hashControllerRevisionData(newRevisionData)
	if err != nil {
		return nil, err
	}

	revisionList := &appsv1.ControllerRevisionList{}
	if err := r.List(ctx, revisionList, client.InNamespace(workload.Namespace)); err != nil {
		return nil, err
	}

	// look for existing revisions
	for _, rev := range revisionList.Items {
		hash, err := hashControllerRevisionData(rev.Data)
		if err != nil {
			continue
		}
		if hash == newRevisionHash {
			// found existing revision
			return &rev, nil
		}
	}

	// revision not found, create a new one
	revision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-rev-", workload.Name),
			Namespace:    workload.Namespace,
			Labels: map[string]string{
				"customworkload": workload.Name,
			},
		},
		Data:     newRevisionData,
		Revision: time.Now().UnixNano(),
	}

	if err := controllerutil.SetControllerReference(workload, revision, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, revision); err != nil {
		fmt.Println("Failed to create ControllerRevision:", err)
		return nil, err
	}

	// Cleanup excess revisions
	limit := workload.Spec.RevisionHistoryLimit
	if limit > 0 && len(revisionList.Items) >= int(limit) {
		sort.Slice(revisionList.Items, func(i, j int) bool {
			return revisionList.Items[i].Revision < revisionList.Items[j].Revision
		})
		for i := 0; i < len(revisionList.Items)-int(limit); i++ {
			_ = r.Delete(ctx, &revisionList.Items[i])
		}
	}
	return revision, nil
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
		For(&customreplicasetv1.CustomWorkload{}).
		Owns(&corev1.Pod{}).
		Named("customworkload").
		Complete(r)
}
