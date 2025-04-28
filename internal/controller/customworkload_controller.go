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

	if err := r.managePods(ctx, &workload, childPods, log); err != nil {
		return ctrl.Result{Requeue: true}, err
	}

	// Create or update template revision
	//latestRevision, err := r.manageTemplateRevision(ctx, &workload)
	//if err != nil {
	//	return ctrl.Result{Requeue: true}, err
	//}

	// Group Pods by revision hash
	//podsByRevision, updatedCount, err := groupPodsByTemplateHash(childPods, latestRevision)
	//if err != nil {
	//	return ctrl.Result{Requeue: true}, err
	//}

	// Manage Pods: create/delete based on replicas, partition, and revision
	//if err := r.managePods(ctx, &workload, podsByRevision, updatedCount, latestRevision); err != nil {
	//	return ctrl.Result{Requeue: true}, err
	//}

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

func (r *CustomWorkloadReconciler) managePods(ctx context.Context, workload *appsv1.CustomWorkload, childPods corev1.PodList, log logr.Logger) error {
	existing := len(childPods.Items)
	desired := int(workload.Spec.Replicas)

	// Scale up - create new pods
	if existing < desired {
		podsToCreate := desired - existing
		log.Info("Scaling up pods", "desired", desired, "existing", existing)
		if err := r.createPods(ctx, workload, podsToCreate); err != nil {
			log.Error(err, "Failed to create Pods")
			return err
		}
	}

	// Scale down - delete excess pods
	if existing > desired {
		podsToDelete := existing - desired
		log.Info("Scaling down pods", "desired", desired, "existing", existing)
		if err := r.deletePods(ctx, childPods.Items, podsToDelete); err != nil {
			log.Error(err, "Failed to delete Pods")
			return err
		}
	}

	return nil
}

func (r *CustomWorkloadReconciler) createPods(ctx context.Context, workload *appsv1.CustomWorkload, podsToCreate int) error {
	for podsToCreate > 0 {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: workload.Name + "-",
				Namespace:    workload.Namespace,
				Labels: map[string]string{
					"customworkload": workload.Name,
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
		podsToCreate--
	}
	return nil
}

func (r *CustomWorkloadReconciler) deletePods(ctx context.Context, childPods []corev1.Pod, podsToDelete int) error {
	// sort pods by oldest first
	sort.Slice(childPods, func(i, j int) bool {
		return childPods[i].CreationTimestamp.Time.Before(childPods[j].CreationTimestamp.Time)
	})

	// delete the oldest pods first
	toDelete := childPods[:podsToDelete]
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

// SetupWithManager sets up the controller with the Manager.
func (r *CustomWorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.CustomWorkload{}).
		Owns(&corev1.Pod{}).
		Named("customworkload").
		Complete(r)
}
