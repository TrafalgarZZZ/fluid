/*

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

package scheduler

import (
	"context"
	"fmt"
	"github.com/fluid-cloudnative/fluid/pkg/scheduler/queue"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
)

const finalizerName = "fluid-job-controller-finalizer"

// FluidJobReconciler reconciles a FluidJob object
type FluidJobReconciler struct {
	client.Client
	Log            logr.Logger
	Scheme         *runtime.Scheme
	SchedulerQueue *queue.Queue
}

//+kubebuilder:rbac:groups=data.fluid.io,resources=fluidjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=data.fluid.io,resources=fluidjobs/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the FluidJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.6.4/pkg/reconcile
func (r *FluidJobReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()
	log := r.Log.WithValues("fluidjob", req.NamespacedName)

	job, err := utils.GetFluidJob(r.Client, req.Name, req.Namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			return utils.NoRequeue()
		}
		log.Error(err, "unable to get fluid job", "namespace", req.Namespace, "name", req.Name)
		return utils.RequeueIfError(err)
	}

	var object runtime.Object = job
	objectMeta, err := r.GetRuntimeObjectMeta(object)
	if err != nil {
		return utils.RequeueIfError(err)
	}

	if !objectMeta.GetDeletionTimestamp().IsZero() {
		return r.ReconcileDeletion(job)
	}

	if !utils.ContainsString(objectMeta.GetFinalizers(), finalizerName) {
		return r.AddFinalizerAndRequeue(object, finalizerName)
	}

	r.Log.Info("Adding job to scheduler queue", "job name", job.Name)
	//r.SchedulerQueue <- job
	r.SchedulerQueue.Enqueue(job)

	return utils.NoRequeue()
}

func (r *FluidJobReconciler) GetRuntimeObjectMeta(object runtime.Object) (objectMeta metav1.Object, err error) {
	objectMetaAccessor, isOM := object.(metav1.ObjectMetaAccessor)
	if !isOM {
		err = fmt.Errorf("object is not ObjectMetaAccessor")
		return
	}
	objectMeta = objectMetaAccessor.GetObjectMeta()
	return
}

func (r *FluidJobReconciler) ReconcileDeletion(job *datav1alpha1.FluidJob) (ctrl.Result, error) {
	exists, err := helm.CheckRelease(job.Name, job.Namespace)
	if err != nil {
		r.Log.Error(err, "can't check release for job", "job name", job.Name)
		return utils.RequeueIfError(err)
	}

	if exists {
		err = helm.DeleteRelease(job.Name, job.Namespace)
		if err != nil {
			r.Log.Error(err, "can't delete relase for job", "job name", job.Name)
			return utils.RequeueIfError(err)
		}
	}

	r.Log.Info("before clean up finalizer", "fluidjob", job)
	objectMeta, err := r.GetRuntimeObjectMeta(job)
	if err != nil {
		return utils.RequeueIfError(err)
	}

	if !objectMeta.GetDeletionTimestamp().IsZero() {
		finalizers := utils.RemoveString(objectMeta.GetFinalizers(), finalizerName)
		objectMeta.SetFinalizers(finalizers)
		r.Log.Info("After clean up finalizer", "job", job)
		if err := r.Update(context.TODO(), job); err != nil {
			r.Log.Error(err, "Failed to remove finalzer")
			return utils.RequeueIfError(err)
		}
		r.Log.V(1).Info("Finalizer is removed", "job", job)
	}

	return ctrl.Result{}, nil
}

func (r *FluidJobReconciler) AddFinalizerAndRequeue(object runtime.Object, finalizerName string) (ctrl.Result, error) {
	objectMeta, err := r.GetRuntimeObjectMeta(object)
	if err != nil {
		return utils.RequeueIfError(err)
	}

	prevGeneration := objectMeta.GetGeneration()
	objectMeta.SetFinalizers(append(objectMeta.GetFinalizers(), finalizerName))
	if err := r.Update(context.TODO(), object); err != nil {
		r.Log.Error(err, "Failed to add finalizer", "StatusUpdateError", object)
		return utils.RequeueIfError(err)
	}

	currentGeneration := objectMeta.GetGeneration()
	return utils.RequeueImmediatelyUnlessGenerationChanged(prevGeneration, currentGeneration)
}

// SetupWithManager sets up the controller with the Manager.
func (r *FluidJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datav1alpha1.FluidJob{}).
		Complete(r)
}
