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
	"github.com/fluid-cloudnative/fluid/pkg/scheduler/queue"
	"github.com/fluid-cloudnative/fluid/pkg/utils"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
)

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
		log.Error(err, "unable to get fluid job", "namespace", req.Namespace, "name", req.Name)
		return utils.RequeueIfError(err)
	}

	r.Log.Info("Adding job to scheduler queue", "job name", job.Name)
	//r.SchedulerQueue <- job
	r.SchedulerQueue.Enqueue(job)

	return utils.NoRequeue()
}

// SetupWithManager sets up the controller with the Manager.
func (r *FluidJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datav1alpha1.FluidJob{}).
		Complete(r)
}
