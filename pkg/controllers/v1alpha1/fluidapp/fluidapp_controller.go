/*
 Copyright 2022 The Fluid Authors.

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

package fluidapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/ctrl/watch"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
)

const controllerName string = "FluidAppController"

type FluidAppReconciler struct {
	client.Client
	Recorder record.EventRecorder
	*FluidAppReconcilerImplement
}

func (f *FluidAppReconciler) ControllerName() string {
	return controllerName
}

func (f *FluidAppReconciler) ManagedResource() client.Object {
	return &corev1.Pod{}
}

type reconcileRequestContext struct {
	context.Context
	Log logr.Logger
	pod *corev1.Pod
	types.NamespacedName
}

func NewFluidAppReconciler(client client.Client,
	log logr.Logger,
	recorder record.EventRecorder) *FluidAppReconciler {
	return &FluidAppReconciler{
		Client:                      client,
		Recorder:                    recorder,
		FluidAppReconcilerImplement: NewFluidAppReconcilerImplement(client, log, recorder),
	}
}

// Reconcile reconciles Pod
// +kubebuilder:rbac:groups=v1,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=v1,resources=pods/status,verbs=get;update;patch
func (f *FluidAppReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	requestCtx := reconcileRequestContext{
		Context:        ctx,
		Log:            f.Log.WithValues("fluidapp", request.NamespacedName),
		NamespacedName: request.NamespacedName,
	}
	pod, err := kubeclient.GetPodByName(f.Client, request.Name, request.Namespace)
	if err != nil {
		requestCtx.Log.Error(err, "fetch pod error")
		return reconcile.Result{}, err
	}
	if pod == nil {
		requestCtx.Log.Info("pod not found", "name", request.Name, "namespace", request.Namespace)
		return reconcile.Result{}, nil
	}
	requestCtx.pod = pod

	if watch.ShouldInQueue(pod) {
		requestCtx.Log.Info("reconcile pod with fuse sidecar", "name", request.Name, "namespace", request.Namespace)
		return f.internalReconcile(requestCtx)
	}

	return f.internalReconcileWarmup(requestCtx)
}

func (f *FluidAppReconciler) internalReconcile(ctx reconcileRequestContext) (ctrl.Result, error) {
	pod := ctx.pod

	// umount fuse sidecars
	err := f.umountFuseSidecars(pod)
	if err != nil {
		ctx.Log.Error(err, "umount fuse sidecar error", "podName", pod.Name, "podNamespace", pod.Namespace)
		return utils.RequeueIfError(err)
	}
	return utils.NoRequeue()
}

func (f *FluidAppReconciler) internalReconcileWarmup(ctx reconcileRequestContext) (ctrl.Result, error) {
	pod := ctx.pod
	var datasetWarmupPathMap map[string][]string
	for k, v := range pod.Annotations {
		if k == "fluid.io/auto-dataset-warmup" {
			datasetWarmupPathMap = parseWarmupInfo(v)
			break
		}
	}
	if datasetWarmupPathMap == nil {
		return ctrl.Result{}, nil
	}

	for datasetName, targetPaths := range datasetWarmupPathMap {
		dataload := &datav1alpha1.DataLoad{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: pod.Namespace,
				Name:      fmt.Sprintf("%s-warmup", datasetName),
			},
			Spec: datav1alpha1.DataLoadSpec{
				Dataset: datav1alpha1.TargetDataset{
					Name:      datasetName,
					Namespace: pod.Namespace,
				},
				LoadMetadata: true,
				Target: []datav1alpha1.TargetPath{},
			},
		}

		for _, tp := range targetPaths {
			dataload.Spec.Target = append(dataload.Spec.Target, datav1alpha1.TargetPath{
				Path: tp,
			})
		}

		if err := f.Client.Create(context.TODO(), dataload); err != nil {
			if utils.IgnoreAlreadyExists(err) == nil {
				continue
			}
			return utils.RequeueIfError(err)
		}
	}

	return ctrl.Result{}, nil
}

func (f *FluidAppReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return watch.SetupAppWatcherWithReconciler(mgr, options, f)
}

func NewCache(scheme *runtime.Scheme) cache.NewCacheFunc {
	return cache.BuilderWithOptions(cache.Options{
		Scheme: scheme,
		SelectorsByObject: cache.SelectorsByObject{
			&corev1.Pod{}: {Label: labels.SelectorFromSet(labels.Set{
				"app": "tgi-llama2",
				// common.InjectSidecarDone: common.True,
			})},
		},
	})
}

func parseWarmupInfo(warmupInfoStr string) (datasetWarmupPathMap map[string][]string) {
	datasetWarmupPathMap = map[string][]string{}

	// e.g. warmupInfoStr is like "<dataset1>:/path1,/path/to/dir;<dataset2>:/path2"
	warmupInfoByDataset := strings.Split(warmupInfoStr, ";")
	if len(warmupInfoByDataset) == 0 {
		return
	}

	for _, info := range warmupInfoByDataset {
		items := strings.Split(info, ":")
		if len(items) < 2 {
			continue
		}
		datasetName := items[0]
		targetPaths := strings.Split(items[1], ",")
		datasetWarmupPathMap[datasetName] = targetPaths
	}

	return
}
