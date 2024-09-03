package preload

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// FluidMutatingHandler mutates a pod and has implemented admission.DecoderInjector
type PreloadFilesHandler struct {
	Client client.Client
	// A decoder will be automatically injected
	decoder *admission.Decoder
}

var _ common.AdmissionHandler = &PreloadFilesHandler{}

func (a *PreloadFilesHandler) Setup(client client.Client, decoder *admission.Decoder) {
	a.Client = client
	a.decoder = decoder
}

// Handle is the mutating logic of pod
func (a *PreloadFilesHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	defer utils.TimeTrack(time.Now(), "CreateUpdatePodForSchedulingHandler.Handle",
		"req.name", req.Name, "req.namespace", req.Namespace)

	if utils.GetBoolValueFromEnv(common.EnvDisableInjection, false) {
		return admission.Allowed("skip mutating the pod because global injection is disabled")
	}

	var setupLog = ctrl.Log.WithName("handle")
	pod := &corev1.Pod{}
	err := a.decoder.Decode(req, pod)
	if err != nil {
		setupLog.Error(err, "unable to decoder pod from req")
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Before K8s 1.24, pod.Namespace may not be trustworthy so we deny invalid requests for security concern.
	// See related bugfix at https://github.com/kubernetes/kubernetes/pull/94637
	if len(pod.Namespace) != 0 && pod.Namespace != req.Namespace {
		return admission.Denied("found invalid pod.metadata.namespace, it must either be empty or equal to request's namespace")
	}

	var undoNamespaceOverride bool = false
	if len(pod.Namespace) == 0 {
		if len(req.Namespace) == 0 {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("unexepcted error: both pod.metadata.namespace and request's namespace is empty"))
		}
		// Override pod.Namespace with req.Namespace in order to pass namespace info to deeper functions.
		// But we must revert the overriding to avoid a side effect of the mutation.
		setupLog.Info("detecting empty pod.metadata.namespace, overriding it with request.namespace", "request.namespace", req.Namespace)
		pod.Namespace = req.Namespace
		undoNamespaceOverride = true
	}

	// check whether should inject
	if common.CheckExpectValue(pod.Labels, common.EnableFluidInjectionFlag, common.False) {
		setupLog.Info("skip mutating the pod because injection is disabled", "Pod", pod.Name, "Namespace", pod.Namespace)
		return admission.Allowed("skip mutating the pod because injection is disabled")
	}

	err = a.MutatePod(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if undoNamespaceOverride {
		pod.Namespace = ""
	}

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		setupLog.Error(err, "unable to marshal pod")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	resp := admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
	setupLog.V(1).Info("patch response", "name", pod.GetName(), "namespace", pod.GetNamespace(), "patches", utils.DumpJSON(resp.Patch))
	return resp
}

func (a *PreloadFilesHandler) MutatePod(pod *corev1.Pod) error {
	labels := pod.ObjectMeta.Labels
	if val, exists := labels["preload-files-sidecar.fluid.io/inject-done"]; exists && val == "true" {
		return nil
	}

	if val, exists := labels["preload-files-sidecar.fluid.io/inject"]; !exists || val != "true" {
		return nil
	}

	annotations := pod.ObjectMeta.Annotations
	preloadingPath := annotations["preload-files-sidecar.fluid.io/path"]
	if len(preloadingPath) == 0 {
		return errors.New("\"preload-files-sidecar.fluid.io/path\" is required but not found in pod.metadata.annotations")
	}

	preloadingThreadNum := 2 // default thread num is 2
	preloadingThreads := annotations["preload-files-sidecar.fluid.io/num_thread"]
	if len(preloadingThreads) != 0 {
		if num, err := strconv.ParseInt(preloadingThreads, 10, 32); err != nil {
			return errors.Wrapf(err, "failed to parse integer \"%v\" from \"preload-files-sidecar.fluid.io/num_thread\"", preloadingThreads)
		} else {
			preloadingThreadNum = int(num)
		}
	}

	sidecar := a.genPreloadSidecar(preloadingPath, preloadingThreadNum, a.getFluidVolumesNames(pod)[0])

	pod.Spec.Containers = append([]corev1.Container{sidecar}, pod.Spec.Containers...)
	pod.ObjectMeta.Labels["preload-files-sidecar.fluid.io/inject-done"] = "true"

	return nil
}

func (a *PreloadFilesHandler) genPreloadSidecar(preloadingPath string, preloadingThreadNum int, fluidVolName string) corev1.Container {
	var preloadImage = "registry-vpc.cn-beijing.aliyuncs.com/fluid-namespace/fluid-util:preload-sidecar-v0.2"
	if customImage, exists := os.LookupEnv("PRELOAD_SIDECAR_IMAGE"); exists && len(customImage) != 0 {
		preloadImage = customImage
	}
	container := corev1.Container{
		Name:  "fluid-preload",
		Image: preloadImage,
		Env: []corev1.EnvVar{
			{
				Name:  "WARMUP_THREAD_PER_FILE",
				Value: strconv.FormatInt(int64(preloadingThreadNum), 10),
			},
			{
				Name:  "WARMUP_PATH_GLOB",
				Value: filepath.Join("/mnt/models", preloadingPath),
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      fluidVolName,
				MountPath: "/mnt/models",
			},
		},
	}

	return container
}

func (a *PreloadFilesHandler) getFluidVolumesNames(pod *corev1.Pod) []string {
	return []string{"llm-model-vol"}
}
