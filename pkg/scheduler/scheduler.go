package scheduler

import (
	"context"
	"fmt"
	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/scheduler/queue"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type Scheduler struct {
	Client         client.Client
	SchedulerQueue *queue.Queue
	Log            logr.Logger
	RunningJobs    map[string]*v1alpha1.FluidJob
}

func (s *Scheduler) Run() {
	stopCh := make(chan struct{})
	go wait.Until(s.ScheduleOnce, time.Second*20, stopCh)
}

func (s *Scheduler) ScheduleOnce() {
	length := s.SchedulerQueue.Length()
	if length == 0 {
		s.Log.Info("No pending job, skipping scheduling")
		return
	}

	err := s.checkJobRunningStatus()
	if err != nil {
		s.Log.Error(err, "cannot check job running status")
		return
	}

	should := s.shouldScheduleNextJob()
	if !should {
		s.Log.Info("No resources for scheduling next job, just wait...")
		return
	}

	job := s.SchedulerQueue.GetOne()
	s.Log.Info("Scheduling job", "job name", job.Name)

	err = s.installJob(job)
	if err != nil {
		s.Log.Error(err, "can't install job", "job namespace", job.Namespace, "job name", job.Name)
		return
	}

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if j, err := utils.GetFluidJob(s.Client, job.Name, job.Namespace); err != nil {
			return err
		} else {
			j.Status.Phase = v1alpha1.PhaseRunning
			if err = s.Client.Status().Update(context.TODO(), j); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		s.Log.Error(err, "can't update phase for job", "job name", job.Name)
		return
	}

	s.RunningJobs[job.Name] = job
}

func (s *Scheduler) checkJobRunningStatus() error {
	for name, job := range s.RunningJobs {
		var shouldRemove bool

		masterPodName := fmt.Sprintf("%s-master-0", name)
		pod, err := kubeclient.GetPod(s.Client, masterPodName, job.Namespace)
		if err != nil {
			if utils.IgnoreNotFound(err) == nil {
				shouldRemove = true
				s.Log.Info("Found master pod not exists", "pod name", masterPodName)
			} else {
				return err
			}
		} else {
			if pod != nil {
				if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
					shouldRemove = true
				}
			}
		}

		if shouldRemove {
			s.Log.Info("Remove job", "job name", name)
			delete(s.RunningJobs, name)
		}
	}

	return nil
}

func (s *Scheduler) shouldScheduleNextJob() bool {
	return len(s.RunningJobs) < 2
}

func (s *Scheduler) installDataset(job *v1alpha1.FluidJob) error {
	datasetName := job.Spec.JobRef.DataClaim
	datasetSpec := job.Spec.DatasetRef

	dataset := &v1alpha1.Dataset{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "data.fluid.io/v1alpha1",
			Kind:       "Dataset",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      datasetName,
		},
		Spec:   datasetSpec,
		Status: v1alpha1.DatasetStatus{},
	}

	runtimeSpec := job.Spec.RuntimeRef
	runtime := &v1alpha1.AlluxioRuntime{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "data.fluid.io/v1alpha1",
			Kind:       "AlluxioRuntime",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      datasetName,
		},
		Spec:   runtimeSpec,
		Status: v1alpha1.RuntimeStatus{},
	}

	err := s.Client.Create(context.TODO(), dataset)
	if err != nil {
		return err
	}

	err = s.Client.Create(context.TODO(), runtime)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) installJob(job *v1alpha1.FluidJob) error {
	jobConfig := job.Spec.JobRef

	compSpec := CompSpec{
		Command:    jobConfig.Command,
		Env:        jobConfig.Env,
		Image:      jobConfig.Image,
		WorkingDir: jobConfig.WorkingDir,
	}

	compSpec.DataClaim = jobConfig.DataClaim

	value := &PyTorchJobArgs{
		Metadata: Metadata{
			Name:      job.Name,
			Namespace: job.Namespace,
		},
		Master: Master{
			Replicas: jobConfig.MasterReplicas,
			Spec:     compSpec,
		},
		Worker: Worker{
			Replicas: jobConfig.WorkerReplicas,
			Spec:     compSpec,
		},
	}

	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}

	valueFile, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("%s-%s-values.yaml", job.Namespace, job.Name))
	if err != nil {
		return err
	}

	valueFileName := valueFile.Name()
	err = ioutil.WriteFile(valueFileName, data, 0400)
	if err != nil {
		return err
	}

	err = helm.InstallRelease(job.Name, job.Namespace, valueFileName, "/charts/pytorchjob")
	if err != nil {
		return err
	}

	return nil
}
