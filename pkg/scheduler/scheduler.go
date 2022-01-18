package scheduler

import (
	"context"
	"fmt"
	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/scheduler/queue"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type Scheduler struct {
	Client         client.Client
	SchedulerQueue *queue.Queue
	Log            logr.Logger
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
	job := s.SchedulerQueue.GetOne()
	s.Log.Info("Scheduling job", "job name", job.Name)

	// todo check resource availble

	// FIFO Strategy
	err := s.installDataset(job)
	if err != nil {
		s.Log.Error(err, "can't install dataset", "job namespace", job.Namespace, "job name", job.Name)
	}

	err = s.installJob(job)
	if err != nil {
		s.Log.Error(err, "can't install job", "job namespace", job.Namespace, "job name", job.Name)
	}
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
