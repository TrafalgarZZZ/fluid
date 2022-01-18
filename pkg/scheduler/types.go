package scheduler

import v1 "k8s.io/api/core/v1"

type PyTorchJobArgs struct {
	Metadata Metadata `yaml:"metadata"`
	Master   Master   `yaml:"master,omitempty"`
	Worker   Worker   `yaml:"worker,omitempty"`
}

type Metadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type Master struct {
	Replicas int      `yaml:"replicas"`
	Spec     CompSpec `yaml:",inline"`
}

type Worker struct {
	Replicas int      `yaml:"replicas"`
	Spec     CompSpec `yaml:",inline"`
}

type CompSpec struct {
	Command    []string    `yaml:"command"`
	Env        []v1.EnvVar `yaml:"env"`
	Image      string      `yaml:"image"`
	DataClaim  string      `yaml:"dataClaim"`
	WorkingDir string      `yaml:"workingDir"`
}
