/*
Copyright 2023 The Fluid Authors.

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

package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	datasetCSIMountedNum = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dataset_csi_mounted_num",
		Help: "Total num of mounted pods for a specific dataset on a specific node",
	}, []string{"dataset"})

	datasetSidecarMountedNum = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dataset_sidecar_mounted_num",
		Help: "Total num of mutated pods by webhook",
	}, []string{"dataset"})
)

type MountMetrics struct {
	key string
}

func GetMountMetrics(datasetNamespace, datasetName string) *MountMetrics {
	return &MountMetrics{
		key: fmt.Sprintf("%s/%s", datasetNamespace, datasetName),
	}
}

func (m *MountMetrics) CSIMountedNumInc() {
	datasetCSIMountedNum.With(prometheus.Labels{"dataset": m.key}).Inc()
}

func (m *MountMetrics) SidecarMountedNumInc() {
	datasetSidecarMountedNum.With(prometheus.Labels{"dataset": m.key}).Inc()
}

func init() {
	metrics.Registry.MustRegister(datasetUFSFileNum, datasetUFSTotalSize)
}
