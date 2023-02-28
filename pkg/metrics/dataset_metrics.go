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
	datasetUFSFileNum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dataset_ufs_file_num",
		Help: "Total num of files of a specific dataset",
	}, []string{"dataset"})

	datasetUFSTotalSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dataset_ufs_total_size",
		Help: "Total size of files in dataset",
	}, []string{"dataset"})
)

type DatasetMetrics struct {
	key string
}

func GetDatasetMetrics(datasetNamespace, datasetName string) *DatasetMetrics {
	ret := &DatasetMetrics{
		key: fmt.Sprintf("%s/%s", datasetNamespace, datasetName),
	}

	return ret
}

func (m *DatasetMetrics) SetUFSTotalSize(size float64) {
	datasetUFSTotalSize.With(prometheus.Labels{"dataset": m.key}).Set(size)
}

func (m *DatasetMetrics) SetUFSFileNum(num float64) {
	datasetUFSFileNum.With(prometheus.Labels{"dataset": m.key}).Set(num)
}

func init() {
	metrics.Registry.MustRegister(datasetUFSFileNum, datasetUFSTotalSize)
}
