package base

import "github.com/fluid-cloudnative/fluid/pkg/metrics"

type EngineMetrics struct {
	*metrics.DatasetMetrics
	*metrics.RuntimeMetrics
}

func NewEngineMetrics(namespace, name, runtimeType string) *EngineMetrics {
	return &EngineMetrics{
		DatasetMetrics: metrics.GetDatasetMetrics(namespace, name),
		RuntimeMetrics: metrics.NewRuntimeMetrics(runtimeType, namespace, name),
	}
}

func (m *EngineMetrics) ForgetMetrics() bool {
	done1 := m.DatasetMetrics.CleanUpMetrics()
	done2 := m.RuntimeMetrics.CleanUpMetrics()

	return done1 && done2
}
