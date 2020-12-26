package jindo

import "github.com/fluid-cloudnative/fluid/pkg/ddc/base"

// getRuntimeInfo gets runtime info
func (e *JindoEngine) getRuntimeInfo() (base.RuntimeInfoInterface, error) {
	if e.runtimeInfo == nil {
		runtime, err := e.getRuntime()
		if err != nil {
			return e.runtimeInfo, err
		}
		e.runtimeInfo, err = base.BuildRuntimeInfo(e.name, e.namespace, e.runtimeType, runtime.Spec.Tieredstore)
		if err != nil {
			return e.runtimeInfo, err
		}
	}
	return e.runtimeInfo, nil
}
