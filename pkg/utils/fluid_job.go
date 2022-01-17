package utils

import (
	"context"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GetFluidJob(client client.Client, name, namespace string) (*datav1alpha1.FluidJob, error) {

	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	var job datav1alpha1.FluidJob
	if err := client.Get(context.TODO(), key, &job); err != nil {
		return nil, err
	}
	return &job, nil
}
