package kubeclient

import (
	"context"

	"github.com/fluid-cloudnative/fluid/pkg/utils/compatibility"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetCronJobStatus gets CronJob's status given its namespace and name. It converts batchv1beta1.CronJobStatus
// to batchv1.CronJobStatus when batchv1.CronJob is not supported by the cluster.
func GetCronJobStatus(client client.Client, key types.NamespacedName) (*batchv1.CronJobStatus, error) {
	if compatibility.IsBatchV1CronJobSupported() {
		var cronjob batchv1.CronJob
		if err := client.Get(context.TODO(), key, &cronjob); err != nil {
			return nil, err
		}
		return &cronjob.Status, nil
	}

	var cronjob batchv1beta1.CronJob
	if err := client.Get(context.TODO(), key, &cronjob); err != nil {
		return nil, err
	}
	// Convert batchv1beta1.CronJobStatus to batchv1.CronJobStatus and return
	return &batchv1.CronJobStatus{
		Active:             cronjob.Status.Active,
		LastScheduleTime:   cronjob.Status.LastScheduleTime,
		LastSuccessfulTime: cronjob.Status.LastSuccessfulTime,
	}, nil
}
