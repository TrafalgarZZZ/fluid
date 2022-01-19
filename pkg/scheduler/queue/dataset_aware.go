package queue

import (
	"github.com/docker/go-units"
	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/alluxio"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
)

var _ SortStrategy = &DatasetAwareStrategy{}

type DatasetAwareStrategy struct {
	Client client.Client
}

func NewDatasetAwareStrategy(client client.Client) *DatasetAwareStrategy {
	return &DatasetAwareStrategy{Client: client}
}

type DatasetStatus struct {
	TotalSize  int64
	CachedSize int64
}

type JobWithScore struct {
	Job   *JobInQueue
	Score float64
}

type yajobs []JobWithScore

func (y yajobs) Len() int {
	return len(y)
}

func (y yajobs) Less(i, j int) bool {
	return y[i].Score < y[j].Score
}

func (y yajobs) Swap(i, j int) {
	y[i], y[j] = y[j], y[i]
}

func (d *DatasetAwareStrategy) Sort(items []JobInQueue) []JobInQueue {
	var toSort []JobWithScore
	for _, item := range items {
		toSort = append(toSort, d.score(&item))
	}

	sort.Sort(yajobs(toSort))

	var toRet []JobInQueue
	for _, sortedItem := range toSort {
		toRet = append(toRet, *sortedItem.Job)
	}

	return toRet
}

func (d *DatasetAwareStrategy) getDatasetStatusByJob(job *v1alpha1.FluidJob) (DatasetStatus, error) {
	datasetName := job.Spec.JobRef.DataClaim
	namespace := job.Namespace

	dataset, err := utils.GetDataset(d.Client, datasetName, namespace)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			return DatasetStatus{}, nil
		}
		return DatasetStatus{}, err
	}

	if dataset.Status.UfsTotal == "" || dataset.Status.UfsTotal == alluxio.METADATA_SYNC_NOT_DONE_MSG {
		return DatasetStatus{}, nil
	}

	totalSize, _ := units.RAMInBytes(dataset.Status.UfsTotal)

	if cacheStates, ok := dataset.Status.CacheStates[common.Cached]; ok {
		cachedSize, _ := units.RAMInBytes(cacheStates)
		return DatasetStatus{
			TotalSize:  totalSize,
			CachedSize: cachedSize,
		}, nil
	} else {
		return DatasetStatus{}, nil
	}

}

func (d *DatasetAwareStrategy) score(job *JobInQueue) JobWithScore {
	status, _ := d.getDatasetStatusByJob(job.Job)

	if status.TotalSize == 0 || status.CachedSize == 0 {
		return JobWithScore{
			Job:   job,
			Score: 0,
		}
	} else {
		return JobWithScore{
			Job:   job,
			Score: float64(status.CachedSize * 1.0 / status.TotalSize),
		}
	}
}
