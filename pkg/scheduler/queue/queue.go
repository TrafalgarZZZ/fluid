package queue

import (
	"github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"sync"
	"time"
)

type Queue struct {
	items    []JobInQueue
	mutex    sync.Mutex
	strategy SortStrategy
}

type JobInQueue struct {
	EnqueueTimestamp time.Time
	Job              *v1alpha1.FluidJob
}

func NewQueue(strategy SortStrategy) *Queue {
	return &Queue{
		items:    []JobInQueue{},
		mutex:    sync.Mutex{},
		strategy: strategy,
	}
}

func (q *Queue) Length() int {
	return len(q.items)
}

func (q *Queue) Enqueue(job *v1alpha1.FluidJob) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	enqueueTs := time.Now()
	q.items = append(q.items, JobInQueue{
		EnqueueTimestamp: enqueueTs,
		Job:              job,
	})
}

func (q *Queue) GetOne() *v1alpha1.FluidJob {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.items = q.strategy.Sort(q.items)
	ret := q.items[0].Job
	q.items = q.items[1:]
	return ret
}
