package queue

import "sort"

var _ SortStrategy = &FIFOStrategy{}

type FIFOStrategy struct {
}

type jobs []JobInQueue

func (j jobs) Len() int {
	return len(j)
}

func (j jobs) Less(a, b int) bool {
	return j[a].EnqueueTimestamp.Before(j[b].EnqueueTimestamp)
}

func (j jobs) Swap(a, b int) {
	j[a], j[b] = j[b], j[a]
}

func (f *FIFOStrategy) Sort(items []JobInQueue) []JobInQueue {
	toRet := jobs(items)
	sort.Sort(toRet)

	return toRet
}
