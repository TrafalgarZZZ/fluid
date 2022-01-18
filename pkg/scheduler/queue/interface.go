package queue

type SortStrategy interface {
	Sort(items []JobInQueue) []JobInQueue
}
