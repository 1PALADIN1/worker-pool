package worker_pool

type taskInfo struct {
	taskID       uint64
	fn           func()
	taskFinished commandFinishedSignal
}

func newTaskInfo(taskID uint64, fn func()) taskInfo {
	return taskInfo{
		taskID:       taskID,
		fn:           fn,
		taskFinished: make(commandFinishedSignal),
	}
}

type commandFinishedSignal chan struct{}

func newCommandFinishedSignal() commandFinishedSignal {
	return make(commandFinishedSignal)
}

func (s commandFinishedSignal) emit() {
	close(s)
}

func (s commandFinishedSignal) wait() {
	<-s
}

type (
	Queue[T any] struct {
		size int
		head *Node[T]
		last *Node[T]
	}

	Node[T any] struct {
		nextNode *Node[T]
		value    T
	}
)

func NewQueue[T any]() *Queue[T] {
	return new(Queue[T])
}

func (t *Queue[T]) Enqueue(value T) {
	if t.size == 0 {
		t.head = &Node[T]{
			value: value,
		}
		t.last = t.head
	} else {
		newLastNode := &Node[T]{
			value: value,
		}
		t.last.nextNode = newLastNode
		t.last = newLastNode
	}

	t.size++
}

func (t *Queue[T]) Dequeue() (value T) {
	t.size--
	value = t.head.value
	t.head = t.head.nextNode

	if t.size == 0 {
		t.last = nil
	}

	return value
}

func (t *Queue[T]) HasValues() bool {
	return t.size > 0
}
