package worker_pool

import (
	"log"
	"sync/atomic"
)

type WorkerPool struct {
	nextTaskID        uint64
	numberOfWorkers   int
	activeTasksAmount int
	pendingTasks      *Queue[taskInfo]

	// channels
	submitTaskChan   chan taskInfo
	taskFinishedChan chan taskInfo
	stopChan         chan commandFinishedSignal
	stopWaitChan     chan commandFinishedSignal
}

func NewWorkerPool(numberOfWorkers int) *WorkerPool {
	wp := &WorkerPool{
		numberOfWorkers: numberOfWorkers,
		pendingTasks:    NewQueue[taskInfo](),

		submitTaskChan:   make(chan taskInfo),
		taskFinishedChan: make(chan taskInfo),
		stopChan:         make(chan commandFinishedSignal),
		stopWaitChan:     make(chan commandFinishedSignal),
	}

	go wp.schedule()
	return wp
}

// Submit - добавить таску в воркер пул.
func (wp *WorkerPool) Submit(fn func()) {
	taskID := atomic.AddUint64(&wp.nextTaskID, 1)
	wp.submitTaskChan <- newTaskInfo(taskID, fn)
}

// SubmitWait - добавить таску в воркер пул и дождаться окончания ее выполнения.
func (wp *WorkerPool) SubmitWait(fn func()) {
	taskID := atomic.AddUint64(&wp.nextTaskID, 1)
	task := newTaskInfo(taskID, fn)
	wp.submitTaskChan <- task
	task.taskFinished.wait()
}

// Stop - остановить воркер пул, дождаться выполнения только тех тасок, которые выполняются сейчас.
func (wp *WorkerPool) Stop() {
	log.Println("stop worker pool and wait for finishing only active tasks")
	commandFinished := newCommandFinishedSignal()
	wp.stopChan <- commandFinished
	commandFinished.wait()
}

// StopWait - остановить воркер пул, дождаться выполнения всех тасок, даже тех,
// что не начали выполняться, но лежат в очереди.
func (wp *WorkerPool) StopWait() {
	log.Println("stop worker pool and wait for finishing all tasks")
	commandFinished := newCommandFinishedSignal()
	wp.stopWaitChan <- commandFinished
	commandFinished.wait()
}

func (wp *WorkerPool) schedule() {
	log.Printf("start worker pool, number of workers=%d\n", wp.numberOfWorkers)

	for {
		select {
		case task := <-wp.submitTaskChan:
			if wp.activeTasksAmount >= wp.numberOfWorkers {
				wp.pendingTasks.Enqueue(task)
				break
			}

			wp.runTask(task)

		case result := <-wp.taskFinishedChan:
			result.taskFinished.emit()
			wp.activeTasksAmount--

			if wp.pendingTasks.HasValues() {
				wp.runTask(wp.pendingTasks.Dequeue())
			}

		case commandFinished := <-wp.stopChan:
			if wp.activeTasksAmount > 0 {
				for result := range wp.taskFinishedChan {
					result.taskFinished.emit()
					wp.activeTasksAmount--

					if wp.activeTasksAmount == 0 {
						break
					}
				}
			}

			commandFinished.emit()
			return

		case commandFinished := <-wp.stopWaitChan:
			if wp.activeTasksAmount > 0 {
				for result := range wp.taskFinishedChan {
					result.taskFinished.emit()
					wp.activeTasksAmount--

					if wp.pendingTasks.HasValues() {
						wp.runTask(wp.pendingTasks.Dequeue())
						continue
					}

					if wp.activeTasksAmount == 0 {
						break
					}
				}
			}

			commandFinished.emit()
			return
		}
	}
}

func (wp *WorkerPool) runTask(task taskInfo) {
	wp.activeTasksAmount++
	log.Printf("start executing task %d, active tasks %d", task.taskID, wp.activeTasksAmount)

	go func() {
		task.fn()
		log.Printf("task %d finished", task.taskID)
		wp.taskFinishedChan <- task
	}()
}
