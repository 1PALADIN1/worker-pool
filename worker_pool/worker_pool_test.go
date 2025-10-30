package worker_pool_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/1PALADIN1/worker-pool/worker_pool"
)

func TestWorkerPool_Submit(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name            string
		numberOfWorkers int
		tasksAmount     int
	}{
		{
			name:            "5 воркер пуллов 10 задач",
			numberOfWorkers: 5,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 10 задач",
			numberOfWorkers: 10,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 5 задач",
			numberOfWorkers: 10,
			tasksAmount:     5,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var wg sync.WaitGroup
			wg.Add(testCase.tasksAmount)

			actualTasks := make([]int, testCase.tasksAmount)
			expectedTasks := make([]int, testCase.tasksAmount)
			for i := range expectedTasks {
				expectedTasks[i] = i + 1
			}

			wp := worker_pool.NewWorkerPool(testCase.numberOfWorkers)
			for i := range testCase.tasksAmount {
				wp.Submit(func() {
					time.Sleep(200 * time.Millisecond)
					actualTasks[i] = i + 1
					wg.Done()
				})
			}

			wg.Wait()

			for _, taskID := range actualTasks {
				require.NotEqual(t, taskID, 0)
				assert.True(t, slices.Contains(expectedTasks, taskID))
			}
		})
	}
}

func TestWorkerPool_SubmitWait(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name            string
		numberOfWorkers int
		tasksAmount     int
	}{
		{
			name:            "5 воркер пуллов 10 задач",
			numberOfWorkers: 5,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 10 задач",
			numberOfWorkers: 10,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 5 задач",
			numberOfWorkers: 10,
			tasksAmount:     5,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var wg sync.WaitGroup
			wg.Add(testCase.tasksAmount)

			actualTasks := make([]int, testCase.tasksAmount)
			expectedTasks := make([]int, testCase.tasksAmount)
			for i := range expectedTasks {
				expectedTasks[i] = i + 1
			}

			wp := worker_pool.NewWorkerPool(testCase.numberOfWorkers)
			for i := range testCase.tasksAmount {
				go func(i int) {
					wp.SubmitWait(func() {
						time.Sleep(200 * time.Millisecond)
					})

					actualTasks[i] = i + 1
					wg.Done()
				}(i)
			}

			wg.Wait()

			for _, taskID := range actualTasks {
				require.NotEqual(t, taskID, 0)
				assert.True(t, slices.Contains(expectedTasks, taskID))
			}
		})
	}
}

func TestWorkerPool_Stop(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name            string
		numberOfWorkers int
		tasksAmount     int
	}{
		{
			name:            "5 воркер пуллов 10 задач (успевают выполниться только 5 задач)",
			numberOfWorkers: 5,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 10 задач (все задачи успевают выполниться)",
			numberOfWorkers: 10,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 5 задач (все задачи успевают выполниться)",
			numberOfWorkers: 10,
			tasksAmount:     5,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			workingTasksAmount := min(testCase.tasksAmount, testCase.numberOfWorkers)
			actualTasks := make([]int, workingTasksAmount)
			expectedTasks := make([]int, workingTasksAmount)
			for i := range expectedTasks {
				expectedTasks[i] = i + 1
			}

			wp := worker_pool.NewWorkerPool(testCase.numberOfWorkers)
			for i := range testCase.tasksAmount {
				wp.Submit(func() {
					time.Sleep(200 * time.Millisecond)
					actualTasks[i] = i + 1
				})
			}

			wp.Stop()

			for _, taskID := range actualTasks {
				require.NotEqual(t, taskID, 0)
				assert.True(t, slices.Contains(expectedTasks, taskID))
			}
		})
	}
}

func TestWorkerPool_StopWait(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name            string
		numberOfWorkers int
		tasksAmount     int
	}{
		{
			name:            "5 воркер пуллов 10 задач",
			numberOfWorkers: 5,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 10 задач",
			numberOfWorkers: 10,
			tasksAmount:     10,
		},
		{
			name:            "10 воркер пуллов 5 задач",
			numberOfWorkers: 10,
			tasksAmount:     5,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			actualTasks := make([]int, testCase.tasksAmount)
			expectedTasks := make([]int, testCase.tasksAmount)
			for i := range expectedTasks {
				expectedTasks[i] = i + 1
			}

			wp := worker_pool.NewWorkerPool(testCase.numberOfWorkers)
			for i := range testCase.tasksAmount {
				wp.Submit(func() {
					time.Sleep(200 * time.Millisecond)
					actualTasks[i] = i + 1
				})
			}

			wp.StopWait()

			for _, taskID := range actualTasks {
				require.NotEqual(t, taskID, 0)
				assert.True(t, slices.Contains(expectedTasks, taskID))
			}
		})
	}
}
