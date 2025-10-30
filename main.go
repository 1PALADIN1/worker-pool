package main

import (
	"time"

	"github.com/1PALADIN1/worker-pool/worker_pool"
)

const (
	numberOfWorkers = 20
	tasksAmount     = 2_000
)

func main() {
	wp := worker_pool.NewWorkerPool(numberOfWorkers)
	for range tasksAmount {
		wp.Submit(func() {
			time.Sleep(200 * time.Millisecond)
		})
	}

	wp.StopWait()
}
