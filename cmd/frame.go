package main

import (
	"log"
	"sync"
	"time"

	data_gen "git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

var (
	CaseChoices = []string{
		data_gen.UseCaseVehicle,
		data_gen.UseCaseAirQuality,
		data_gen.UseCaseDevOps,
	}
)

// ClientConfig is the configuration used to create an DBClient.

type BenchTask interface {
	Validate()
	PrepareWorkers() int
	PrepareProcess(int)
	RunProcess(int, *sync.WaitGroup)
	// AfterRunProcess(int)
	RunSyncTask()
	SyncEnd()
	Report(time.Time, time.Time) map[string]string
	CleanUp()
}

func RunBenchTask(task BenchTask) map[string]string {
	task.Validate()
	workers := task.PrepareWorkers()
	var workersGroup sync.WaitGroup
	for i := 0; i < workers; i++ {
		task.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			task.RunProcess(w, &workersGroup)
		}(i)
	}
	log.Printf("Started load with %d workers\n", workers)

	// 定时运行状态日志
	task.RunSyncTask()
	start := time.Now()
	workersGroup.Wait()
	end := time.Now()
	task.SyncEnd()
	result := task.Report(start, end)
	task.CleanUp()
	return result
}
