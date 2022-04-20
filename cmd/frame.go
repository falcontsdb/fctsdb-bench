package main

import (
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
	PrepareWorkers()
	Run()
	Report()
	CleanUp()
}

func RunBenchTask(task *BasicBenchTask) map[string]string {
	task.Validate()
	task.PrepareWorkers()
	task.Run()
	result := task.Report()
	task.CleanUp()
	return result
}
