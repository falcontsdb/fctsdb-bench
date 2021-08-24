package bulk_load

import (
	"sync"

	"git.querycap.com/falcontsdb/fctsdb-bench/util/report"
)

type BatchProcessor interface {
	PrepareProcess(i int)
	RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error
	AfterRunProcess(i int)
	EmptyBatchChanel()
}
