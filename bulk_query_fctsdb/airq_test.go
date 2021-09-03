package bulk_query_fctsdb

import (
	"fmt"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
)

func TestAllTypes(t *testing.T) {
	now := time.Now()
	cfg := &airq.AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -1000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 0,
	}
	sim := cfg.ToSimulator()
	for i := 1; i <= AirQuality.Count; i++ {
		qt := AirQuality.Types[i]
		qt.Generator.Init(sim)
		fmt.Println(qt.Generator.Next())
	}
}
