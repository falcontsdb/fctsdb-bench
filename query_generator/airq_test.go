package fctsdb_query_gen

import (
	"fmt"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq"
)

var (
	cfgA = &airq.AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -1000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     0,
	}
	simA = cfgA.ToSimulator()
)

func TestAllTypes(t *testing.T) {
	now := time.Now()
	cfg := &airq.AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -1000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     0,
	}
	sim := cfg.ToSimulator()
	for i := 1; i <= AirQuality.Count; i++ {
		qt := AirQuality.Types[i]
		qt.Generator.Init(sim)
		fmt.Println(qt.Generator.Next())
	}
}

func BenchmarkAirqFromSitesNewest(b *testing.B) {

	aiq := airqFromSitesNewest{count: 1000}
	aiq.Init(simA)
	for i := 0; i <= b.N; i++ {
		aiq.Next()
	}
}

func TestAirqFromSitesNewest(t *testing.T) {
	now := time.Now()
	cfg := &airq.AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -1000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     0,
	}
	sim := cfg.ToSimulator()
	aiq := airqFromSitesNewest{count: 1000}
	aiq.Init(sim)
	n := 100000
	start := time.Now()
	for i := 0; i < n; i++ {
		aiq.Next()
	}
	runt := time.Since(start)
	fmt.Println(float64(n) / runt.Seconds())
}
