package bulk_query_fctsdb

import (
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
)

func BenchmarkCarsNewest(b *testing.B) {
	now := time.Now()
	cfg := &vehicle.VehicleSimulatorConfig{
		Start: now.Add(time.Hour * -1000),
		End:   now,

		VehicleCount:  1000,
		VehicleOffset: 0,
	}
	sim := cfg.ToSimulator()
	car := CarsNewest{count: 8}
	// car := OneCarNewest{}
	car.Init(sim)
	for i := 0; i < b.N; i++ {
		car.Next()
		// fmt.Println(car.Next())
		// break
	}
}
