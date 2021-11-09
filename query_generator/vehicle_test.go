package fctsdb_query_gen

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
)

var (
	now = time.Now()
	cfg = &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -1000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     0,
	}
	sim = cfg.ToSimulator()
)

func BenchmarkCarsNewest(b *testing.B) {

	car := CarsNewest{count: 1000}
	// car := OneCarNewest{}
	car.Init(sim)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		car.Next()
		// fmt.Println(car.Next())
		// break
	}
}

func TestCarsNewest(t *testing.T) {
	now := time.Now()
	cfg := &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -1000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     0,
	}
	sim := cfg.ToSimulator()

	car := CarsNewest{count: 10}
	car.Init(sim)
	// car := OneCarNewest{}
	// fmt.Println(car.Next())
	n := 100000
	start := time.Now()
	for i := 0; i < 10; i++ {
		// car.Next()
		fmt.Println(car.Next())
		// break
	}
	runt := time.Since(start)
	fmt.Println(float64(n) / runt.Seconds())

	// fmt.Println(len(sim.Hosts[1].Name))
	// fmt.Println(len(`select * from vehicle where VIN in (`))
	// fmt.Println(len(`) group by VIN order by time desc limit 1;`))
}

func TestB(t *testing.T) {
	slice := []string{"a", "b", "c", "d", "e", "f"}
	// r := rand.New(rand.NewSource(time.Now().Unix()))
	ShuffleString(slice)
	fmt.Println(slice)
}

func ShuffleString(slice []string) {
	rand.Seed(4)
	// for len(slice) > 0 {
	n := len(slice)
	for i := 0; i < len(slice); i++ {
		// n := len(slice)
		randIndex := rand.Intn(n)
		// randIndex = i
		slice[i], slice[randIndex] = slice[randIndex], slice[i]
		// slice = slice[:n-1]
		fmt.Println(slice, randIndex, i)
	}
}
