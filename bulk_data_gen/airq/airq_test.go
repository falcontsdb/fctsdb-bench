package airq

import (
	"fmt"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq/gbt2260"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

func TestRegion(t *testing.T) {
	reg := gbt2260.NewGBT2260()
	fmt.Println(reg.SearchGBT2260("310112"))
	// fmt.Println(reg.GetAllAreaCode())
	// fmt.Println(reg.GetAllProvince())
	// fmt.Println(reg.GetCityByProvince("120000"))
	// fmt.Println(reg.GetAreaByCity("110100"))
}

func BenchmarkRegion(b *testing.B) {
	reg := gbt2260.NewGBT2260()
	for i := 0; i < b.N; i++ {
		reg.SearchGBT2260("620823")
	}
}

func TestDevice(t *testing.T) {
	fmt.Println(time.Now())
	// rand.Seed(time.Now().UnixNano())
	// fmt.Println(string(NewDevice(1, 1, time.Now()).Province))
	// fmt.Println(reg.GetAllProvince())
	// fmt.Println(reg.GetCityByProvince("120000"))
	// fmt.Println(reg.GetAreaByCity("110100"))
}

// func BenchmarkNewPoint(b *testing.B) {
// 	now := time.Now()

// 	cfg := &vehicle.VehicleSimulatorConfig{
// 		Start: now.Add(time.Hour * -24000),
// 		End:   now,

// 		VehicleCount:  1000,
// 		VehicleOffset: 1,
// 		StartVinIndex: 10000,
// 	}
// 	sim := cfg.ToSimulator()
// 	point := common.MakeUsablePoint()
// 	for i := 0; i < b.N; i++ {
// 		sim.Next(point)
// 	}
// }

func BenchmarkNewPoint(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	// serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	point := common.MakeUsablePoint()
	for i := 0; i < b.N; i++ {
		sim.Next(point)
		// serializer.SerializePoint(out, point)
		point.Reset()
	}
}
