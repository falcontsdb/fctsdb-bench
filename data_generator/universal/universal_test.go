package universal

import (
	"fmt"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/db_client"
)

func TestNewPointDevopsEasy(t *testing.T) {

	now := time.Now()

	cfg := &UniversalSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      2,
		DeviceOffset:     0,
		MeasurementCount: 3,
		TagKeyCount:      3,
		FieldsDefine:     [3]int64{2, 3, 1},
	}
	sim := cfg.ToSimulator()
	ser := db_client.NewFctsdbClient(db_client.ClientConfig{})
	point := common.MakeUsablePoint()
	out := make([]byte, 0, 4*1024)
	for i := 0; i < 20; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializeAndAppendPoint(out, point)
		fmt.Println(string(out))
		out = out[:0]
		point.Reset()
	}
}

func BenchmarkNewPointDevopsEasy(b *testing.B) {

	now := time.Now()

	cfg := &UniversalSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      2,
		DeviceOffset:     0,
		TagKeyCount:      3,
		FieldsDefine:     [3]int64{6, 3, 1},
	}
	sim := cfg.ToSimulator()
	// ser := serializers.NewSerializerInflux()
	point := common.MakeUsablePoint()
	// out := make([]byte, 0, 4*1024)
	for i := 0; i < b.N; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		// ser.SerializePoint(out, point)
		point.Reset()
	}
}
