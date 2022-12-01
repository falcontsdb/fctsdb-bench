package testscene

import (
	"fmt"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/db_client"
)

func BenchmarkNewPointSceneEasy(b *testing.B) {

	now := time.Now()

	cfg := &SceneConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		SeriesCount:      100000,
		SeriesOffset:     1,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()
	// ser := common.NewSerializerInflux()
	// out := Printer{}
	out := make([]byte, 0, 4*1024)
	ser := db_client.NewFctsdbClient(db_client.ClientConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializeAndAppendPoint(out, point)
		point.Reset()
		out = out[:0]
	}
}

func TestNewPointSceneEasy(t *testing.T) {

	now := time.Now()

	cfg := &SceneConfig{
		Start:            now.Add(time.Second * -10),
		End:              now,
		SamplingInterval: time.Second,
		SeriesCount:      10,
		SeriesOffset:     0,
	}
	sim := cfg.ToSimulator()
	ser := db_client.NewFctsdbClient(db_client.ClientConfig{})
	point := common.MakeUsablePoint()
	out := make([]byte, 0, 4*1024)
	// for i := 0; i < 10; i++ {
	i := 0

	// for sim.Next(point) {
	for !sim.Finished() {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializeAndAppendPoint(out, point)
		fmt.Println(string(out))
		point.Reset()
		out = out[:0]
		i += 1
	}
	fmt.Println(i)
	fmt.Println(sim.SeenPoints())
}
