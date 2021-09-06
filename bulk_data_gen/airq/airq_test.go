package airq

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq/gbt2260"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
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

var mutex sync.Mutex

type mockWriter struct {
	outchan chan []byte
}

func (m mockWriter) Write(p []byte) (n int, err error) {
	b := make([]byte, len(p))
	copy(b, p)
	m.outchan <- b
	return len(p), nil
}

var pointPool = sync.Pool{
	New: func() interface{} {
		return common.MakeUsablePoint()
	},
}

func BenchmarkNewPointVehicle(b *testing.B) {

	now := time.Now()

	cfg := &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		VehicleCount:     100,
		VehicleOffset:    1,
	}
	outchan := make(chan []byte, 10000)
	sim := cfg.ToSimulator()
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	// str := []byte("aaa")
	for j := 0; j < runtime.NumCPU(); j++ {
		go func() {
			// runtime.LockOSThread()
			point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				// point := pointPool.Get().(*common.Point)
				sim.Next(point)
				// runtime.Gosched()
				serializer.SerializePoint(out, point)
				point.Reset()
				runtime.Gosched()
				// time.Sleep(0)
				// pointPool.Put(point)
			}
		}()
	}

	// n := 1000000
	// start := time.Now()
	for i := 0; i < b.N; i++ {
		<-outchan
	}
	// interval := time.Since(start)
	// fmt.Println(float64(n) / interval.Seconds())
}

func BenchmarkNewPointAirq1_1(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < 1; j++ {
		go func() {
			// runtime.LockOSThread()
			// point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				point := pointPool.Get().(*common.Point)
				sim.Next(point)
				outPointChan <- point
				// runtime.Gosched()
				// serializer.SerializePoint(out, point)
				// point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}()
	}
	for k := 0; k < 1; k++ {
		go func() {
			for p := range outPointChan {
				serializer.SerializePoint(out, p)
				p.Reset()
				pointPool.Put(p)
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func BenchmarkNewPointAirq1_2(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < 1; j++ {
		go func() {
			// runtime.LockOSThread()
			// point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				point := pointPool.Get().(*common.Point)
				sim.Next(point)
				outPointChan <- point
				// runtime.Gosched()
				// serializer.SerializePoint(out, point)
				// point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}()
	}
	for k := 0; k < 2; k++ {
		go func() {
			for p := range outPointChan {
				serializer.SerializePoint(out, p)
				p.Reset()
				pointPool.Put(p)
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func BenchmarkNewPointAirq2_2(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < 2; j++ {
		go func() {
			// runtime.LockOSThread()
			// point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				point := pointPool.Get().(*common.Point)
				sim.Next(point)
				outPointChan <- point
				// runtime.Gosched()
				// serializer.SerializePoint(out, point)
				// point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}()
	}
	for k := 0; k < 2; k++ {
		go func() {
			for p := range outPointChan {
				serializer.SerializePoint(out, p)
				p.Reset()
				pointPool.Put(p)
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func BenchmarkNewPointAirq2_4(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < 2; j++ {
		go func() {
			// runtime.LockOSThread()
			// point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				point := pointPool.Get().(*common.Point)
				sim.Next(point)
				outPointChan <- point
				// runtime.Gosched()
				// serializer.SerializePoint(out, point)
				// point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}()
	}
	for k := 0; k < 4; k++ {
		go func() {
			for p := range outPointChan {
				serializer.SerializePoint(out, p)
				p.Reset()
				pointPool.Put(p)
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func BenchmarkNewPointAirqHand(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	// outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < runtime.NumCPU(); j++ {
		go func() {
			// runtime.LockOSThread()
			point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				// point := pointPool.Get().(*common.Point)
				sim.Next(point)
				// outPointChan <- point
				// runtime.Gosched()
				serializer.SerializePoint(out, point)
				point.Reset()
				runtime.Gosched()
				// pointPool.Put(point)
			}
		}()
	}
	// for k := 0; k < 4; k++ {
	// 	go func() {
	// 		for p := range outPointChan {
	// 			serializer.SerializePoint(out, p)
	// 			p.Reset()
	// 			pointPool.Put(p)
	// 		}
	// 	}()
	// }
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func BenchmarkNewPointAirq(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		AirqDeviceCount:  100000,
		AirqDeviceOffset: 1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	// outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < runtime.NumCPU(); j++ {
		go func() {
			// runtime.LockOSThread()
			point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				// point := pointPool.Get().(*common.Point)
				sim.Next(point)
				// outPointChan <- point
				// runtime.Gosched()
				serializer.SerializePoint(out, point)
				point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}()
	}
	// for k := 0; k < 4; k++ {
	// 	go func() {
	// 		for p := range outPointChan {
	// 			serializer.SerializePoint(out, p)
	// 			p.Reset()
	// 			pointPool.Put(p)
	// 		}
	// 	}()
	// }
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func BenchmarkNewPointAirq2Sim(b *testing.B) {
	simCount := runtime.NumCPU()
	scaleVar := 100000
	now := time.Now()
	simulators := make([]common.Simulator, simCount)
	var step = scaleVar / simCount
	var offset = 0
	for i := 0; i < simCount; i++ {
		offset = step * i
		if i == simCount-1 {
			step = scaleVar - step*i
		}
		cfg := &AirqSimulatorConfig{
			Start:            now.Add(time.Hour * -24000),
			End:              now,
			SamplingInterval: time.Second,
			AirqDeviceCount:  int64(step),
			AirqDeviceOffset: int64(offset),
		}
		simulators[i] = cfg.ToSimulator()
	}

	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	// outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}

	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	for j := 0; j < simCount; j++ {
		go func(w int) {
			// runtime.LockOSThread()
			serializer := common.NewSerializerInflux()
			sim := simulators[w]
			point := common.MakeUsablePoint()
			for !sim.Finished() {
				sim.Next(point)
				serializer.SerializePoint(out, point)
				point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}(j)
	}
	for i := 0; i < b.N; i++ {
		<-outchan
	}
}

func TestNewPointVehicle(t *testing.T) {
	now := time.Now()

	cfg := &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -24),
		End:              now,
		SamplingInterval: time.Second,
		VehicleCount:     100,
		VehicleOffset:    0,
	}
	outchan := make(chan []byte, 10000)
	sim := cfg.ToSimulator()
	out := mockWriter{
		outchan: outchan,
	}
	serializer := common.NewSerializerInflux()
	// str := []byte("aaa")
	var num int32 = 4
	for j := int32(0); j < num; j++ {
		go func() {
			// point := common.MakeUsablePoint()
			for !sim.Finished() {
				// outchan <- &str
				point := pointPool.Get().(*common.Point)
				// mutex.Lock()
				sim.Next(point)
				serializer.SerializePoint(out, point)
				// mutex.Unlock()
				point.Reset()
				pointPool.Put(point)
			}
			nowNum := atomic.AddInt32(&num, -1)
			if nowNum == 0 {
				close(outchan)
			}
		}()
	}

	n := 0
	start := time.Now()
	for p := range outchan {
		n++
		_ = p
		fmt.Println(string(p))
		if n == 30 {
			break
		}
	}
	interval := time.Since(start)
	fmt.Println(float64(n) / interval.Seconds())
	fmt.Println(n)
}

func TestTime(t *testing.T) {
	now := time.Now()
	fmt.Println(now)
	add := now.Add(time.Second * 10)
	fmt.Println(now)
	fmt.Println(add)
}
