package airq

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq/gbt2260"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/serializers"
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
		DeviceCount:      100,
		DeviceOffset:     1,
	}
	outchan := make(chan []byte, 10000)
	sim := cfg.ToSimulator()
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
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
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	// outchan := make(chan []byte, 10000)
	// out := mockWriter{
	// 	outchan: outchan,
	// }
	// serializer := common.NewSerializerInflux()
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
	// for k := 0; k < 1; k++ {
	// 	go func() {
	// 		for p := range outPointChan {
	// 			serializer.SerializePoint(out, p)
	// 			p.Reset()
	// 			pointPool.Put(p)
	// 		}
	// 	}()
	// }
	for i := 0; i < b.N; i++ {
		// runtime.LockOSThread()
		p := <-outPointChan
		p.Reset()
		pointPool.Put(p)
	}
}

func BenchmarkNewPointAirq1_2(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)
	runtime.GOMAXPROCS(3)
	for j := 0; j < 1; j++ {
		go func() {
			runtime.LockOSThread()
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
			runtime.LockOSThread()
			for p := range outPointChan {
				serializer.SerializePoint(out, p)
				p.Reset()
				pointPool.Put(p)
				// runtime.Gosched()
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
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
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

func BenchmarkNewPointAirq2_4(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 1000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
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

func BenchmarkNewPointAirq2_4_New(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan1 := make(chan *common.Point, 1000)
	outPointChan2 := make(chan *common.Point, 1000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
	sim := cfg.ToSimulator()
	// point := common.MakeUsablePoint()
	// for i := 0; i < b.N; i++ {
	// 	sim.Next(point)
	// 	serializer.SerializePoint(out, point)
	// 	point.Reset()
	// }
	// point := common.MakeUsablePoint()
	// sim.Next(point)

	go func() {
		for !sim.Finished() {
			point := pointPool.Get().(*common.Point)
			sim.Next(point)
			outPointChan1 <- point

		}
	}()
	go func() {
		for !sim.Finished() {
			point := pointPool.Get().(*common.Point)
			sim.Next(point)
			outPointChan2 <- point
		}
	}()
	// for k := 0; k < 2; k++ {
	go func() {
		for p := range outPointChan1 {
			serializer.SerializePoint(out, p)
			p.Reset()
			pointPool.Put(p)
		}

	}()
	go func() {
		for p := range outPointChan2 {
			serializer.SerializePoint(out, p)
			p.Reset()
			pointPool.Put(p)
		}
	}()
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
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	// outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
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
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	// outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}
	serializer := serializers.NewSerializerInflux()
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
		// runtime.LockOSThread()
		<-outchan
	}
}

func BenchmarkNewPointAirq2Sim(b *testing.B) {
	simCount := 1
	scaleVar := 100000
	now := time.Now()
	simulators := make([]*AirqSimulator, simCount)
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
			DeviceCount:      int64(step),
			DeviceOffset:     int64(offset),
		}
		simulators[i] = cfg.ToSimulator()
	}

	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	// outchan := make(chan []byte, 10000)
	// out := mockWriter{
	// 	outchan: outchan,
	// }

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
			runtime.LockOSThread()
			// serializer := common.NewSerializerInflux()
			sim := simulators[w]
			// point := common.MakeUsablePoint()
			for !sim.Finished() {
				point := pointPool.Get().(*common.Point)
				sim.Next(point)
				outPointChan <- point
				// serializer.SerializePoint(out, point)
				// point.Reset()
				// runtime.Gosched()
				// pointPool.Put(point)
			}
		}(j)
	}
	for i := 0; i < b.N; i++ {
		p := <-outPointChan
		p.Reset()
		pointPool.Put(p)
	}
}

func BenchmarkNewPointAirq2SimWith(b *testing.B) {
	simCount := 2
	scaleVar := 100000
	now := time.Now()
	simulators := make([]*AirqSimulator, simCount)
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
			DeviceCount:      int64(step),
			DeviceOffset:     int64(offset),
		}
		simulators[i] = cfg.ToSimulator()
	}

	// out := bufio.NewWriterSize(os.Stdout, 4<<24)
	outPointChan := make(chan *common.Point, 10000)
	outchan := make(chan []byte, 10000)
	out := mockWriter{
		outchan: outchan,
	}

	for j := 0; j < simCount; j++ {
		go func(w int) {
			sim := simulators[w]
			for !sim.Finished() {
				point := pointPool.Get().(*common.Point)
				sim.Next(point)
				outPointChan <- point
			}
		}(j)
	}
	for j := 0; j < simCount; j++ {
		go func(w int) {
			serializer := serializers.NewSerializerInflux()
			for point := range outPointChan {
				serializer.SerializePoint(out, point)
				point.Reset()
				// runtime.Gosched()
				pointPool.Put(point)
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
		DeviceCount:      100,
		DeviceOffset:     0,
	}
	outchan := make(chan []byte, 10000)
	sim := cfg.ToSimulator()
	out := bytes.NewBuffer(make([]byte, 0, 1024))
	serializer := serializers.NewSerializerInflux()
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

// func BenchmarkRandomString(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		RandomString(10)
// 	}
// }

func BenchmarkNewPointVehicleEasy(b *testing.B) {

	now := time.Now()
	cfg := &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      10000,
		DeviceOffset:     1,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()
	for i := 0; i < b.N; i++ {
		sim.Next(point)
		point.Reset()
	}
}

func TestNewPointVehicleEasy(t *testing.T) {

	now := time.Now()
	cfg := &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()
	out := bytes.NewBuffer(make([]byte, 0, 1024))
	ser := serializers.NewSerializerInflux()
	for i := 0; i < 10; i++ {
		sim.Next(point)
		ser.SerializePoint(out, point)
		fmt.Println(out.String())
		out.Reset()
		point.Reset()
	}
}

func BenchmarkNewPointAirqEasy(b *testing.B) {

	now := time.Now()

	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100000,
		DeviceOffset:     1,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()
	// ser := common.NewSerializerInflux()
	// out := Printer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		// ser.SerializePoint(out, point)
		point.Reset()
	}
}

func TestNewPointAirqEasy(t *testing.T) {

	now := time.Now()

	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Second * -10),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      10,
		DeviceOffset:     0,
	}
	sim := cfg.ToSimulator()
	ser := serializers.NewSerializerInflux()
	point := common.MakeUsablePoint()
	out := Printer{}
	// for i := 0; i < 10; i++ {
	i := 0

	// for sim.Next(point) {
	for !sim.Finished() {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		ser.SerializePoint(out, point)
		point.Reset()
		i += 1
	}
	fmt.Println(i)
	fmt.Println(sim.SeenPoints())
}

type Printer struct {
}

func (m Printer) Write(p []byte) (n int, err error) {
	fmt.Println(string(p))
	return len(p), nil
}

func TestNewPointDevopsEasy(t *testing.T) {

	now := time.Now()

	cfg := &devops.DevopsSimulatorConfig{
		Start: now.Add(time.Hour * -24000),
		End:   now,
		// SamplingInterval: time.Second,
		HostCount:  2,
		HostOffset: 0,
	}
	sim := cfg.ToSimulator()
	ser := serializers.NewSerializerInflux()
	point := common.MakeUsablePoint()
	out := Printer{}
	for i := 0; i < 20; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		ser.SerializePoint(out, point)
		point.Reset()
	}
}

func BenchmarkNewPointDevopsEasy(b *testing.B) {

	now := time.Now()

	cfg := &devops.DevopsSimulatorConfig{
		Start: now.Add(time.Hour * -24000),
		End:   now,
		// SamplingInterval: time.Second,
		HostCount:  10000,
		HostOffset: 0,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()
	// ser := common.NewSerializerInflux()
	// out := Printer{}
	for i := 0; i < b.N; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		// ser.SerializePoint(out, point)
		point.Reset()
	}
}

func TestSafe(t *testing.T) {
	now := time.Now()

	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Second * -1000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100,
		DeviceOffset:     0,
	}
	sim := cfg.ToSimulator()
	// ser := common.NewSerializerInflux()
	wg := sync.WaitGroup{}
	var count int64 = 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// out := bytes.NewBuffer(make([]byte, 0, 8*1024))
			point := common.MakeUsablePoint()
			// for !sim.Finished() {
			for sim.Next(point) <= sim.Total() {
				time.Sleep(time.Microsecond)
				// sim.Next(point)
				// ser.SerializePoint(out, point)
				point.Reset()
				atomic.AddInt64(&count, 1)
				// out.Reset()
			}
		}()
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
	}
	wg.Wait()
	fmt.Println(count)
}

var (
	num    = int64(987654321)
	numstr = "987654321"
)

// strconv.ParseInt
func BenchmarkStrconvParseInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strconv.Itoa(int(num))
		// if x != num || err != nil {
		// 	b.Error(err)
		// }
	}
}

// strconv.Atoi
func BenchmarkStrconvAtoi(b *testing.B) {
	buf := make([]byte, 0, 1024*1024)
	for i := 0; i < b.N; i++ {
		strconv.AppendInt(buf, num, 10)
		// if x != num || err != nil {
		// 	b.Error(err)
		// }
	}
}

// fmt.Sscan
func BenchmarkStrconvFmtSscan(b *testing.B) {
	buf := make([]byte, 0, 1024*1024)
	for i := 0; i < b.N; i++ {
		strconv.AppendInt(buf, 77, 10)
	}
}

func BenchmarkStrconvFmtSscan2(b *testing.B) {
	buf := make([]byte, 0, 1024*1024)
	for i := 0; i < b.N; i++ {
		AppendInt(buf, 77)
	}
}

const digits = "0123456789"
const smallsString = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

func TestStrconvFmtSscan(b *testing.T) {
	buf := make([]byte, 0, 1024*1024)
	buf = AppendInt(buf, 85447)
	fmt.Println(string(buf))
}

func AppendInt(dst []byte, u int64) (d []byte) {
	var a [64 + 1]byte // +1 for sign of 64bit value in base 2
	i := len(a)
	var neg bool = false
	if u < 0 {
		u = -u
		neg = true
	}
	us := uint(u)
	for us > 0 {
		is := us % 10
		us /= 10
		i--
		a[i] = digits[is]
	}

	if neg {
		i--
		a[i] = '-'
	}

	d = append(dst, a[i:]...)
	return
}

func TestAirqNextSql(t *testing.T) {
	start, err := time.Parse(time.RFC3339, "2021-01-01T00:00:00+08:00")
	if err != nil {
		log.Fatal(err)
	}
	end, err := time.Parse(time.RFC3339, "2021-02-01T00:00:00+08:00")
	if err != nil {
		log.Fatal(err)
	}
	cfg := &AirqSimulatorConfig{
		Start:            start,
		End:              end,
		SamplingInterval: time.Second,
		DeviceCount:      100,
		DeviceOffset:     0,
		SqlTemplates:     []string{"select mean(aqi) as aqi from city_air_quality where city in '{city*6}' and time >= '{now}'-30d and time < '{now}' group by time(1d)"},
	}
	sim := cfg.ToSimulator()
	point := common.MakeUsablePoint()
	sim.Next(point)
	point.Reset()
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	sim.SetWrittenPoints(10000)
	sim.NextSql(buf)
	fmt.Println(buf.String())
}

func BenchmarkAirqNextSql(b *testing.B) {
	now := time.Now()
	cfg := &AirqSimulatorConfig{
		Start:            now.Add(time.Second * -100),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      100,
		DeviceOffset:     0,
		SqlTemplates:     []string{"select mean(aqi) as aqi from city_air_quality where city in '{city*1000}' and time >= '{now}'-30d and time < '{now}' group by time(1d)"},
	}
	sim := cfg.ToSimulator()
	point := common.MakeUsablePoint()
	sim.Next(point)
	point.Reset()
	buf := bytes.NewBuffer(make([]byte, 0, 8*1024))
	for i := 0; i < b.N; i++ {
		sim.NextSql(buf)
		buf.Reset()
	}

	// fmt.Println(buf.String())
}
