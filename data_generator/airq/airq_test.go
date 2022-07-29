package airq

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/serializers"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/gbt2260"
)

func TestRegion(t *testing.T) {
	// reg := gbt2260.NewGBT2260()
	// fmt.Println(reg.SearchGBT2260("310112"))
	// count := 0
	// gbt2260Table := gbt2260.GetGbt2260Table()
	// for _, cell := range gbt2260Table {
	// 	code := cell[0]
	// 	if code[len(code)-2:] == "00" {
	// 		if code[len(code)-4:] != "0000" {
	// 			fmt.Println(cell[0], cell[1])
	// 			count += 1
	// 		}
	// 	}
	Region := gbt2260.NewGBT2260()
	AreaCode := Region.GetAreaCodeByCity("510100")
	fmt.Println(AreaCode)
	// fmt.Println(count)
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
	out := make([]byte, 0, 4*1024)
	ser := serializers.NewSerializerInflux()
	for i := 0; i < 10; i++ {
		sim.Next(point)
		out = ser.SerializePoint(out, point)
		fmt.Println(string(out))
		out = out[:0]
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
	out := make([]byte, 0, 4*1024)
	ser := serializers.NewSerializerInflux()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializePoint(out, point)
		point.Reset()
		out = out[:0]
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
	out := make([]byte, 0, 4*1024)
	// for i := 0; i < 10; i++ {
	i := 0

	// for sim.Next(point) {
	for !sim.Finished() {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializePoint(out, point)
		point.Reset()
		out = out[:0]
		i += 1
	}
	fmt.Println(i)
	fmt.Println(sim.SeenPoints())
}

func TestNewPointDevposEasy(t *testing.T) {

	now := time.Now()

	cfg := &devops.DevopsSimulatorConfig{
		Start: now.Add(time.Second * -10),
		End:   now,
		// SamplingInterval: time.Second,
		HostCount:  1,
		HostOffset: 0,
	}
	devops.EpochDuration = time.Second
	sim := cfg.ToSimulator()
	ser := serializers.NewSerializerInflux()
	point := common.MakeUsablePoint()
	out := make([]byte, 0, 4*1024)
	// for i := 0; i < 10; i++ {
	i := 0

	// for sim.Next(point) {
	for !sim.Finished() {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializePoint(out, point)
		out = out[:0]
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
	out := make([]byte, 0, 4*1024)
	for i := 0; i < 20; i++ {
		// host := sim.Hosts[i%len(sim.Hosts)]
		// _ = string(host.SiteID)
		sim.Next(point)
		out = ser.SerializePoint(out, point)
		out = out[:0]
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

func TestTimeStamp(t *testing.T) {
	timestampStart, err := time.Parse(time.RFC3339, "2018-01-01T00:10:00Z")
	if err != nil {
		log.Fatalln("parse start error: ", err)
	}
	timestampStart = timestampStart.UTC()

	nowTime := timestampStart.Add(time.Minute)

	fmt.Println(nowTime.UTC().UnixNano())
}
