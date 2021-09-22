package vehicle

import (
	"log"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// Type IotSimulatorConfig is used to create a IotSimulator.
type VehicleSimulatorConfig struct {
	Start            time.Time
	End              time.Time
	SamplingInterval time.Duration
	VehicleCount     int64
	VehicleOffset    int64
}

func (d *VehicleSimulatorConfig) ToSimulator() *VehicleSimulator {
	if d.VehicleCount <= 0 {
		log.Fatal("the vehicle count is unavailable")
	}
	vehicleInfos := make([]Vehicle, d.VehicleCount)
	var measNum int64

	for i := 0; i < len(vehicleInfos); i++ {
		vehicleInfos[i] = NewVehicle(i, int(d.VehicleOffset), d.Start)
		measNum += int64(vehicleInfos[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / d.SamplingInterval.Nanoseconds()
	maxPoints := epochs * measNum
	dg := &VehicleSimulator{
		madePoints: -1, //保证madePoint在next方法中被使用时的初始值是0
		madeValues: 0,
		maxPoints:  maxPoints,

		// currentHostIndex: 0,
		Hosts:            vehicleInfos,
		SamplingInterval: d.SamplingInterval,
		TimestampStart:   d.Start,
		TimestampEnd:     d.End,
	}

	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type VehicleSimulator struct {
	madePoints int64
	maxPoints  int64
	madeValues int64
	// simulatedMeasurementIndex int
	// currentHostIndex int

	Hosts            []Vehicle
	SamplingInterval time.Duration
	TimestampStart   time.Time
	TimestampEnd     time.Time
}

func (g *VehicleSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *VehicleSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *VehicleSimulator) Total() int64 {
	return g.maxPoints
}

func (g *VehicleSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints-1
}

// Next advances a Point to the next state in the generator.
func (v *VehicleSimulator) Next(p *common.Point) bool {
	// switch to the next metric if needed
	madePoint := atomic.AddInt64(&v.madePoints, 1)
	hostIndex := madePoint % int64(len(v.Hosts))

	vehicle := &v.Hosts[hostIndex]
	// vehicle.SimulatedMeasurements[0].Tick(v.SamplingInterval)
	// 为了协程安全，这里不使用Tick方法
	timestamp := v.TimestampStart.Add(v.SamplingInterval * time.Duration(madePoint/int64(len(v.Hosts))))
	p.SetTimestamp(&timestamp)

	// Populate host-specific tags: for example, LSVNV2182E2100001
	p.AppendTag([]byte("VIN"), vehicle.Name)

	// Populate measurement-specific tags and fields:
	vehicle.SimulatedMeasurements[0].ToPoint(p)

	// v.madePoints++
	// v.madeValues += int64(len(p.FieldValues))
	atomic.AddInt64(&v.madeValues, int64(len(p.FieldValues)))
	return madePoint < v.maxPoints //方便另一只线程安全的结束方式，for sim.next(point){...} 保证产生的总点数正确，注意最后一次{...}里面的代码不执行
}
