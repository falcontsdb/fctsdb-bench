package airq

import (
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// Type AirqSimulatorConfig is used to create a AirqSimulator.

type AirqSimulatorConfig struct {
	Start            time.Time
	End              time.Time
	SamplingInterval time.Duration
	AirqDeviceCount  int64
	AirqDeviceOffset int64
}

func (d *AirqSimulatorConfig) ToSimulator() *AirqSimulator {
	AirqDevices := make([]AirqDevice, d.AirqDeviceCount)
	var measNum int64

	for i := 0; i < len(AirqDevices); i++ {
		AirqDevices[i] = NewAirqDevice(i, int(d.AirqDeviceOffset), d.Start)
		measNum += int64(AirqDevices[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / d.SamplingInterval.Nanoseconds()
	maxPoints := epochs * measNum
	dg := &AirqSimulator{
		madePoints: -1, //保证madePoint在next方法中被使用时的初始值是0
		madeValues: 0,
		maxPoints:  maxPoints,

		currentHostIndex: 0,
		Hosts:            AirqDevices,

		// timestampNow:   d.Start,
		SamplingInterval: d.SamplingInterval,
		TimestampStart:   d.Start,
		TimestampEnd:     d.End,
	}

	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type AirqSimulator struct {
	madePoints       int64
	maxPoints        int64
	madeValues       int64
	currentHostIndex int

	Hosts            []AirqDevice
	SamplingInterval time.Duration
	TimestampStart   time.Time
	TimestampEnd     time.Time
}

func (s *AirqSimulator) SeenPoints() int64 {
	return s.madePoints
}

func (s *AirqSimulator) SeenValues() int64 {
	return s.madeValues
}

func (s *AirqSimulator) Total() int64 {
	return s.maxPoints
}

func (s *AirqSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints-1
}

// Next advances a Point to the next state in the generator.
func (s *AirqSimulator) Next(p *common.Point) bool {

	// switch to the next metric if needed
	madePoint := atomic.AddInt64(&s.madePoints, 1)
	hostIndex := madePoint % int64(len(s.Hosts))

	Airq := &s.Hosts[hostIndex]
	// vehicle.SimulatedMeasurements[0].Tick(v.SamplingInterval)
	// 为了协程安全, 且由于这里只有一张表，这里不使用Tick方法
	timestamp := s.TimestampStart.Add(s.SamplingInterval * time.Duration(madePoint/int64(len(s.Hosts))))
	p.SetTimestamp(&timestamp)

	// Populate host-specific tags: for example, LSVNV2182E2100001
	p.AppendTag(AirqTagKeys[0], Airq.Province)
	p.AppendTag(AirqTagKeys[1], Airq.City)
	p.AppendTag(AirqTagKeys[2], Airq.County)
	p.AppendTag(AirqTagKeys[3], Airq.SiteType)
	p.AppendTag(AirqTagKeys[4], Airq.SiteID)

	// Populate measurement-specific tags and fields:
	Airq.SimulatedMeasurements[0].ToPoint(p)

	atomic.AddInt64(&s.madeValues, int64(len(p.FieldValues)))
	return madePoint < s.maxPoints //方便另一只线程安全的结束方式，for sim.next(point){...} 保证产生的总点数正确，注意最后一次{...}里面的代码不执行
}
