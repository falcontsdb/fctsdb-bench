package devops

import (
	"sync/atomic"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// A DevopsSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type DevopsSimulator struct {
	madePoints int64
	madeValues int64
	maxPoints  int64

	simulatedMeasurementIndex int

	hostIndex int
	hosts     []Host

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *DevopsSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *DevopsSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *DevopsSimulator) Total() int64 {
	return g.maxPoints
}

func (g *DevopsSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints-1 //对应g.madePoints从0开始
}

// Type DevopsSimulatorConfig is used to create a DevopsSimulator.
type DevopsSimulatorConfig struct {
	Start time.Time
	End   time.Time

	HostCount  int64
	HostOffset int64
}

func (d *DevopsSimulatorConfig) ToSimulator() *DevopsSimulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = NewHost(i, int(d.HostOffset), d.Start)
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * (d.HostCount * NHostSims)
	dg := &DevopsSimulator{
		madePoints: -1, //由于atomic是先加后返回值，为了保证从0开始，需要先置为-1
		madeValues: 0,
		maxPoints:  maxPoints,

		simulatedMeasurementIndex: 0,

		hostIndex: 0,
		hosts:     hostInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

// Next advances a Point to the next state in the generator.
func (d *DevopsSimulator) Next(p *Point) {
	// // switch to the next host if needed
	// if d.simulatedMeasurementIndex == NHostSims {
	// 	d.simulatedMeasurementIndex = 0
	// 	d.hostIndex++
	// }
	// if d.hostIndex == len(d.hosts) {
	// 	d.hostIndex = 0
	// 	for i := 0; i < len(d.hosts); i++ {
	// 		d.hosts[i].TickAll(EpochDuration)
	// 	}
	// }
	// host := &d.hosts[d.hostIndex]

	madePoint := atomic.AddInt64(&d.madePoints, 1)
	hostIndex := (madePoint / NHostSims) % int64(len(d.hosts))
	host := &d.hosts[hostIndex]
	// 为了协程安全, 这里不使用TickAll方法
	timestamp := d.timestampStart.Add(EpochDuration * time.Duration(madePoint/int64(len(d.hosts))/NHostSims))
	p.SetTimestamp(&timestamp)
	if hostIndex == int64(len(d.hosts)-1) {
		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(EpochDuration)
		}
	}

	// Populate host-specific tags:
	p.AppendTag(MachineTagKeys[0], host.Name)
	p.AppendTag(MachineTagKeys[1], host.Region)
	p.AppendTag(MachineTagKeys[2], host.Datacenter)
	p.AppendTag(MachineTagKeys[3], host.Rack)
	p.AppendTag(MachineTagKeys[4], host.OS)
	p.AppendTag(MachineTagKeys[5], host.Arch)
	p.AppendTag(MachineTagKeys[6], host.Team)
	p.AppendTag(MachineTagKeys[7], host.Service)
	p.AppendTag(MachineTagKeys[8], host.ServiceVersion)
	p.AppendTag(MachineTagKeys[9], host.ServiceEnvironment)

	// // Populate measurement-specific tags and fields:
	// host.SimulatedMeasurements[d.simulatedMeasurementIndex].ToPoint(p)
	// d.madePoints++
	// d.simulatedMeasurementIndex++
	// d.madeValues += int64(len(p.FieldValues))

	host.SimulatedMeasurements[madePoint%NHostSims].ToPoint(p)
	atomic.AddInt64(&d.madeValues, int64(len(p.FieldValues)))
}
