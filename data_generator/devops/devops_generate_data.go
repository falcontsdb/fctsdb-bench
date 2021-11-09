package devops

import (
	"io"
	"sync/atomic"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/common"
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
	return g.madePoints >= g.maxPoints
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
		madePoints: 0,
		madeValues: 0,
		maxPoints:  maxPoints,

		simulatedMeasurementIndex: 0,
		hostIndex:                 0,
		hosts:                     hostInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

// Next advances a Point to the next state in the generator.
func (d *DevopsSimulator) Next(p *Point) int64 {
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
	poindIndex := madePoint - 1 //由于atomic是先加后返回值，为了保证next中方法从0开始，需要先置为-1
	hostIndex := (poindIndex / NHostSims) % int64(len(d.hosts))
	host := &d.hosts[hostIndex]
	// 为了多协程timestamp不混乱, 这里不使用TickAll方法
	// madePoint 增加int64(len(d.hosts))*NHostSims次，timestamp增加一次，保证每张表里面两条数据之间的间隔为 EpochDuration
	timestamp := d.timestampStart.Add(EpochDuration * time.Duration(poindIndex/int64(len(d.hosts))/NHostSims))
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
	return madePoint //方便另一种线程安全的结束方式，for sim.next(point) <= sim.total(){...} 保证产生的总点数正确，注意最后一次{...}里面的代码不执行
}

func (d *DevopsSimulator) NextSql(wr io.Writer) int64 {
	return 0
}
func (g *DevopsSimulator) SetWrittenPoints(num int64) {
}
func (g *DevopsSimulator) SetSqlTemplate(sqlTemplates []string) error {
	return nil
}
