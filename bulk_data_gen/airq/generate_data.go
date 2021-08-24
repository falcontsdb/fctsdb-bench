package airq

import (
	"fmt"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// Type IotSimulatorConfig is used to create a IotSimulator.
type AirqSimulatorConfig struct {
	Start time.Time
	End   time.Time

	AirqCount  int64
	AirqOffset int64
}

func (d *AirqSimulatorConfig) ToSimulator() *AirqSimulator {
	AirqInfos := make([]Airq, d.AirqCount)
	var measNum int64

	for i := 0; i < len(AirqInfos); i++ {
		AirqInfos[i] = NewAirq(i, int(d.AirqOffset), d.Start)
		measNum += int64(AirqInfos[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * measNum
	dg := &AirqSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  maxPoints,

		currentAirqIndex: 0,
		Airqs:            AirqInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type AirqSimulator struct {
	madePoints int64
	maxPoints  int64
	madeValues int64

	simulatedMeasurementIndex int

	currentAirqIndex int
	Airqs            []Airq

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *AirqSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *AirqSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *AirqSimulator) Total() int64 {
	return g.maxPoints
}

func (g *AirqSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (v *AirqSimulator) Next(p *Point) {
	// switch to the next metric if needed
	if v.currentAirqIndex == len(v.Airqs) {
		v.currentAirqIndex = 0
		v.simulatedMeasurementIndex++
	}

	if v.simulatedMeasurementIndex == NAirqSims {
		v.simulatedMeasurementIndex = 0

		for i := 0; i < len(v.Airqs); i++ {
			v.Airqs[i].TickAll(EpochDuration)
		}
	}

	Airq := &v.Airqs[v.currentAirqIndex]

	// Populate host-specific tags: for example, LSVNV2182E2100001
	vin := fmt.Sprintf("LSVNV2182E2%d", v.currentAirqIndex)
	p.AppendTag([]byte("VIN"), []byte(vin))
	// p.AppendTag(MachineTagKeys[1], host.Region)
	//p.AppendTag(MachineTagKeys[2], host.Datacenter)
	//p.AppendTag(MachineTagKeys[3], host.Rack)
	//p.AppendTag(MachineTagKeys[4], host.OS)
	//p.AppendTag(MachineTagKeys[5], host.Arch)
	//p.AppendTag(MachineTagKeys[6], host.Team)
	//p.AppendTag(MachineTagKeys[7], host.Service)
	//p.AppendTag(MachineTagKeys[8], host.ServiceVersion)
	//p.AppendTag(MachineTagKeys[9], host.ServiceEnvironment)

	// Populate measurement-specific tags and fields:
	Airq.SimulatedMeasurements[v.simulatedMeasurementIndex].ToPoint(p)

	v.madePoints++
	v.currentAirqIndex++
	v.madeValues += int64(len(p.FieldValues))

	return
}
