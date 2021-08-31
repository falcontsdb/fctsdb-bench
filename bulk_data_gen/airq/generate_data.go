package airq

import (
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// Type AirqSimulatorConfig is used to create a AirqSimulator.

type AirqSimulatorConfig struct {
	Start time.Time
	End   time.Time

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

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * measNum
	dg := &AirqSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  maxPoints,

		currentAirqIndex: 0,
		Airqs:            AirqDevices,

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
	Airqs            []AirqDevice

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
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
	return s.madePoints >= s.maxPoints
}

// Next advances a Point to the next state in the generator.
func (s *AirqSimulator) Next(p *Point) {
	// switch to the next metric if needed
	if s.currentAirqIndex == len(s.Airqs) {
		s.currentAirqIndex = 0
		s.simulatedMeasurementIndex++
	}

	if s.simulatedMeasurementIndex == NAirqSims {
		s.simulatedMeasurementIndex = 0

		for i := 0; i < len(s.Airqs); i++ {
			s.Airqs[i].TickAll(EpochDuration)
		}
	}

	Airq := &s.Airqs[s.currentAirqIndex]

	// Populate host-specific tags: for example, LSVNV2182E2100001
	p.AppendTag(AirqTagKeys[0], Airq.Province)
	p.AppendTag(AirqTagKeys[1], Airq.City)
	p.AppendTag(AirqTagKeys[2], Airq.County)
	p.AppendTag(AirqTagKeys[3], Airq.SiteType)
	p.AppendTag(AirqTagKeys[4], Airq.SiteID)

	// Populate measurement-specific tags and fields:
	Airq.SimulatedMeasurements[s.simulatedMeasurementIndex].ToPoint(p)

	s.madePoints++
	s.currentAirqIndex++
	s.madeValues += int64(len(p.FieldValues))
}
