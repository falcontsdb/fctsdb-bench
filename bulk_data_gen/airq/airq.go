package airq

import (
	"fmt"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

var (
	// The duration of a log epoch.
	EpochDuration = 1 * time.Second

	// Tag fields common to all inside sensors:
	AirqKey = []byte("room_id")

	// Tag fields common to all inside sensors:
	AirqTagKeys = [][]byte{
		[]byte("sensor_id"),
		[]byte("home_id"),
		[]byte(""),
	}

	DefaultAirqDateTimeStart = "2018-01-01T00:00:00Z"
	DefaultAirqDateTimeEnd   = "2018-01-01T00:00:01Z"
)

// Mark 表的数量
const NAirqSims = 1

// Type Host models a machine being monitored by Telegraf.
type Airq struct {
	SimulatedMeasurements []SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Name []byte
}

func NewHostMeasurements(start time.Time) []SimulatedMeasurement {
	sm := []SimulatedMeasurement{
		NewEntityMeasurement(start),
	}

	if len(sm) != NAirqSims {
		panic("logic error: incorrect number of measurements")
	}
	return sm
}

func NewAirq(i int, offset int, start time.Time) Airq {
	sm := NewHostMeasurements(start)

	h := Airq{
		// Tag Values that are static throughout the life of a Host:
		Name:                  []byte(fmt.Sprintf("Airq_%d", i+offset)),
		SimulatedMeasurements: sm,
	}

	return h
}

// TickAll advances all Distributions of a Host.
func (v *Airq) TickAll(d time.Duration) {
	for i := range v.SimulatedMeasurements {
		v.SimulatedMeasurements[i].Tick(d)
	}
}

func (v *Airq) NumMeasurements() int {
	return len(v.SimulatedMeasurements)
}
