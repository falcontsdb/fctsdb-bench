package vehicle

import (
	"fmt"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

var (

	// Tag fields common to all inside sensors:
	RoomTagKey = []byte("room_id")

	// Tag fields common to all inside sensors:
	SensorHomeTagKeys = [][]byte{
		[]byte("sensor_id"),
		[]byte("home_id"),
	}

	DefaultVehicleDateTimeStart = "2018-01-01T00:00:00Z"
	DefaultVehicleDateTimeEnd   = "2018-01-01T00:00:01Z"
)

// Mark 表的数量
const NVehicleSims = 1

// Type Host models a machine being monitored by Telegraf.
type Vehicle struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Name []byte
}

func NewHostMeasurements(start time.Time) []common.SimulatedMeasurement {
	sm := []common.SimulatedMeasurement{
		NewEntityMeasurement(start),
	}

	if len(sm) != NVehicleSims {
		panic("logic error: incorrect number of measurements")
	}
	return sm
}

func NewVehicle(i int, offset int, start time.Time) Vehicle {
	sm := NewHostMeasurements(start)

	h := Vehicle{
		// Tag Values that are static throughout the life of a Host:
		Name:                  []byte(fmt.Sprintf("LSVNV2182E%09d", i+offset)),
		SimulatedMeasurements: sm,
	}

	return h
}

// TickAll advances all Distributions of a Host.
func (v *Vehicle) TickAll(d time.Duration) {
	for i := range v.SimulatedMeasurements {
		v.SimulatedMeasurements[i].Tick(d)
	}
}

func (v *Vehicle) NumMeasurements() int {
	return len(v.SimulatedMeasurements)
}
