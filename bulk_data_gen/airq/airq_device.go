package airq

import (
	"fmt"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq/gbt2260"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// Count of choices for auto-generated tag values:
const (
	MachineRackChoicesPerDatacenter = 100
	MachineServiceChoices           = 20
	MachineServiceVersionChoices    = 2
)

var (
	Region   = gbt2260.NewGBT2260()
	AreaCode = Region.GetAllAreaCode()
)

var (
	SiteTypeChoices = [][]byte{
		[]byte("国控站"),
		[]byte("省控点"),
		[]byte("市控点"),
	}
	AirqTagKeys = [][]byte{
		[]byte("province"),
		[]byte("city"),
		[]byte("county"),
		[]byte("site_type"),
		[]byte("site_id"),
	}
)

// Type Host models a machine being monitored by Telegraf.
type AirqDevice struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Province, City, County, SiteType, SiteID []byte
}

func NewAirqDeviceMeasurements(start time.Time) []common.SimulatedMeasurement {
	sm := []common.SimulatedMeasurement{
		NewCityAirQualityMeasurement(start),
	}
	return sm
}

func NewAirqDevice(i int, offset int, start time.Time) AirqDevice {
	sm := NewAirqDeviceMeasurements(start)
	region := Region.SearchGBT2260(string(common.RandChoice(AreaCode)))
	h := AirqDevice{
		Province:              []byte(region[0]),
		City:                  []byte(region[1]),
		County:                []byte(region[2]),
		SiteType:              common.RandChoice(SiteTypeChoices),
		SiteID:                []byte(fmt.Sprintf("DEV%09d", i+offset)),
		SimulatedMeasurements: sm,
	}

	return h
}

func (air *AirqDevice) NumMeasurements() int {
	return len(air.SimulatedMeasurements)
}

// TickAll advances all Distributions of a Host.
func (air *AirqDevice) TickAll(d time.Duration) {
	for i := range air.SimulatedMeasurements {
		air.SimulatedMeasurements[i].Tick(d)
	}
}
