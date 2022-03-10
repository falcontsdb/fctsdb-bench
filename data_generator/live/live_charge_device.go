package live

import (
	"fmt"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/gbt2260"
)

var (
	Region   = gbt2260.NewGBT2260()
	AreaCode = Region.GetAreaCodeByCity("510100")
)

var (
	ChargeTagKeys = [][]byte{
		[]byte("province"),
		[]byte("city"),
		[]byte("county"),
		[]byte("site_id"),
	}
)

// Type Host models a machine being monitored by Telegraf.
type ChargeDevice struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	// Province, City, County, SiteType, SiteID []byte
	TagValues [][]byte
}

func NewChargeDeviceMeasurements(start time.Time) []common.SimulatedMeasurement {
	sm := []common.SimulatedMeasurement{
		NewCityAirQualityMeasurement(start),
	}
	return sm
}

func NewChargeDevice(i int, offset int, start time.Time) ChargeDevice {
	sm := NewChargeDeviceMeasurements(start)
	region := Region.SearchGBT2260(string(common.RandChoice(AreaCode)))
	tagValues := make([][]byte, len(ChargeTagKeys))
	tagValues[0] = []byte(region[0])
	tagValues[1] = []byte(region[1])
	tagValues[2] = []byte(region[2])
	tagValues[3] = []byte(fmt.Sprintf("DEV%09d", i+offset))
	h := ChargeDevice{
		TagValues:             tagValues,
		SimulatedMeasurements: sm,
	}

	return h
}

func (d *ChargeDevice) NumMeasurements() int {
	return len(d.SimulatedMeasurements)
}
