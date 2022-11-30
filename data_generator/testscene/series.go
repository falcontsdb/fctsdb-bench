package testscene

import (
	"fmt"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

// Count of choices for auto-generated tag values:
const (
	MachineRackChoicesPerDatacenter = 100
	MachineServiceChoices           = 20
	MachineServiceVersionChoices    = 2
)

var (
	TagKeys = [][]byte{
		[]byte("f_lab_name"),
		[]byte("f_point_id"),
	}
)

// Type Host models a machine being monitored by Telegraf.
type Series struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	// Province, City, County, SiteType, SiteID []byte
	TagValues [][]byte
}

func NewMeasurements(start time.Time) []common.SimulatedMeasurement {
	sm := []common.SimulatedMeasurement{
		NewMeasurement(start),
	}
	return sm
}

func NewSeries(i int, offset int, start time.Time) Series {
	sm := NewMeasurements(start)
	tagValues := make([][]byte, len(TagKeys))
	tagValues[0] = fastrand.RandomNormalBytes(20)
	tagValues[1] = []byte(fmt.Sprintf("DEV%09d", i+offset))
	h := Series{
		TagValues:             tagValues,
		SimulatedMeasurements: sm,
	}
	return h
}

func (s *Series) NumMeasurements() int {
	return len(s.SimulatedMeasurements)
}
