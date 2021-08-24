package airq

import (
	"fmt"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"github.com/ppmoon/gbt2260/v2"
)

const NHostSims = 9

// Count of choices for auto-generated tag values:
const (
	MachineRackChoicesPerDatacenter = 100
	MachineServiceChoices           = 20
	MachineServiceVersionChoices    = 2
)

var (
	Region = gbt2260.NewGBT2260()
)

var (
	SiteTypeChoices = [][]byte{
		[]byte("国控站"),
		[]byte("省控点"),
		[]byte("市控点"),
	}
)

// Type Host models a machine being monitored by Telegraf.
type Device struct {
	SimulatedMeasurements []SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Province, City, County, SiteType, SiteID []byte
}

func NewDeviceMeasurements(start time.Time) []SimulatedMeasurement {
	sm := []SimulatedMeasurement{
		// NewCPUMeasurement(start),
	}

	if len(sm) != NHostSims {
		panic("logic error: incorrect number of measurements")
	}
	return sm
}

func NewDevice(i int, offset int, start time.Time) Device {
	sm := NewHostMeasurements(start)

	region := Region.GetAllProvince()
	// rackId := rand.Int63n(MachineRackChoicesPerDatacenter)

	h := Device{
		// Tag Values that are static throughout the life of a Host:
		Province: []byte(fmt.Sprintf("host_%d", i+offset)),
		City:     []byte(fmt.Sprintf("%s", region.Name)),
		County:   RandChoice(region.Datacenters),
		SiteType: RandChoice(SiteTypeChoices),
		SiteID:   []byte(fmt.Sprintf("Dev_%07d", i+offset)),

		SimulatedMeasurements: sm,
	}

	return h
}

// TickAll advances all Distributions of a Host.
func (h *Host) TickAll(d time.Duration) {
	for i := range h.SimulatedMeasurements {
		h.SimulatedMeasurements[i].Tick(d)
	}
}
