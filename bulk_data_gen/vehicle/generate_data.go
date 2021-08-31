package vehicle

import (
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

// Type IotSimulatorConfig is used to create a IotSimulator.
type VehicleSimulatorConfig struct {
	Start time.Time
	End   time.Time

	VehicleCount  int64
	VehicleOffset int64
}

func (d *VehicleSimulatorConfig) ToSimulator() *VehicleSimulator {
	vehicleInfos := make([]Vehicle, d.VehicleCount)
	var measNum int64

	for i := 0; i < len(vehicleInfos); i++ {

		vehicleInfos[i] = NewVehicle(i, int(d.VehicleOffset), d.Start)
		measNum += int64(vehicleInfos[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * measNum
	dg := &VehicleSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  maxPoints,

		currentVehicleIndex: 0,
		Vehicles:            vehicleInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type VehicleSimulator struct {
	madePoints int64
	maxPoints  int64
	madeValues int64

	simulatedMeasurementIndex int

	currentVehicleIndex int
	Vehicles            []Vehicle

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *VehicleSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *VehicleSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *VehicleSimulator) Total() int64 {
	return g.maxPoints
}

func (g *VehicleSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (v *VehicleSimulator) Next(p *Point) {
	// switch to the next metric if needed
	if v.currentVehicleIndex == len(v.Vehicles) {
		v.currentVehicleIndex = 0
		v.simulatedMeasurementIndex++
	}

	if v.simulatedMeasurementIndex == NVehicleSims {
		v.simulatedMeasurementIndex = 0

		for i := 0; i < len(v.Vehicles); i++ {
			v.Vehicles[i].TickAll(EpochDuration)
		}
	}

	vehicle := &v.Vehicles[v.currentVehicleIndex]

	// Populate host-specific tags: for example, LSVNV2182E2100001
	p.AppendTag([]byte("VIN"), vehicle.Name)

	// Populate measurement-specific tags and fields:
	vehicle.SimulatedMeasurements[v.simulatedMeasurementIndex].ToPoint(p)

	v.madePoints++
	v.currentVehicleIndex++
	v.madeValues += int64(len(p.FieldValues))
}
