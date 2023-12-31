package common

import (
	"io"
)

const (
	DefaultDateTimeStart        = "2018-01-01T00:00:00Z"
	DefaultDateTimeEnd          = "2018-01-02T00:00:00Z"
	UseCaseDevOps               = "devops"
	UseCaseIot                  = "iot"
	UseCaseDashboard            = "dashboard"
	UseCaseMetaquery            = "metaquery"
	UseCaseWindowAggregate      = "window-agg"
	UseCaseGroupAggregate       = "group-agg"
	UseCaseBareAggregate        = "bare-agg"
	UseCaseGroupWindowTranspose = "group-window-transpose"
	UseCaseVehicle              = "vehicle"
	UseCaseAirQuality           = "air-quality"
	UseCaseLiveCharge           = "live"
	UseCaseScene                = "scene"
)

// Use case choices:
var UseCaseChoices = []string{
	UseCaseDevOps,
	UseCaseIot,
	UseCaseDashboard,
	UseCaseMetaquery,
	UseCaseWindowAggregate,
	UseCaseGroupAggregate,
	UseCaseBareAggregate,
	UseCaseVehicle,
	UseCaseAirQuality,
}

// Simulator simulates a use case.
type Simulator interface {
	Total() int64
	SeenPoints() int64
	SeenValues() int64
	Finished() bool
	Next(*Point) int64
	NextSql(io.Writer) int64
	SetWrittenPoints(int64)
	SetSqlTemplate([]string) error
	ClearMadePointNum()
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	ToPoint(*Point) bool //returns true if point if properly filled, false means, that point should be skipped
}

// MakeUsablePoint allocates a new Point ready for use by a Simulator.

type MeasurementInfo struct {
	MeasurementName []byte
	TagKeys         [][]byte
	FieldKeys       [][]byte
	FieldType       []string
}
