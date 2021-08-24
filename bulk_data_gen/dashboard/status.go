package dashboard

import (
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

var (
	StatusByteString = []byte("status") // heap optimization
	// Field keys for 'air condition indoor' points.
	ServiceUpFieldKey = []byte("service_up")
)

type StatusMeasurement struct {
	timestamp time.Time
	serviceUp Distribution
}

func NewStatusMeasurement(start time.Time) *StatusMeasurement {
	//state
	serviceUp := TSD(0, 1, 0)

	return &StatusMeasurement{
		timestamp: start,
		serviceUp: serviceUp,
	}
}

func (m *StatusMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	m.serviceUp.Advance()
}

func (m *StatusMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(StatusByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendField(ServiceUpFieldKey, int(m.serviceUp.Get()))
	return true
}
