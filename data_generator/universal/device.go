package universal

import (
	"strconv"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

// Type Host models a machine being monitored by Telegraf.
type Device struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	// Province, City, County, SiteType, SiteID []byte
	TagKeys   [][]byte
	TagValues [][]byte
}

func NewDeviceMeasurements(fieldDefine [3]int64) []common.SimulatedMeasurement {
	sm := []common.SimulatedMeasurement{
		NewMeasurement("table_0", fieldDefine),
	}
	return sm
}

func NewDevice(id int64, tagsDefine []int64, fieldDefine [3]int64) Device {
	sm := NewDeviceMeasurements(fieldDefine)
	d := Device{
		SimulatedMeasurements: sm,
	}

	for i, valueCount := range tagsDefine {
		d.TagKeys = append(d.TagKeys, []byte("key_"+strconv.Itoa(i)))
		d.TagValues = append(d.TagValues, []byte("Tag"+strconv.Itoa(i)+"Value"+strconv.FormatInt(id%valueCount, 10)))
	}

	return d
}

func (air *Device) NumMeasurements() int {
	return len(air.SimulatedMeasurements)
}

type Measurement struct {
	name       []byte
	intFiled   [][]byte
	floatField [][]byte
	strField   [][]byte
	// rand      rand.RNG
}

func NewMeasurement(name string, fieldDefine [3]int64) *Measurement {
	m := &Measurement{name: []byte(name)}

	for j := 0; j < int(fieldDefine[0]); j++ {
		m.intFiled = append(m.intFiled, []byte("int_"+strconv.Itoa(j)))
	}
	for j := 0; j < int(fieldDefine[1]); j++ {
		m.floatField = append(m.floatField, []byte("float_"+strconv.Itoa(j)))
	}
	for j := 0; j < int(fieldDefine[2]); j++ {
		m.strField = append(m.strField, []byte("string_"+strconv.Itoa(j)))
	}

	return m
}

func (m *Measurement) Tick(d time.Duration) {
}

func (m *Measurement) ToPoint(p *common.Point) bool {
	p.SetMeasurementName(m.name)
	for _, f := range m.intFiled {
		p.AppendInt64Field(f, int64(fastrand.Uint32n(100000000)))
	}
	for _, f := range m.floatField {
		p.AppendField(f, fastrand.Float64())
	}
	for _, f := range m.strField {
		p.AppendField(f, fastrand.RandomNormalBytes(10))
	}
	return true
}
