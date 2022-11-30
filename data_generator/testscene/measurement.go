package testscene

import (
	// "math/rand"

	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	MeasurementName = []byte("t_neimeng_label_name_v1") // heap optimization
)

var (
	// Field keys for 'airq CityAirQuality' points.
	FieldKeys = [][]byte{
		[]byte("f_describe"),
		[]byte("f_max"),
		[]byte("f_min"),
	}
)

type Measurement struct {
	timestamp time.Time
}

func NewMeasurement(start time.Time) *Measurement {
	return &Measurement{
		timestamp: start,
	}
}

func (m *Measurement) ToPoint(p *common.Point) bool {
	p.SetMeasurementName(MeasurementName)
	randNum := fastrand.Uint64() //一个64位随机数可以通过掩码的形式生成其他数字，减少随机数的生成，9+9+9+7+6+8+10 = 58

	// aqi占9位，随机范围即10-522，以此类推
	p.AppendField(FieldKeys[0], fastrand.RandomNormalBytes(20))
	randNum >>= 9
	p.AppendField(FieldKeys[1], int64(randNum&uint64(1<<9-1)+10))
	randNum >>= 9
	p.AppendField(FieldKeys[2], int64(randNum&uint64(1<<9-1)+10))
	randNum >>= 9
	return true
}
