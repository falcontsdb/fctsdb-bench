package airq

import (
	// "math/rand"

	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	CityAirQualityByteString = []byte("city_air_quality") // heap optimization
)

var (
	// Field keys for 'airq CityAirQuality' points.
	CityAirQualityFieldKeys = [][]byte{
		[]byte("aqi"),
		[]byte("pm10"),
		[]byte("pm25"),
		[]byte("no2"),
		[]byte("so2"),
		[]byte("o3"),
		[]byte("co"),
		[]byte("tips"),
	}
)

type CityAirQualityMeasurement struct {
	timestamp time.Time
}

func NewCityAirQualityMeasurement(start time.Time) *CityAirQualityMeasurement {
	return &CityAirQualityMeasurement{
		timestamp: start,
	}
}

func (m *CityAirQualityMeasurement) ToPoint(p *common.Point) bool {
	p.SetMeasurementName(CityAirQualityByteString)
	randNum := fastrand.Uint64() //一个64位随机数可以通过掩码的形式生成其他数字，减少随机数的生成，9+9+9+7+6+8+10 = 58

	// aqi占9位，随机范围即10-522，以此类推
	p.AppendField(CityAirQualityFieldKeys[0], int64(randNum&uint64(1<<9-1)+10))
	randNum >>= 9
	p.AppendField(CityAirQualityFieldKeys[1], int64(randNum&uint64(1<<9-1)+10))
	randNum >>= 9
	p.AppendField(CityAirQualityFieldKeys[2], int64(randNum&uint64(1<<9-1)+10))
	randNum >>= 9
	p.AppendField(CityAirQualityFieldKeys[3], int64(randNum&uint64(1<<7-1)+10))
	randNum >>= 7
	p.AppendField(CityAirQualityFieldKeys[4], int64(randNum&uint64(1<<6-1)+2))
	randNum >>= 6
	p.AppendField(CityAirQualityFieldKeys[5], int64(randNum&uint64(1<<8-1)+10))
	randNum >>= 8
	p.AppendField(CityAirQualityFieldKeys[6], float32(randNum&uint64(1<<10-1))/1000.0+0.5)
	p.AppendField(CityAirQualityFieldKeys[7], fastrand.RandomNormalBytes(20))
	return true
}
