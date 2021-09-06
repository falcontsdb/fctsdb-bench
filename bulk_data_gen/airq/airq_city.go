package airq

import (
	"math/rand"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
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

func (m *CityAirQualityMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
}

func (m *CityAirQualityMeasurement) ToPoint(p *common.Point) bool {
	p.SetMeasurementName(CityAirQualityByteString)
	// p.SetTimestamp(&m.timestamp)

	p.AppendField(CityAirQualityFieldKeys[0], rand.Intn(490)+10)
	p.AppendField(CityAirQualityFieldKeys[1], rand.Intn(490)+10)
	p.AppendField(CityAirQualityFieldKeys[2], rand.Intn(490)+10)
	p.AppendField(CityAirQualityFieldKeys[3], rand.Intn(90)+10)
	p.AppendField(CityAirQualityFieldKeys[4], rand.Intn(30)+2)
	p.AppendField(CityAirQualityFieldKeys[5], rand.Intn(200)+10)
	p.AppendField(CityAirQualityFieldKeys[6], rand.Float64()+0.5)
	p.AppendField(CityAirQualityFieldKeys[7], RandomString(20))

	return true
}

func RandomString(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits := 6                           // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}
