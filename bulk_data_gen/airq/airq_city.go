package airq

import (
	// "math/rand"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	rand "git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
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
	// rand      rand.RNG
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
	randNum := rand.Int63() //一个64位随机数可以通过掩码的形式，9+9+9+7+6+8+10 = 58
	p.AppendField(CityAirQualityFieldKeys[0], randNum&int64(1<<9-1)+10)
	randNum >>= 9
	p.AppendField(CityAirQualityFieldKeys[1], randNum&int64(1<<9-1)+10)
	randNum >>= 9
	p.AppendField(CityAirQualityFieldKeys[2], randNum&int64(1<<9-1)+10)
	randNum >>= 9
	p.AppendField(CityAirQualityFieldKeys[3], randNum&int64(1<<7-1)+10)
	randNum >>= 7
	p.AppendField(CityAirQualityFieldKeys[4], randNum&int64(1<<6-1)+2)
	randNum >>= 6
	p.AppendField(CityAirQualityFieldKeys[5], randNum&int64(1<<8-1)+10)
	randNum >>= 8
	p.AppendField(CityAirQualityFieldKeys[6], float32(randNum&int64(1<<10-1))/1000.0+0.5)
	// p.AppendField(CityAirQualityFieldKeys[6], rand.Float32()+0.5)
	p.AppendField(CityAirQualityFieldKeys[7], m.RandomString(20))

	return true
}

func (m *CityAirQualityMeasurement) RandomString(n int) []byte {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits := 6                           // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	// sb := strings.Builder{}
	// sb.Grow(n)
	buf := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			// sb.WriteByte(letterBytes[idx])
			buf[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	// return sb.String()
	// return *(*string)(unsafe.Pointer(&buf))
	return buf
}
