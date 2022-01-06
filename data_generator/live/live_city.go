package live

import (
	// "math/rand"

	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	CityCastByteString = []byte("chengdu_cast") // heap optimization
)

var (
	// Field keys for 'airq CityAirQuality' points.
	CityCast = [][]byte{
		[]byte("water"),
		[]byte("power"),
		[]byte("gas"),
	}
)

type CityCastMeasurement struct {
	timestamp time.Time
	water     int64
	power     int64
	gas       int64
	// rand      rand.RNG
}

func NewCityAirQualityMeasurement(start time.Time) *CityCastMeasurement {
	return &CityCastMeasurement{
		timestamp: start,
	}
}

func (m *CityCastMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
}

func (m *CityCastMeasurement) ToPoint(p *common.Point) bool {
	p.SetMeasurementName(CityCastByteString)
	randNum := fastrand.Uint32() //一个64位随机数可以通过掩码的形式生成其他数字，减少随机数的生成，9+9+9+7+6+8+10 = 58

	// aqi占9位，随机范围即10-522，以此类推
	m.water += int64(randNum&uint32(1<<4-1) + 5)
	p.AppendField(CityCast[0], m.water)
	randNum >>= 4
	m.power += int64(randNum&uint32(1<<4-1) + 50)
	p.AppendField(CityCast[1], m.power)
	randNum >>= 4
	m.gas += int64(randNum & uint32(1<<1-1))
	p.AppendField(CityCast[2], m.gas)
	randNum >>= 1
	return true
}

func (m *CityCastMeasurement) RandomString(n int) []byte {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits := 6                            // 6 bits to represent a letter index
	letterIdxMask := uint32(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 32 / letterIdxBits            // # of letter indices fitting in 63 bits

	// sb := strings.Builder{}
	// sb.Grow(n)
	buf := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, fastrand.Uint32(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = fastrand.Uint32(), letterIdxMax
		}
		idx := int(cache&letterIdxMask) % len(letterBytes)
		buf[i] = letterBytes[idx]
		i--

		cache >>= letterIdxBits
		remain--
	}

	// return *(*string)(unsafe.Pointer(&buf))
	return buf
}
