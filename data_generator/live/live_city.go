package live

import (
	// "math/rand"

	"sync/atomic"
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
	counter   int64
	waterDay  float64
	powerDay  float64
	gasDay    float64
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
	// randNum := fastrand.Uint32() //一个64位随机数可以通过掩码的形式生成其他数字，减少随机数的生成，9+9+9+7+6+8+10 = 58
	atomic.AddInt64(&m.counter, 1)

	// aqi占9位，随机范围即10-522，以此类推
	// p.AppendField(CityCast[0], atomic.AddInt64(&m.water, int64(float64(randNum&uint32(1<<10-1)+2)*m.waterDay/1000)))
	// randNum >>= 10
	// p.AppendField(CityCast[1], atomic.AddInt64(&m.power, int64(float64(randNum&uint32(1<<10-1)+10)*m.powerDay/1000)))
	// randNum >>= 10
	// p.AppendField(CityCast[2], atomic.AddInt64(&m.gas, int64(float64(randNum&uint32(1<<10-1))*m.gasDay/1000)))
	// randNum >>= 1

	if m.counter%6 == 0 {
		m.waterDay = float64(fastrand.Uint32n(300) + 1)
		m.gasDay = float64(fastrand.Uint32n(200) + 1)
		m.powerDay = float64(fastrand.Uint32n(100) + 1)
		// fmt.Println(m.bigNum)
		// m.bigNum = 0.1
	}
	p.AppendField(CityCast[0], atomic.AddInt64(&m.water, int64(m.waterDay)/10))
	p.AppendField(CityCast[1], atomic.AddInt64(&m.power, int64(m.powerDay)/10))
	p.AppendField(CityCast[2], atomic.AddInt64(&m.gas, int64(m.gasDay)/10))

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
