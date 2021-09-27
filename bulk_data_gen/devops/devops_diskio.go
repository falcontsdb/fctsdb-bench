package devops

import (
	"fmt"
	// "math/rand"
	"sync/atomic"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	rand "git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	DiskIOByteString = []byte("diskio") // heap optimization
	SerialByteString = []byte("serial")

	DiskIOFields = []LabeledDistributionMaker{
		{[]byte("reads"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("writes"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("read_bytes"), func() Distribution { return MWD(ND(100, 1), 0) }},
		{[]byte("write_bytes"), func() Distribution { return MWD(ND(100, 1), 0) }},
		{[]byte("read_time"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("write_time"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("io_time"), func() Distribution { return MWD(ND(5, 1), 0) }},
	}
)

type DiskIOMeasurement struct {
	timestamp time.Time

	serial []byte
	// distributions []Distribution
	fieldValues []int64
}

func NewDiskIOMeasurement(start time.Time) *DiskIOMeasurement {
	distributions := make([]Distribution, len(DiskIOFields))
	for i := range DiskIOFields {
		distributions[i] = DiskIOFields[i].DistributionMaker()
	}

	serial := []byte(fmt.Sprintf("%03d-%03d-%03d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000)))
	if Config != nil { // partial override from external config
		serial = Config.GetTagBytesValue(DiskIOByteString, SerialByteString, true, serial)
	}
	return &DiskIOMeasurement{
		serial: serial,

		timestamp: start,
		// distributions: distributions,
		fieldValues: make([]int64, len(DiskIOFields)),
	}
}

func (m *DiskIOMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// for i := range m.distributions {
	// 	m.distributions[i].Advance()
	// }
}

func (m *DiskIOMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(DiskIOByteString)
	// p.SetTimestamp(&m.timestamp)

	p.AppendTag(SerialByteString, m.serial)
	letterIdxBits := 5                           // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	for i, cache, remain := len(m.fieldValues)-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		idx := cache & letterIdxMask
		value := atomic.AddInt64(&m.fieldValues[i], idx)
		p.AppendField(DiskIOFields[i].Label, value)
		i--

		cache >>= letterIdxBits
		remain--
	}
	// for i := range m.fieldValues {
	// 	value := atomic.AddInt64(&m.fieldValues[i], num)
	// 	p.AppendField(DiskIOFields[i].Label, ))
	// }
	return true
}
