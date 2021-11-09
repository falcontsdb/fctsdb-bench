package devops

import (
	"fmt"
	// "math/rand"
	"sync/atomic"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/common"
	rand "git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	NetByteString = []byte("net") // heap optimization
	NetTags       = [][]byte{
		[]byte("interface"),
	}

	NetFields = []LabeledDistributionMaker{
		{[]byte("bytes_sent"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("bytes_recv"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("packets_sent"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("packets_recv"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("err_in"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("err_out"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("drop_in"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("drop_out"), func() Distribution { return MWD(ND(5, 1), 0) }},
	}
)

type NetMeasurement struct {
	timestamp time.Time

	interfaceName []byte
	uptime        time.Duration
	// distributions []Distribution
	fieldValues []int64
}

func NewNetMeasurement(start time.Time) *NetMeasurement {
	distributions := make([]Distribution, len(NetFields))
	for i := range NetFields {
		distributions[i] = NetFields[i].DistributionMaker()
	}

	interfaceName := []byte(fmt.Sprintf("eth%d", rand.Intn(4)))
	if Config != nil { // partial override from external config
		interfaceName = Config.GetTagBytesValue(NetByteString, NetTags[0], true, interfaceName)
	}
	return &NetMeasurement{
		interfaceName: interfaceName,

		timestamp: start,
		// distributions: distributions,
		fieldValues: make([]int64, len(NetFields)),
	}
}

func (m *NetMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// for i := range m.distributions {
	// 	m.distributions[i].Advance()
	// }
}

func (m *NetMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(NetByteString)
	// p.SetTimestamp(&m.timestamp)

	p.AppendTag(NetTags[0], m.interfaceName)

	letterIdxBits := 5                           // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	for i, cache, remain := len(m.fieldValues)-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		idx := cache & letterIdxMask
		value := atomic.AddInt64(&m.fieldValues[i], idx)
		p.AppendField(NetFields[i].Label, value)
		i--

		cache >>= letterIdxBits
		remain--
	}
	// for i := range m.distributions {
	// 	p.AppendField(NetFields[i].Label, int64(m.distributions[i].Get()))
	// }
	return true
}
