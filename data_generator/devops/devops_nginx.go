package devops

import (
	"fmt"
	// "math/rand"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	NginxByteString = []byte("nginx") // heap optimization

	NginxTags = [][]byte{
		[]byte("port"),
		[]byte("server"),
	}

	NginxFields = []LabeledDistributionMaker{
		{[]byte("accepts"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("active"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("handled"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("reading"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("requests"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("waiting"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("writing"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
	}
)

type NginxMeasurement struct {
	timestamp time.Time

	port, serverName []byte
	// distributions    []Distribution
	fieldValues []int64
}

func NewNginxMeasurement(start time.Time) *NginxMeasurement {
	distributions := make([]Distribution, len(NginxFields))
	for i := range NginxFields {
		distributions[i] = NginxFields[i].DistributionMaker()
	}

	serverName := []byte(fmt.Sprintf("nginx_%d", fastrand.Uint32n(100000)))
	port := []byte(fmt.Sprintf("%d", fastrand.Uint32n(20000)+1024))
	if Config != nil { // partial override from external config
		serverName = Config.GetTagBytesValue(NginxByteString, NginxTags[1], true, serverName)
		port = Config.GetTagBytesValue(NginxByteString, NginxTags[0], true, port)
	}
	return &NginxMeasurement{
		port:       port,
		serverName: serverName,

		timestamp: start,
		// distributions: distributions,
		fieldValues: make([]int64, len(NginxFields)),
	}
}

func (m *NginxMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// for i := range m.distributions {
	// 	m.distributions[i].Advance()
	// }
}

func (m *NginxMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(NginxByteString)
	// p.SetTimestamp(&m.timestamp)

	p.AppendTag(NginxTags[0], m.port)
	p.AppendTag(NginxTags[1], m.serverName)

	letterIdxBits := 7                            // 6 bits to represent a letter index
	letterIdxMask := uint64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 64 / letterIdxBits            // # of letter indices fitting in 63 bits

	for i, cache, remain := len(m.fieldValues)-1, fastrand.Uint64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = fastrand.Uint64(), letterIdxMax
		}
		idx := cache & letterIdxMask
		// value := atomic.AddInt64(&m.fieldValues[i], idx)
		p.AppendField(NginxFields[i].Label, int64(idx)) // 0~128之间随机整数
		i--

		cache >>= letterIdxBits
		remain--
	}
	// for i := range m.distributions {
	// 	p.AppendField(NginxFields[i].Label, int64(m.distributions[i].Get()))
	// }
	return true
}
