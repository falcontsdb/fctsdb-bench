package devops

import (
	// "math/rand"
	"sync/atomic"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	rand "git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	KernelByteString   = []byte("kernel") // heap optimization
	BootTimeByteString = []byte("boot_time")
	KernelFields       = []LabeledDistributionMaker{
		{[]byte("interrupts"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("context_switches"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("processes_forked"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("disk_pages_in"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("disk_pages_out"), func() Distribution { return MWD(ND(5, 1), 0) }},
	}
)

type KernelMeasurement struct {
	timestamp time.Time

	bootTime int64
	// uptime   time.Duration
	// distributions []Distribution
	fieldValues []int64
}

func NewKernelMeasurement(start time.Time) *KernelMeasurement {
	distributions := make([]Distribution, len(KernelFields))
	for i := range KernelFields {
		distributions[i] = KernelFields[i].DistributionMaker()
	}

	bootTime := rand.Int63n(240)
	return &KernelMeasurement{
		bootTime: bootTime,

		timestamp: start,
		// distributions: distributions,
		fieldValues: make([]int64, len(KernelFields)),
	}
}

func (m *KernelMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// for i := range m.distributions {
	// 	m.distributions[i].Advance()
	// }
}

func (m *KernelMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(KernelByteString)
	// p.SetTimestamp(&m.timestamp)

	p.AppendField(BootTimeByteString, m.bootTime)
	letterIdxBits := 3                           // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	for i, cache, remain := len(m.fieldValues)-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		idx := cache & letterIdxMask
		value := atomic.AddInt64(&m.fieldValues[i], idx)
		p.AppendField(KernelFields[i].Label, value)
		i--

		cache >>= letterIdxBits
		remain--
	}

	// for i := range m.distributions {
	// 	p.AppendField(KernelFields[i].Label, int64(m.distributions[i].Get()))
	// }
	return true
}
