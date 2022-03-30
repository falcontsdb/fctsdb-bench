package devops

import (
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	CPUByteString      = []byte("cpu")       // heap optimization
	CPUTotalByteString = []byte("cpu-total") // heap optimization
)

var (
	// Field keys for 'cpu' points.
	CPUFieldKeys = [][]byte{
		[]byte("usage_user"),
		[]byte("usage_system"),
		[]byte("usage_idle"),
		[]byte("usage_nice"),
		[]byte("usage_iowait"),
		[]byte("usage_irq"),
		[]byte("usage_softirq"),
		[]byte("usage_steal"),
		[]byte("usage_guest"),
		[]byte("usage_guest_nice"),
	}
)

type CPUMeasurement struct {
	timestamp time.Time
	// distributions []Distribution
}

func NewCPUMeasurement(start time.Time) *CPUMeasurement {
	// distributions := make([]Distribution, len(CPUFieldKeys))
	// for i := range distributions {
	// 	distributions[i] = &ClampedRandomWalkDistribution{
	// 		State: rand.Float64() * 100.0,
	// 		Min:   0.0,
	// 		Max:   100.0,
	// 		Step: &NormalDistribution{
	// 			Mean:   0.0,
	// 			StdDev: 1.0,
	// 		},
	// 	}
	// }
	return &CPUMeasurement{
		timestamp: start,
		// distributions: distributions,
	}
}

func (m *CPUMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	// for i := range m.distributions {
	// 	m.distributions[i].Advance()
	// }
}

func (m *CPUMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(CPUByteString)
	// p.SetTimestamp(&m.timestamp)
	letterIdxBits := 7                            // 6 bits to represent a letter index
	letterIdxMask := uint64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 64 / letterIdxBits            // # of letter indices fitting in 63 bits

	for i, cache, remain := len(CPUFieldKeys)-1, fastrand.Uint64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = fastrand.Uint64(), letterIdxMax
		}
		idx := int(cache & letterIdxMask)
		p.AppendField(CPUFieldKeys[i], float64(idx)/1.27151) // 0~127/1.27151的随机值
		i--

		cache >>= letterIdxBits
		remain--
	}
	// for i := range m.distributions {
	// 	p.AppendField(CPUFieldKeys[i], m.distributions[i].Get())
	// }

	return true
}
