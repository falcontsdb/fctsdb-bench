package devops

import (
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	MemoryByteString = []byte("mem") // heap optimization

	// Choices for modeling a host's memory capacity.
	MemoryMaxBytesChoices = []uint64{8 << 30, 12 << 30, 16 << 30}

	// Field keys for 'mem' points.
	MemoryFieldKeys = [][]byte{
		[]byte("total"),
		[]byte("available"),
		[]byte("used"),
		[]byte("free"),
		[]byte("cached"),
		[]byte("buffered"),
		[]byte("used_percent"),
		[]byte("available_percent"),
		[]byte("buffered_percent"),
	}
)

type MemMeasurement struct {
	// this doesn't change:
	bytesTotal uint64

	// these change:
	timestamp time.Time
	// bytesUsedDist, bytesCachedDist, bytesBufferedDist Distribution
	// bytesUsed, bytesCached, bytesBufferedDist int64
}

func NewMemMeasurement(start time.Time) *MemMeasurement {
	bytesTotal := MemoryMaxBytesChoices[fastrand.Uint32n(uint32(len(MemoryMaxBytesChoices)))]
	// bytesUsedDist := &ClampedRandomWalkDistribution{
	// 	State: rand.Float64() * float64(bytesTotal),
	// 	Min:   0.0,
	// 	Max:   float64(bytesTotal),
	// 	Step: &NormalDistribution{
	// 		Mean:   0.0,
	// 		StdDev: float64(bytesTotal) / 64,
	// 	},
	// }
	// bytesCachedDist := &ClampedRandomWalkDistribution{
	// 	State: rand.Float64() * float64(bytesTotal),
	// 	Min:   0.0,
	// 	Max:   float64(bytesTotal),
	// 	Step: &NormalDistribution{
	// 		Mean:   0.0,
	// 		StdDev: float64(bytesTotal) / 64,
	// 	},
	// }
	// bytesBufferedDist := &ClampedRandomWalkDistribution{
	// 	State: rand.Float64() * float64(bytesTotal),
	// 	Min:   0.0,
	// 	Max:   float64(bytesTotal),
	// 	Step: &NormalDistribution{
	// 		Mean:   0.0,
	// 		StdDev: float64(bytesTotal) / 64,
	// 	},
	// }
	return &MemMeasurement{
		timestamp: start,

		bytesTotal: bytesTotal,
		// bytesUsedDist:     bytesUsedDist,
		// bytesCachedDist:   bytesCachedDist,
		// bytesBufferedDist: bytesBufferedDist,
	}
}

func (m *MemMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// m.bytesUsedDist.Advance()
	// m.bytesCachedDist.Advance()
	// m.bytesBufferedDist.Advance()
}

func (m *MemMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(MemoryByteString)
	// p.SetTimestamp(&m.timestamp)

	total := int64(m.bytesTotal)
	used := int64(fastrand.Uint64n(m.bytesTotal))
	cached := int64(fastrand.Uint64n(m.bytesTotal))
	buffered := int64(fastrand.Uint64n(m.bytesTotal))

	p.AppendField(MemoryFieldKeys[0], total)
	p.AppendField(MemoryFieldKeys[1], total-used)
	p.AppendField(MemoryFieldKeys[2], used)
	p.AppendField(MemoryFieldKeys[3], cached)
	p.AppendField(MemoryFieldKeys[4], buffered)
	p.AppendField(MemoryFieldKeys[5], used)
	p.AppendField(MemoryFieldKeys[6], 100.0*(float64(used)/float64(total)))
	p.AppendField(MemoryFieldKeys[7], 100.0*(float64(total-used)/float64(total)))
	p.AppendField(MemoryFieldKeys[8], 100.0*(float64(total-buffered)/float64(total)))
	return true
}
