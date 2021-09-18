package devops

import (
	// "math/rand"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	rand "git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

var (
	PostgresqlByteString = []byte("postgresl") // heap optimization
	PostgresqlFields     = []LabeledDistributionMaker{
		{[]byte("numbackends"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("xact_commit"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("xact_rollback"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blks_read"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blks_hit"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_returned"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_fetched"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_inserted"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_updated"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("tup_deleted"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("conflicts"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("temp_files"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("temp_bytes"), func() Distribution { return CWD(ND(1024, 1), 0, 1024*1024*1024, 0) }},
		{[]byte("deadlocks"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blk_read_time"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
		{[]byte("blk_write_time"), func() Distribution { return CWD(ND(5, 1), 0, 1000, 0) }},
	}
)

type PostgresqlMeasurement struct {
	timestamp time.Time
	// distributions []Distribution
	fieldValues []int64
}

func NewPostgresqlMeasurement(start time.Time) *PostgresqlMeasurement {
	distributions := make([]Distribution, len(PostgresqlFields))
	for i := range PostgresqlFields {
		distributions[i] = PostgresqlFields[i].DistributionMaker()
	}

	return &PostgresqlMeasurement{
		timestamp: start,
		// distributions: distributions,
		fieldValues: make([]int64, len(PostgresqlFields)),
	}
}

func (m *PostgresqlMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// for i := range m.distributions {
	// 	m.distributions[i].Advance()
	// }
}

func (m *PostgresqlMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(PostgresqlByteString)
	// p.SetTimestamp(&m.timestamp)

	letterIdxBits := 10                          // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	for i, cache, remain := len(m.fieldValues)-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		idx := cache & letterIdxMask
		// value := atomic.AddInt64(&m.fieldValues[i], idx)
		p.AppendField(PostgresqlFields[i].Label, idx)
		i--

		cache >>= letterIdxBits
		remain--
	}
	// for i := range m.distributions {
	// 	p.AppendField(PostgresqlFields[i].Label, int64(m.distributions[i].Get()))
	// }
	return true
}
