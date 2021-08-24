package timescaledb

import (
	"time"

	bulkQuerygen "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_gen"
)

// TimescaleDevopsSingleHost produces Timescale-specific queries for the devops single-host case.
type TimescaleDevopsSingleHost struct {
	TimescaleDevops
}

func NewTimescaleDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleDevops)
	return &TimescaleDevopsSingleHost{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
