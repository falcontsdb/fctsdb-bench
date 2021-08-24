package cassandra

import (
	"time"

	bulkQuerygen "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_gen"
)

// CassandraDevopsGroupby produces Cassandra-specific queries for the devops groupby case.
type CassandraDevopsGroupby struct {
	CassandraDevops
}

func NewCassandraDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*CassandraDevops)
	return &CassandraDevopsGroupby{
		CassandraDevops: *underlying,
	}

}

func (d *CassandraDevopsGroupby) Dispatch(i int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
