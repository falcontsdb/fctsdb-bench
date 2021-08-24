package influxdb

import (
	"time"

	bulkQuerygen "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_gen"
)

func NewInfluxQLGroupAggregateLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Last, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxGroupAggregateLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Last, Flux, dbConfig, queriesFullRange, scaleVar)
}
