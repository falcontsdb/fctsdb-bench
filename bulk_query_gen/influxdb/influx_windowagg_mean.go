package influxdb

import (
	"time"

	bulkQuerygen "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_gen"
)

func NewInfluxQLWindowAggregateMean(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxWindowAggregateQuery(Mean, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

func NewFluxWindowAggregateMean(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxWindowAggregateQuery(Mean, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
