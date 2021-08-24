package influxdb

import (
	"fmt"
	"time"

	bulkQuerygen "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_gen"
)

// InfluxDashboardQueueBytes produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardQueueBytes struct {
	InfluxDashboard
}

func NewInfluxQLDashboardQueueBytes(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardQueueBytes{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardQueueBytes(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardQueueBytes{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardQueueBytes) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT mean("queueBytes") FROM "telegraf"."default"."influxdb_hh_processor" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY time(1m), "host" fill(0)
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT mean(\"temp_files\") FROM postgresl WHERE cluster_id = '%s' and %s group by time(1m), hostname, fill(0)", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "postgresl" and r._field == "temp_files" and r.cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> group(columns: ["hostname"]) `+
			`|> aggregateWindow(every: 1m, fn: mean, createEmpty: true) `+
			`|> fill(value: 0.0) `+
			`|> keep(columns: ["_time", "_value", "hostname"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Hinted HandOff Queue Size (MB), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
