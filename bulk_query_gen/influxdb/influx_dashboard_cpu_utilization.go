package influxdb

import (
	"fmt"
	"time"

	bulkQuerygen "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_gen"
)

// InfluxDashboardCpuUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardCpuUtilization struct {
	InfluxDashboard
}

func NewInfluxQLDashboardCpuUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardCpuUtilization{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardCpuUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardCpuUtilization{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardCpuUtilization) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT mean("usage_user") FROM "telegraf"."default"."cpu" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: GROUP BY host, time(1m)
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT mean(\"usage_user\") FROM cpu WHERE cluster_id = '%s' and %s group by hostname,time(1m)", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "cpu" and r._field == "usage_user" and r._cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> group(columns:["hostname"]) `+
			`|> aggregateWindow(every: 1m, fn: mean, createEmpty: false) `+
			`|> keep(columns: ["_time", "_value", "hostname"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) CPU Utilization (Percent), rand cluster, %s by host, 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
