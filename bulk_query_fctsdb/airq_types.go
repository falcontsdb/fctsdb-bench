package bulk_query_fctsdb

import "time"

var (
	AirqTypes = NewQueryTypes("air-quality")
)

func init() {

	// case 1
	AirqTypes.Regist(&QueryType{
		Name:      "airq_from_one_site_newest",
		RawSql:    "select * from city_air_quality where site_id = {site_id} order by time desc limit 1;",
		Comment:   "查询某个站点最新的一条数据\n业务用途：实时查看站点空气质量监\n控数据库能力：指定tag按时间排序取最新数据",
		Generator: &airqFromOneSiteNewest{},
	})
	// case 2.1
	AirqTypes.Regist(&QueryType{
		Name:      "airq_from_sites_newest(10)",
		RawSql:    "select * from city_air_quality where site_id in ({site_id1}, {site_id2}, ...) group by site_id order by time desc limit 1;",
		Comment:   "查询一批站点最新的一条数据\n业务用途：监控一批站点的实时监控数据，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &airqFromSitesNewest{count: 10},
	})
	// case 2.2
	AirqTypes.Regist(&QueryType{
		Name:      "airq_from_sites_newest(10)",
		RawSql:    "select * from city_air_quality where site_id in ({site_id1}, {site_id2}, ...) group by site_id order by time desc limit 1;",
		Comment:   "查询一批站点最新的一条数据\n业务用途：监控一批站点的实时监控数据，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &airqFromSitesNewest{count: 100},
	})
	// case 2.3
	AirqTypes.Regist(&QueryType{
		Name:      "airq_from_sites_newest(10)",
		RawSql:    "select * from city_air_quality where site_id in ({site_id1}, {site_id2}, ...) group by site_id order by time desc limit 1;",
		Comment:   "查询一批站点最新的一条数据\n业务用途：监控一批站点的实时监控数据，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &airqFromSitesNewest{count: 1000},
	})

	// case 3.1
	// case3中包含两个语句，这里是第一条
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_from_one_site_in_a_day",
		RawSql:    "select count(aqi) from city_air_quality where site_id = {site_id} and time > now()-1d;",
		Comment:   "分页查询某个站点最近一天的空气质量数据\n业务用途：用于统计分析查询\n数据库能力：指定tag和时间段，分页查看数据",
		Generator: &countOfDataFromOneSite{Period: time.Hour * 24},
	})

	// case 3.2
	// case3中包含两个语句，这里是第二条
	AirqTypes.Regist(&QueryType{
		Name:      "limit-offset-with-time-of-one-site",
		RawSql:    "select * from city_air_quality where device_id = {site_id} and time > now()-1d order by time desc limit 100 offset 100;",
		Comment:   "分页查询某个站点最近一天的空气质量数据\n业务用途：用于统计分析查询\n数据库能力：指定tag和时间段，分页查看数据",
		Generator: &limitOffsetWithTimeOfOneSite{Period: time.Hour * 24},
	})

	// case 4
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_from_one_site_in_a_month",
		RawSql:    "select count(aqi) from city_air_quality where site_id = {site_id} and time > now()-30d;",
		Comment:   "统计查询最近一个月某站点新条数据\n业务用途：通常用于统计分析或者每月计费等\n数据库能力：指定tag和一个月时间段，计算某个field的count数",
		Generator: &countOfDataFromOneSite{Period: time.Hour * 24 * 30},
	})

	// case 5
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_in_a_month",
		RawSql:    "select count(aqi) from city_air_quality where time > now()-30d;",
		Comment:   "统计查询最近一个月所有站点总共新增了多少条数据\n业务用途：通常用于统计分析，每月生成报表等\n数据库能力：指定一个月时间段，计算某个field的count数",
		Generator: &countOfData{Period: time.Hour * 24 * 30},
	})

	// case 6
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_group_by_tag_in_a_month",
		RawSql:    "select count(aqi) from city_air_quality where city = {city} and time > now()-30d group by county;",
		Comment:   "在某个城市里，按区县分组，统计查询最近一个月城市里所有区县新增了多少数据\n业务用途：通常用于统计分析，每月生成报表等\n数据库能力：指定一个月时间段，并按tag分组，计算某个field的count数",
		Generator: &countOfDataGroupByTag{Period: time.Hour * 24 * 30},
	})

	// case 7
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_from_one_site_in_a_year",
		RawSql:    "select count(aqi) from city_air_quality where site_id={site_id} and time > now()-1y;",
		Comment:   "统计查询最近一年某站点新增了多少条数据\n业务用途：通常用于年度统计分析报表等\n数据库能力：指定tag和年时间段，计算某个field的count数",
		Generator: &countOfDataFromOneSite{Period: time.Hour * 24 * 365},
	})

	// case 8
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_in_a_year",
		RawSql:    "select count(aqi) from city_air_quality where time > now()-1y;",
		Comment:   "统计查询最近一年所有车辆总共新增了多少条数据\n业务用途：通常用于年度统计分析报表等\n数据库能力：指定一年时间段，计算某个field的count数",
		Generator: &countOfData{Period: time.Hour * 24 * 365},
	})

	// case 9
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_data_group_by_city_in_a_year",
		RawSql:    "select count(aqi) from city_air_quality where time > now()-1y group by city;",
		Comment:   "按城市分组，统计查询最近一年所有城市新增了多少数据\n业务用途：通常用于年度统计分析报表等\n数据库能力：指定一年时间段，并按中层级tag分组，计算某个field的count数",
		Generator: &countOfDataGroupByCity{Period: time.Hour * 24 * 365},
	})

	// case 10
	AirqTypes.Regist(&QueryType{
		Name:      "mean_of_last_group_by",
		RawSql:    "select mean(*) from (select last(*) from city_air_quality where city={city} group by site_id)",
		Comment:   "查询城市级别最新实时数据\n业务用途：用于页面展示城市级别实时数据\n数据库能力：对子查询求平均值，子查询为：指定某中层级tag，并按某低层级tag分组，返回分组最新值",
		Generator: &meanOfLastGroupBy{},
	})

	// case 11
	AirqTypes.Regist(&QueryType{
		Name:      "last_group_by",
		RawSql:    "select last(*) from city_air_quality where city={city} group by site_id",
		Comment:   "查询某城市按站点分组的站点实时数据\n业务用途：用于页面展示城市级别站点实时数据\n数据库能力：指定某低层级tag，并按某tag分组，返回分组最新值",
		Generator: &lastGroupBy{},
	})

	// case 12
	AirqTypes.Regist(&QueryType{
		Name:      "mean_of_one_site_group_by_time",
		RawSql:    "select mean(*) from city_air_quality where site_id = {site_id} and time > now()-24h group by time(1h);",
		Comment:   "按小时分组，查看过去24小时某站点各污染物的平均值变化\n业务用途：展示某站点24小时污染物变化趋势\n数据库能力：指定某低层级tag和24h时间段，并按1h作为分组窗口，查询所有字段平均值",
		Generator: &meanOfOneSiteGroupByTime{Period: time.Hour * 24, GroupByPeriod: time.Hour},
	})

	// case 13
	AirqTypes.Regist(&QueryType{
		Name:      "mean_of_one_city_group_by_time",
		RawSql:    "select mean(*) from city_air_quality where city = {city} and time > now()-24h group by time(1h);",
		Comment:   "按小时分组，查看过去24小时某站点各污染物的平均值变化\n业务用途：展示某站点24小时污染物变化趋势\n数据库能力：指定某中层级tag和24h时间段，并按1h作为分组窗口，查询所有字段平均值",
		Generator: &meanOfOneCityGroupByTime{Period: time.Hour * 24, GroupByPeriod: time.Hour},
	})

	// case 14
	AirqTypes.Regist(&QueryType{
		Name:      "top_of_last_group_by_site",
		RawSql:    "select top(aqi, 100), site_id from (select last(aqi) as aqi from city_air_quality where city={city} group by site_id)",
		Comment:   "查看某城市下所有站点的aqi实时排序\n业务用途：查看某城市下的所有站点污染物实时排名\n数据库能力：对子查询求topN。子查询为：指定中层级tag，并按低层级tag分组，查询分组最新值",
		Generator: &topOfLastGroupBySite{},
	})

	//todo where从句中的city=是否有必要存在

	// case 15
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_mean_group_by_time",
		RawSql:    "select count(aqi) from (select mean(aqi) as aqi from city_air_quality where city = {city} and site_id = {site_id} and time > now()-30d group by time(1d)) where aqi > 50;",
		Comment:   "查看一个月内某城市某站点的aqi在指定区间范围内的天数\n业务用途：查看某站点在一个月内，天气质量为优/良/差的天数\n数据库能力：对子查询某字段做范围查询，并求count。子查询为：指定中层级tag和底层级tag，以及1个月时间段，并按天时间分组，查询某字段平均值",
		Generator: &countOfMeanGroupBytime{Period: time.Hour * 24 * 30, GroupByPeriod: time.Hour * 24},
	})

	// case 16
	AirqTypes.Regist(&QueryType{
		Name:      "count_of_mean_group_by_time",
		RawSql:    "select count(aqi) from (select mean(aqi) as aqi from city_air_quality where city = {city} and time > now()-30d group by time(1d)) where aqi > 50;",
		Comment:   "查看一个月内某城市的aqi在指定区间范围内的天数\n业务用途：查看某城市在一个月内，天气质量为优/良/差的天数\n数据库能力：对子查询某字段做范围查询，并求count。子查询为：指定中层级tag，以及1个月时间段，并按天时间分组，查询某字段平均值",
		Generator: &countOfMeanGroupBytime1{Period: time.Hour * 24 * 30, GroupByPeriod: time.Hour * 24},
	})

	// case 17
	AirqTypes.Regist(&QueryType{
		Name:      "top_of_mean_group_by_city",
		RawSql:    "select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where province={province} and time > now()-1d group by city)",
		Comment:   "查看最近一天的省内城市排序\n业务用途：查看某省内最近一天的所有城市排名\n数据库能力：对子查询求topN。子查询为：指定高层级tag和最近一天时间段，并按中层级分组，查询某字段平均值",
		Generator: &topOfMeanGroupByCity{Period: time.Hour * 24 * 30},
	})

	// case 18
	AirqTypes.Regist(&QueryType{
		Name:      "top_of_mean_group_by_city1",
		RawSql:    "select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where time > now()-1d group by city)",
		Comment:   "查看最近一天的全国城市排序\n业务用途：查看全国最近一天的所有城市排名\n数据库能力：对子查询求topN。子查询为：指定最近一天时间段，并按中层级分组，查询某字段平均值",
		Generator: &topOfMeanGroupByCity1{Period: time.Hour * 24},
	})

	// case 19
	AirqTypes.Regist(&QueryType{
		Name:      "top_of_mean_group_by_city1_in_a_month",
		RawSql:    "select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where time > now()-1d group by city)",
		Comment:   "查看最近一个月的全国城市排序\n业务用途：查看全国最近一月的所有城市排名\n数据库能力：对子查询求topN。子查询为：指定最近一个月时间段，并按中层级分组，查询某字段平均值",
		Generator: &topOfMeanGroupByCity1{Period: time.Hour * 24 * 30},
	})

	//todo
	// case 20
	AirqTypes.Regist(&QueryType{
		Name:      "top_of_mean_group_by_city1_in_a_month",
		RawSql:    "select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where time > now()-1d group by city)",
		Comment:   "查看某城市过去某月按天分组的某污染物平均值\n业务用途：用于历史统计，作为污染日历展示\n数据库能力：对子查询求topN。子查询为：指定中层级tag和一个月时间段，并按1天为时间窗口分组，查询某字段平均值",
		Generator: &topOfMeanGroupByCity1{Period: time.Hour * 24 * 30},
	})

}
