package bulk_query_fctsdb

var (
	AirqTypes = NewQueryTypes("air-quality")
)

func init() {

	// case 1
	AirqTypes.Regist(&QueryType{
		Name:   "one_care_newest",
		RawSql: "select * from vehicle where VIN={id} order by time desc limit 1",
		Comment: `查询某辆车的最新状态
业务用途：监控车辆的实时运行状态
数据库能力：指定tag按时间排序取最新数据`,
		Generator: &OneDeviceNewest{},
	})

	//case 2
	// AirqTypes.Regist(&QueryType{
	// 		Name:   "one_care_newest",
	// 		RawSql: "select * from vehicle where VIN={id} order by time desc limit 1",
	// 		Comment: `查询某辆车的最新状态
	// 业务用途：监控车辆的实时运行状态
	// 数据库能力：指定tag按时间排序取最新数据`,
	// 	})

}
