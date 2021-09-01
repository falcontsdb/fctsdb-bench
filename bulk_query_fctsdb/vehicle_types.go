package bulk_query_fctsdb

var (
	VehicleTypes = NewQueryTypes("vehicle")
)

func init() {

	// case 1
	VehicleTypes.Regist(&QueryType{
		Name:      "查询某辆车的最新状态",
		RawSql:    "select * from vehicle where VIN={id} order by time desc limit 1;",
		Comment:   "业务用途：监控车辆的实时运行状态\n数据库能力：指定tag按时间排序取最新数据",
		Generator: &OneCarNewest{},
	})

	// case 2.1
	VehicleTypes.Regist(&QueryType{
		Name:      "查询一批辆车（10辆）的最新状态",
		RawSql:    "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment:   "业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &CarsNewest{count: 10},
	})

	// case 2.2
	VehicleTypes.Regist(&QueryType{
		Name:      "查询一批辆车（100辆）的最新状态",
		RawSql:    "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment:   "业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &CarsNewest{count: 100},
	})

	// case 2.3
	VehicleTypes.Regist(&QueryType{
		Name:      "查询一批辆车（500辆）的最新状态",
		RawSql:    "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment:   "业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &CarsNewest{count: 500},
	})

	// case 2.4
	VehicleTypes.Regist(&QueryType{
		Name:      "查询一批辆车(1000辆)的最新状态",
		RawSql:    "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment:   "业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等\n数据库能力：指定一批tag，并按tag分组时间排序取最新数据",
		Generator: &CarsNewest{count: 1000},
	})

	// case 3
	VehicleTypes.Regist(&QueryType{
		Name:      "分页查询某辆车的最近一天的状态变化",
		RawSql:    "select * from vehicle where VIN={id} and time > now()-1d order by time desc limit 100 offset 100;",
		Comment:   "业务用途：用于展示查看一段时间车辆的状态变化\n数据库能力：指定tag和时间段，分页查看数据",
		Generator: &CarPaging{},
	})

	// case 4
	VehicleTypes.Regist(&QueryType{
		Name:      "统计查询最近一个月某辆车新增了多少条数据",
		RawSql:    "select count(value1) from vehicle where VIN={id} and time > now()-30d;",
		Comment:   "业务用途：通常用于统计分析或者每月计费等\n指定tag和一个月时间段，计算某个field的count数",
		Generator: &OneCarMessageCountMonth{},
	})

	// case 5
	VehicleTypes.Regist(&QueryType{
		Name:      "统计查询最近一个月所有车辆总共新增了多少条数据",
		RawSql:    "select count(value1) from vehicle where time > now()-30d;",
		Comment:   "业务用途：通常用于统计分析，每月生成报表等\n数据库能力：指定一个月时间段，计算某个field的count数",
		Generator: &CarsMessageCountMonth{},
	})

	// case 6
	VehicleTypes.Regist(&QueryType{
		Name:      "按车辆分组，统计查询最近一个月所有车辆分别新增了多少数据",
		RawSql:    "select count(value1) from vehicle where time > now()-30d group by VIN;",
		Comment:   "业务用途：通常用于统计分析，每月生成报表等\n数据库能力：指定一个月时间段，并按tag分组，计算某个field的count数",
		Generator: &CarsGroupMessageCountMonth{},
	})

	// case 7
	VehicleTypes.Regist(&QueryType{
		Name:      "统计查询最近一年某辆车新增了多少条数据",
		RawSql:    "select count(value1) from vehicle where VIN={id} and time > now()-1y;",
		Comment:   "业务用途：通常用于年度统计分析报表等\n数据库能力：指定tag和一年时间段，计算某个field的count数",
		Generator: &OneCarMessageCountYear{},
	})

	// case 8
	VehicleTypes.Regist(&QueryType{
		Name:      "统计查询最近一年所有车辆总共新增了多少条数据",
		RawSql:    "select count(value1) from vehicle where time > now()-1y;",
		Comment:   "业务用途：通常用于年度统计分析报表等\n数据库能力：指定一年时间段，计算某个field的count数",
		Generator: &CarsMessageCountYear{},
	})

	// case 9
	VehicleTypes.Regist(&QueryType{
		Name:      "按车辆分组，统计查询最近一年所有车辆分别新增了多少数据",
		RawSql:    "select count(value1) from vehicle where time > now()-1y group by VIN;",
		Comment:   "业务用途：通常用于年度统计分析报表等\n数据库能力：指定一年时间段，并按tag分组，计算某个field的count数",
		Generator: &CarsGroupMessageCountYear{},
	})

}
