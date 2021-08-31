package bulk_query_fctsdb

var (
	VehicleTypes = QueryTypes{
		CaseName: "vehicle",
	}
)

func init() {

	// case 1
	VehicleTypes.Regist(&QueryType{
		Name:   "one_car_newest",
		RawSql: "select * from vehicle where VIN={id} order by time desc limit 1;",
		Comment: `查询某辆车的最新状态
业务用途：监控车辆的实时运行状态
数据库能力：指定tag按时间排序取最新数据`,
		Generator: &OneCarNewest{},
	})

	// case 2
	VehicleTypes.Regist(&QueryType{
		Name:   "cars(10)_newest",
		RawSql: "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment: `查询一批辆车（10辆）的最新状态
		业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等
		数据库能力：指定一批tag，并按tag分组时间排序取最新数据`,
		Generator: &CarsNewest{count: 10},
	})

	// case 3
	VehicleTypes.Regist(&QueryType{
		Name:   "cars(100)_newest",
		RawSql: "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment: `查询一批辆车（100辆）的最新状态
		业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等
		数据库能力：指定一批tag，并按tag分组时间排序取最新数据`,
		Generator: &CarsNewest{count: 100},
	})

	// case 4
	VehicleTypes.Regist(&QueryType{
		Name:   "cars(500)_newest",
		RawSql: "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment: `查询一批辆车（500辆）的最新状态
		业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等
		数据库能力：指定一批tag，并按tag分组时间排序取最新数据`,
		Generator: &CarsNewest{count: 500},
	})

	// case 5
	VehicleTypes.Regist(&QueryType{
		Name:   "cars(1000)_newest",
		RawSql: "select * from vehicle where VIN in ({id1}, {id2}, ...) group by VIN order by time desc limit 1;",
		Comment: `查询一批辆车(100辆)的最新状态
		业务用途：监控一批车辆的实时运行状态，通常用于大屏监控等
		数据库能力：指定一批tag，并按tag分组时间排序取最新数据`,
		Generator: &CarsNewest{count: 1000},
	})

	// case 6
	VehicleTypes.Regist(&QueryType{
		Name:   "cars_paging",
		RawSql: "select * from vehicle where VIN={id} and time > now()-1d order by time desc limit 100 offset 100;",
		Comment: `分页查询某辆车的最近一天的状态变化
		业务用途：用于展示查看一段时间车辆的状态变化
		数据库能力：指定tag和时间段，分页查看数据`,
		Generator: &CarsPaging{},
	})

}
