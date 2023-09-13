# fctsdb-bench
fctsdb场景性能测试工具基于开源的[influxdb-comparisons](https://github.com/influxdata/influxdb-comparisons)工具重构而来。
用于实现[海东青性能测试用例设计](https://rockontrol.yuque.com/gtoifn/zd5mco/erg0pp))的内容.

influxdb-comparisons工具数据生成、写入、查询等所有过程分开，每个过程一套工具。
fctsdb-bench设计时不采用这种思想，一个fctsdb-bench工具集合了数据的生成、写入、查询语句生成、查询等所有命令

## 1 编译
进入fctsdb-bench的目录，在此目录下执行以下命令make即可生成fctsdb-bench工具

会生成三个可执行文件：

fcbench是本机可以执行程序，一般使用这个程序

fcbench-amd64是amd64处理器的linux系统可执行文件

fcbench-arm64是arm64处理器的linux系统可执行文件

## 2 工具使用手册

使用fcbench -h可以查看所有支持的子命令，如下：
```
Available Commands:
 
  agent       代理程序，和数据库运行在一起，支持被远程调用开启关闭数据库（开发团队内部使用）
  list        展示所有场景（case）和对应的查询语句类型（query-type）
  mixed       混合读写测试
  mock        模仿海东青数据库，测试本工具能力上限
  query       生成查询语句并直接发送至数据库
  schedule    从配置文件中读取执行任务并顺序执行
  write       生成数据并直接发送至数据库

  query-gen(隐藏命令)   生成数据库查询语句，输出到stdout，搭配query-load使用
  query-load(隐藏命令)  从文件或者stdin载入查询语句，并发送查询到数据库，需要先使用query-gen命令
  data-gen(隐藏命令)    生成不同场景（case）的数据，输出到stdout，搭配data-load使用
  data-load(隐藏命令)   从文件或者stdin载入数据，并发送数据到数据库，需要先使用data-gen命令
```
###  2.1 数据写入测试
使用fcbench write命令可以进行数据写入测试
-h查看帮助信息，支持的flags如下：
```
Flags:
      --urls string                  *被测数据库的地址 (default "http://localhost:8086")
      --db string                    *数据库的database名称 (default "benchmark_db")
      --use-case string              *使用的测试场景(可选场景: vehicle, air-quality, devops) (default "vehicle")
      --scale-var int                *场景的变量，一般情况下是场景中模拟机的数量 (default 1)
      --scale-var-offset int         *场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)
      --sampling-interval duration   *模拟机的采样时间 (default 1s)
      --timestamp-start string       *模拟机开始采样的时间 (RFC3339) (default "2018-01-01T00:00:00Z")
      --timestamp-end string         *模拟机采样结束数据 (RFC3339) (default "2018-01-02T00:00:00Z")
      --seed int                     *全局随机数种子(设置为0是使用当前时间作为随机数种子) (default 12345678)
      --batch-size int               1个http请求中携带Point个数 (default 100)
      --gzip int                     是否使用gzip,level[0-9],小于0表示不使用 (default 1)
      --workers int                  并发的http个数 (default 1)
      --time-limit duration          最大测试时间(-1表示不生效)，>0会使参数timestamp-end失效 (default -1ns)
      --debug                        是否打印详细日志(default false).
      --cpu-profile string           将cpu-profile信息写入文件的地址，用于自测此工具
      --do-db-create                 是否创建数据库 (default true)
```
<b>注意：带*的参数叫做信息元参数，其他为运行参数</b>

例如：使用以下命令
```
fcbench write --use-case vehicle --scale-var 1000 --sampling-interval 10s --urls http://localhost:8086
```
上述命令表示使用车载（vehicle）场景，模拟1000辆车，每个车采样时间间隔10s，在默认时间范围2018-01-01T00:00:00Z~~~2018-01-02T00:00:00Z的写入默认数据库benchmark_db中。

fcbench write这个命令集合了数据生成和数据写入两个过程，在这个过程中如果发现数据库不存在，会自动创建数据库。

如果已有数据库，不想创建数据库，可以添加--do-db-create=false

###  2.2 查询测试
使用fcbench query命令可以进行查询测试，它需要先使用2.1中的命令将数据写入到数据库进行测试。

<b>注意：
为了保证查询语句能命中数据，以下参数中带*号的信息元参数必须和fcbench write保持一致。</b>
```
Flags:
      --urls string                  *被测数据库的地址 (default "http://localhost:8086")
      --db string                    *数据库的database名称 (default "benchmark_db")
      --use-case string              *使用的测试场景(可选场景: vehicle, air-quality, devops) (default "vehicle")
      --scale-var int                *场景的变量，一般情况下是场景中模拟机的数量 (default 1)
      --scale-var-offset int         *场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)
      --sampling-interval duration   *模拟机的采样时间 (default 1s)
      --timestamp-start string       *模拟机开始采样的时间 (RFC3339) (default "2018-01-01T00:00:00Z")
      --timestamp-end string         *模拟机采样结束数据 (RFC3339) (default "2018-01-02T00:00:00Z")
      --seed int                     *全局随机数种子(设置为0是使用当前时间作为随机数种子) (default 12345678)
      --batch-size int               1个http请求中携带查询语句个数 (default 1)
      --gzip int                     是否使用gzip,level[0-9],小于0表示不使用 (default 1)
      --workers int                  并发的http个数 (default 1)
      --query-type int               查询类型 (default 1)
      --query-count int              生成的查询语句数量 (default 1000)
      --time-limit duration          最大测试时间(-1表示不生效)，>0会使query-count参数失效 (default -1ns)
      --debug                        是否打印详细日志(default false).
      --cpu-profile string           将cpu-profile信息写入文件的地址，用于自测此工具
      --do-db-create                 是否创建数据库 (default true)
```
这些参数在默认情况下是一致的，如果有修改，需要额外注意.

一个执行查询测试的例子：
1、我们使用list命令可以查看所有内置的场景以及支持的查询类型。
```
fcbench list
```
--detail可以获取到更多的关于说明。

2、写入数据，注意信息元参数
```
fcbench write --use-case vehicle --scale-var 1000 --sampling-interval 10s --urls http://localhost:8086
```

3、对应步骤2章节的查询，信息元参数一致，如下：
```
fcbench query --query-type 1 --use-case vehicle --scale-var 1000 --sampling-interval 10s --urls http://localhost:8086 --query-count 1000
```
查询测试提供两种结束控制，一种是以查询语句数量，使用--query-count标签；另一种是使用运行时间，--time-limit标签。

注意：一旦使用--time-limit标签，--query-count就无效了。例如下面测试5分钟：
```
fcbench query --query-type 1 --use-case vehicle --scale-var 1000 --sampling-interval 10s --urls http://localhost:8086 --time-limit 5m
```

###  2.3 混合读写
使用fcbench mixed命令可以进行混合查询测试，支持的参数如下：
```
Flags:
      --urls string                  *被测数据库的地址 (default "http://localhost:8086")
      --db string                    *数据库的database名称 (default "benchmark_db")
      --use-case string              *使用的测试场景(可选场景: vehicle, air-quality, devops) (default "vehicle")
      --scale-var int                *场景的变量，一般情况下是场景中模拟机的数量 (default 1)
      --scale-var-offset int         *场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)
      --sampling-interval duration   *模拟机的采样时间 (default 1s)
      --timestamp-start string       *开始测试前准备数据的开始时间 (RFC3339) (default "2018-01-01T00:00:00Z")
      --timestamp-prepare string     *开始测试前准备数据的结束时间 (RFC3339) (default "2018-01-01T00:10:00Z")
      --seed int                     *全局随机数种子(设置为0是使用当前时间作为随机数种子) (default 12345678)
      --batch-size int               1个http请求中携带Point个数 (default 100)
      --gzip int                     是否使用gzip,level[0-9],小于0表示不使用 (default 1)
      --workers int                  并发的http个数 (default 1)
      --mix-mode string              混合模式，支持parallel(按线程比例混合)、request(按请求比例混合) (default "parallel")
      --query-type int               查询类型 (default 1)
      --query-percent int            查询请求所占百分比 (default 0)
      --query-count int              生成的查询语句数量 (default 1000)
      --time-limit duration          最大测试时间(-1表示不生效)，>0会使参数timestamp-end失效 (default -1ns)
      --debug                        是否打印详细日志(default false).
      --cpu-profile string           将cpu-profile信息写入文件的地址，用于自测此工具
      --do-db-create                 是否创建数据库 (default true)
```
一种典型的混合场景测试过程如下：

1、执行以下命令，<b>注意timestamp-start、timestamp-prepare参数，表明测试混合前先准备timestamp-start到timestamp-prepare的数据</b>
```
fcbench  mixed --query-type 1  write --use-case vehicle --scale-var 1000 --timestamp-start "2018-01-01T00:00:00Z" --timestamp-prepare "2018-01-02T00:00:00Z" --sampling-interval 10s --urls http://localhost:8086 
```

###  2.4 数据写入的其他方式
2.1和2.2中的write和query命令都是使用协程，一边生成数据，一边写入。
经过调优，4核情况下，vehicle场景能支撑480 000 points/s的生成速度。air-quality场景能支撑1 000 000 points/s 

我们注意到influxdb-comparisons是先生成数据到文件或者stdout，再由另一个工具写入到数据库。
因此参照influxdb-comparisons工具，也提供以下类似的命令，仅用来进行对比, 已经隐藏, 不对外展示。

数据生成：
```
fcbench data-gen --use-case vehicle --scale-var 1000 --sampling-interval 10s >> data.txt
```
数据写入：
```
fcbench data-load --urls http://localhost:8086 --file data.txt
```

查询语句生成：
```
fcbench query-gen --use-case vehicle --scale-var 1000 --sampling-interval 10s >> query.txt
```
查询语句查询：
```
fcbench query-load --urls http://localhost:8086 --file query.txt
```

###  2.5 高级功能-调度器（schedule）
使用fcbench schedule命令可以连续执行多次测试，配合需要使用fcbench agent命令


一般情况下，我们的测试拓扑如下：

<b>测试机（fcbench-schedule）</b> -------> <b>被测机（fctsdb数据库+fcbench-agent）</b>

为了支持两次测试间清理数据库：fcbench agent命令提供了一种方式，可以在被测机上，开启、关闭、清理数据库。
按照以下步骤进行测试：

1、被测机启动agent
```
fcbench agent --fctsdb-path /root/fctsdb/v16n/fctsdb --fctsdb-config /root/fctsdb/configs/test.conf
//注意agent默认监听端口为8966，可以通过option port来设置
```

2、可以查看调度器内置的配置文件，写入到testcase.txt：
```
fcbench schedule list > testcase.txt
```
根据需求修改testcase.txt, 根据配置文件运行多次测试, 结果会记录在一个最新时间为名字的csv中，例如benchmark_1013_173901.csv：
```
fchench schedule --agent http://{被测机ip}:agent端口 --grafana http://10.10.2.30:8888/sources/1/dashboards/3 --config-path testcase.txt 
```

其中--grafana是监控前端地址, 用来拼接完整的监控地址:  http://10.10.2.30:8888/sources/1/dashboards/3?refresh=Paused&tempVars%5Bhost%5D=10.10.2.29&lower=2022-09-07T10%3A36%3A53Z&upper=2022-09-07T10%3A41%3A53Z

3、使用以下命令可以将两次测试生成的csv进行对比, 并生成对比html测试报告：
```
./fcbench schedule create ~/result/fctsdb-amd/v15n.csv ~/result/fctsdb-amd/v16n.csv --out write-v15n-v16n.html
```

<b>配置文件</b>

下面介绍下配置文件中的参数和功能。
在配置文件中，每一行表示一个测试用例（testcase）。
```
{"Group":"车载Series变化","MixMode":"write_only","UseCase":"vehicle","Workers":64,"BatchSize":1000,"ScaleVar":1,"SamplingInterval":"1s","TimeLimit":"5s","UseGzip":1,"QueryPercent":0,"PrePareData":"","NeedPrePare":false,"Clean":true,"SqlTemplate":null}
```
1. 测试用例（testcase）的参数说明
```
Group：分组名，主要用于后续生成报告的时候进行分组展示
MixMode：混合方式，纯读，纯写，读写混合
UseCase: 数据集，用于设定测试所在执行的数据集，这些数据集集成在代码中，添加特定的数据集需要在代码中添加
Workers：并发数
BatchSize：写入时单体请求携带的数据量
ScaleVar ：数据集中series数量（series，时序数据库的概念）
SamplingInterval : 采样时间
TimeLimit：测试持续时间
UseGzip：请求使用的Gzip等级
QueryPercent： 查询请求比例，纯写测试时为0，纯读测试时为100
PrePareData ：准备多久的数据
NeedPrepare：是否需要准备
Clean：是否对当前数据库进行清理，如果为true，在用例执行前会通过agent控制数据库停止，删库，启动

SqlTemplate：在存在查询请求的测试中生效，具体内容看下一节
```

2. sql模板功能
在使用influxdb-comparisons工具进行测试过程中发现，它的查询语句需要在代码中添加，从而设置了该功能。例如
```sql
select mean(aqi) as aqi from city_air_quality where city in '{city*6}' and time >= '{now}'-30d group by time(1d)
```
这个语句中{city*6}表示在数据库中city的tag列中任选6个值填入这个地方，'{now}'表示最新一条数据的时间。

   
3. set功能
testcase.txt文件支持动态设置agent端的fctsdb数据库路径和config文件路径
实现方式是在文件中添加内容，一个典型的例子如下：

```
{"Group":"车载Series变化","MixMode":"write_only","UseCase":"vehicle","Workers":64,"BatchSize":1000,"ScaleVar":1,"SamplingInterval":"1s","TimeLimit":"5s","UseGzip":1,"QueryPercent":0,"PrePareData":"","NeedPrePare":false,"Clean":true,"SqlTemplate":null}
$Set {"BinPath":"/root/fctsdb/fctsdb", "ConfigPath":"/root/fctsdb/config"}
{"Group":"车载Series变化","MixMode":"write_only","UseCase":"vehicle","Workers":64,"BatchSize":1000,"ScaleVar":1000,"SamplingInterval":"1s","TimeLimit":"5s","UseGzip":1,"QueryPercent":0,"PrePareData":"","NeedPrePare":false,"Clean":true,"SqlTemplate":null}
```
在读取配置文件后，会对配置文件中所有Set语句都进行测试，以确定Set语句中提供的BinPath和ConfigPath都是可用的。

###  2.6 高级功能-mock
使用fcbench mock支持mock一个海东青数据库，用以测试环境是否达标。

## 3 代码结构



###  3.1 文档目录
```
.
├── agent                       agent功能的client和service代码目录
│   ├── client.go                  agent功能client代码
│   ├── fctsdb_handlers.go         agent功能service中fctsdb的handle
│   ├── influxdbv2_handlers.go     agent功能service中influxdbv2的handle
│   ├── matrixdb_handlers.go       agent功能service中matrixdb的handle
│   ├── mysql_handlers.go          agent功能service中mysql的handle
│   ├── opentsdb_handlers.go       agent功能service中opentsdb的handle
│   └── service.go                 agent功能service中外层代码
├── buildin_testcase            schedule命令内置测试用例以及这些用例的html对比测试报告生成器
│   ├── report.go                  对比报告生成器   
│   ├── testcase.go                内置用例定义文件
├── cmd                         命令行、运行框架文件
│   ├── agent.go                   agent命令的实现
│   ├── basic_bench_task.go        benchmark运行框架，定义了基础的一次性能测试的全部流程，关联write、query、mixed三个命令
│   ├── command.go                 write、query、mixed三个命令的定义文件
│   ├── data_gen.go                data_gen隐藏命令的实现
│   ├── data_load.go               data_load隐藏命令的实现                
│   ├── main.go                    程序入口文件
│   ├── qps.go                     basic_bench_task所需的qps处理文件
│   ├── query_gen.go               query_gen命令的实现
│   ├── query_load.go              query_load命令的实现
│   └── scheduler.go               scheduler命令的实现
├── data_generator              不同场景数据生成模块，生成的结果对象为common子模块的point对象
│   ├── airq                       空气质量场景的数据与sql生成器模块
│   ├── common                     所有场景所需的通用抽象
│   ├── dashboard                  dashboard场景，来源于influxdb-comparisons，暂未完全适配我们框架
│   ├── devops                     devops场景，来源于influxdb-comparisons，暂未完全适配我们框架
│   ├── iot                        iot场景，来源于influxdb-comparisons，暂未完全适配我们框架
│   ├── live                       生活消费场景，临时测试
│   ├── metaqueries                metaqueries场景，来源于influxdb-comparisons，暂未完全适配我们框架
│   ├── universal                  universal--万能场景, 根据一些关键数量生成数据, 例如"{\"MeasurementCount\":2000,\"TagKeyCount\":1,\"FieldsDefine\":[40,40,20]}"
│   └── vehicle                    车载场景的数据与sql生成器模块
├── db_client                   数据库初始化、创建db、写入、查询、序列化器的模块
│   ├── common.go
│   ├── fctsdb_client.go
│   ├── influxdbv2_client.go
│   ├── matrixdb_client.go
│   ├── mysql_client.go
│   └── opentsdb_client.go
├── query_generator             内置的场景查询语句模板
├── report                      对比测试报告中需要用的简单组件渲染抽象
│   ├── page.go                    页面渲染，包括标题、测试组等
│   ├── picture                    图形组件渲染，折线图、柱形图等等
│   ├── src                        渲染所需js等资源文件
│   └── table                      表格组件渲染
└── util                        存放一些用到的小模块
    ├── fastrand                   一个快速的rand模块，使用golang runtime中的相关函数，协程安全且性能极高
    ├── gbt2260                    中国地理位置编码
    └── keydriver                  关键字驱动，未实现
```


###  3.2 写入数据核心设计思路

1、data_generator模块生成数据，产出对象为data_generator/common/point.go中的point对象
```
type Point struct {
	MeasurementName  []byte                 #表名字
	TagKeys          [][]byte               #tag名字
	TagValues        [][]byte               #tag的值
	FieldKeys        [][]byte               #field名字
	FieldValues      []interface{}          #field值
	Int64FiledKeys   [][]byte               #int64类型field名字，特例化，加速int64类型的转换
	Int64FiledValues []int64                #int64类型field值，特例化，加速int64类型的转换
	Timestamp        *time.Time             #时间搓
}
```
其中，对int64这种类型单独存储，是为了减少vehicle这种场景在大量int64的field情况下，转换成interface{}的时间消耗，提升性能。

2、point对象由db_client中不同的数据库进行序列化，主要是db_client/common.go的DBClient对象的以下三个方法。
```
      // 序列化器，序列化一个batch为目标，分为三个阶段。返回结果是append到一个bytes数组中。
	// 1、准备阶段，添加一些头信息或者类似mysql的列信息
	BeforeSerializePoints(buf []byte, p *common.Point) []byte
	// 2、序列化一个point对象，并把添加到bytes数组中
	SerializeAndAppendPoint(buf []byte, p *common.Point) []byte
	// 3、batch的尾部内容，例如一些结束符;等等
	AfterSerializePoints(buf []byte, p *common.Point) []byte
```
通过这三个方法，最后生成的结果一个是byte数组，包含一个batch的数据。

3、将步骤2中序列化后的byte数组进行发送，调用的db_client/common.go的DBClient对象的write方法。
```
      Write(body []byte) (int64, error) 
```
###  3.3 查询数据核心设计思路
1、在data_generator/common/sql_temlate.go文件中，设计了一种简单的模板替换。
```
// 举个例子：
// "select mean(aqi) as aqi from city_air_quality where city in '{city*6}' and time >= '{now}'-30d group by time(1d)"
// 将被分割成base段: "select mean(aqi) as aqi from city_air_quality where city in '"、"' and time >= '"、"'-30d group by time(1d)"三个
// 关键字: city、now
// 重复次数: 6、1
```

对应这个例子，在air场景中的data_generator/airq/generate_data.go文件的nextSql方法进行关键字替换，下列这个方法
```
func (s *AirqSimulator) NextSql(wr io.Writer) int64
```

最终生成下面这个完整sql：
```
select mean(aqi) as aqi from city_air_quality where city in '百色市','德宏傣族景颇族自治州','大连市','济宁市','佳木斯市','石家庄市' and time >= '2021-01-01T00:01:40+08:00'-30d group by time(1d)
```
完整的例子可见data_generator/airq/airq_test.go


2、将步骤1中得到sql进行发送，调用的db_client/common.go的DBClient对象的query方法。
```
      Query(body []byte) (int64, error) 
```

###  3.4 主要流程调用链
最主要的测试流程的调用链如下图：
```
cmd/main.go
  │ 
  ├── cmd/command.go   ───┐
  │                       ├──> basic_bench_task.go 
  ├── cmd/scheduler.go ───┘            ├──> data_generator(*.go) ──> util(*.go)
  │              │                     └──> db_clent(*.go)
  │              └──> agent/client.go  
  │                                   
  └── cmd/agent.go ───> agent/serivce.go ───> agent/*_handlers.go
```

html测试报告生成功能调用链
```
cmd/main.go 
  └── cmd/scheduler.go  ───> buildin_testcase/report.go ───>  reprot/*.go                                  
```

新加数据库，主要添加一个db_client文件


