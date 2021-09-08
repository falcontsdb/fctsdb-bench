# fctsdb-bench
fctsdb场景性能测试工具基于开源的[influxdb-comparisons](https://github.com/influxdata/influxdb-comparisons)工具改编而来。
用于实现[海东青性能测试用例设计](https://rockontrol.yuque.com/gtoifn/zd5mco/erg0pp))的内容.
influxdb-comparisons工具数据生成、写入、查询等所有过程分开，每个过程一套工具。
fctsdb-bench设计时不采用这种思想，一个fctsdb-bench工具集合了数据的生成、写入、查询语句生成、查询等所有命令

## 1 编译
进入fctsdb-bench的目录，在此目录下执行以下命令即可生成fctsdb-bench工具
```
go build -o fcbench cmd/bench-fctsdb/*.go
```

其他influxdb-comparisons原生自带的工具仍然可以编译，详情见influxdb-comparisons的文档，此处不再累述。

## 2 工具使用手册
###  2.1 数据测试

使用以下命令进行数据写入测试
```
fcbench write --use-case vehicle --scale-var 1000 --sampling-interval 10s --urls http://localhost:8086
```
上述命令表示使用车载（vehicle）场景，模拟1000辆车，每个车采样时间间隔10s，在默认时间范围2018-01-01T00:00:00Z-2018-01-02T00:00:00Z的写入默认数据库benchmark_db中。
fcbench write这个命令集合了数据生成和数据写入两个过程，在这个过程中如果发现数据库不存在，会自动创建数据库。
如果已有数据库，不想创建数据库，可以添加--do-db-create=false

###  2.2 查询测试
使用fcbench query命令可以进行查询测试，它需要先使用2.1中的命令将数据写入到数据库进行测试。
<b>注意：</b>
为了保证查询语句能命中数据，以下参数必须和fcbench write保持一致。
```
--db string                    数据库的database名称 (default "benchmark_db")
--do-db-create                 是否创建数据库 (default true)
--sampling-interval duration   模拟机的采样时间 (default 1s)
--scale-var int                场景的变量，一般情况下是场景中模拟机的数量 (default 1)
--scale-var-offset int         场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)
--seed int                     全局随机数种子(设置为0是使用当前时间作为随机数种子) (default 12345678) 
--timestamp-end string         模拟机采样结束数据 (RFC3339) (default "2018-01-02T00:00:00Z")
--timestamp-start string       模拟机开始采样的时间 (RFC3339) (default "2018-01-01T00:00:00Z")
--urls string                  被测数据库的地址 (default "http://localhost:8086")
--use-case string              使用的测试场景(可选场景: vehicle, air-quality) (default "vehicle")
 ```
 这些参数在默认情况下是一致的，如果有修改，需要额外注意.

我们使用list命令可以查看所有内置的场景以及支持的查询类型。
```
fcbench list
```
--detail可以获取到更多的关于说明。

对应2.1章节的查询示例如下：
```
fcbench query 1 --use-case vehicle --scale-var 1000 --sampling-interval 10s --urls http://localhost:8086 --query-count 1000
```
查询测试提供两种结束控制，一种是以查询语句数量，使用--query-count标签；另一种是使用运行时间，--time-limit标签。注意：一旦使用--time-limit标签，--query-count就无效了。

###  2.3 数据写入的其他方式
2.1和2.2中的write和query命令都是使用协程，一边生成数据，一边写入。
经过调优，4核情况下，vehicle场景能支撑480 000 points/s的生成速度。air-quality场景能支撑1 000 000 points/s 

我们注意到influxdb-comparisons是先生成数据到文件或者stdout，再由另一个工具写入到数据库。
因此参照influxdb-comparisons工具，提供以下类似的命令进行方式对比。
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

###  3 混合读写（敬请期待...）