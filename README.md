# fctsdb-bench
fctsdb场景性能测试工具基于开源的[influxdb-comparisons](https://github.com/influxdata/influxdb-comparisons)工具改编而来。
用于实现[海东青性能测试用例设计](https://rockontrol.yuque.com/gtoifn/zd5mco/erg0pp))的内容.

influxdb-comparisons工具数据生成、写入、查询等所有过程分开，每个过程一套工具。
fctsdb-bench设计时不采用这种思想，一个fctsdb-bench工具集合了数据的生成、写入、查询语句生成、查询等所有命令

## 1 编译
进入fctsdb-bench的目录，在此目录下执行以下命令make即可生成fctsdb-bench工具

会生成三个可执行文件：

fcbench是本机可以执行程序，一般使用这个程序

fcbench-amd64是amd64处理器的linux系统可执行文件

fcbench-arm64是arm64处理器的linux系统可执行文件

其他influxdb-comparisons原生自带的工具仍然可以编译，详情见influxdb-comparisons的文档，此处不再累述。

## 2 工具使用手册

使用fcbench -h可以查看所有支持的子命令，如下：
```
Available Commands:
  data-gen    生成不同场景（case）的数据，输出到stdout，搭配data-load使用
  data-load   从文件或者stdin载入数据，并发送数据到数据库，需要先使用data-gen命令
  list        展示所有场景（case）和对应的查询语句类型（query-type）
  mixed       混合读写测试
  mock        模仿海东青数据库，测试本工具能力上限
  query       生成查询语句并直接发送至数据库
  query-gen   生成数据库查询语句，输出到stdout，搭配query-load使用
  query-load  从文件或者stdin载入查询语句，并发送查询到数据库，需要先使用query-gen命令
  schedule    从配置文件中读取执行任务并顺序执行
  write       生成数据并直接发送至数据库
```
###  2.1 数据测试
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

###  2.5 高级功能-调度器
使用fcbench schedule命令可以连续执行多次测试，配合需要使用fcbench agent命令

一般情况下，我们的测试拓扑如下：

测试机（fcbench-schedule） ------- 被测机（fctsdb数据库+fcbench-agent）

为了支持两次测试间清理数据库：fcbench agent命令提供了一种方式，可以在被测机上，开启、关闭、清理数据库。

可以查看调度器内置的配置文件，写入到testcase.txt：
```
fcbench schedule list > testcase.txt
```
根据配置文件运行多次测试：
```
fchench schedule --agent http://{被测机ip}:端口  --config-path testcase.txt
```

###  2.6 高级功能-mock
使用fcbench mock支持mock一个海东青数据库，用以测试环境是否达标。

