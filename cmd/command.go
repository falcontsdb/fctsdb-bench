// bulk_load_fctsdb loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"fmt"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"github.com/spf13/cobra"
)

var (
	mixReadWrite = &BasicBenchTask{needPrePare: true}
	mixedCmd     = &cobra.Command{
		Use:   "mixed",
		Short: "混合读写测试",
		Run: func(cmd *cobra.Command, args []string) {
			common.RunBenchTask(mixReadWrite)
		},
	}
	writeTask = &BasicBenchTask{mixMode: "write_only", needPrePare: false}
	writeCmd  = &cobra.Command{
		Use:   "write",
		Short: "生成数据并直接发送至数据库",
		Run: func(cmd *cobra.Command, args []string) {
			common.RunBenchTask(writeTask)
		},
	}
	queryTask = &BasicBenchTask{mixMode: "read_only", needPrePare: false}
	queryCmd  = &cobra.Command{
		Use:   "query",
		Short: "生成查询语句并直接发送至数据库",
		Run: func(cmd *cobra.Command, args []string) {
			common.RunBenchTask(queryTask)
		},
	}
)

func init() {
	InitMixed(mixReadWrite, mixedCmd)
	InitWrite(writeTask, writeCmd)
	InitQuery(queryTask, queryCmd)
	rootCmd.AddCommand(mixedCmd)
	rootCmd.AddCommand(writeCmd)
	rootCmd.AddCommand(queryCmd)
}

func InitMixed(task *BasicBenchTask, cmd *cobra.Command) {
	cmdFlags := cmd.Flags()
	cmdFlags.SortFlags = false
	// 信息参数
	// writeFlag.StringVar(&d.format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))
	cmdFlags.StringVar(&task.csvDaemonUrls, "urls", "http://localhost:8086", "*被测数据库的地址")
	cmdFlags.StringVar(&task.dbName, "db", "benchmark_db", "*数据库的database名称")
	cmdFlags.StringVar(&task.useCase, "use-case", common.CaseChoices[0], fmt.Sprintf("*使用的测试场景(可选场景: %s)", strings.Join(common.CaseChoices, ", ")))
	cmdFlags.Int64Var(&task.scaleVar, "scale-var", 1, "*场景的变量，一般情况下是场景中模拟机的数量")
	cmdFlags.Int64Var(&task.scaleVarOffset, "scale-var-offset", 0, "*场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)")
	cmdFlags.DurationVar(&task.samplingInterval, "sampling-interval", time.Second, "*模拟机的采样时间")
	cmdFlags.StringVar(&task.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "*开始测试前准备数据的开始时间 (RFC3339)")
	cmdFlags.StringVar(&task.timestampEndStr, "timestamp-prepare", "2018-01-01T00:10:00Z", "*开始测试前准备数据的结束时间 (RFC3339)")
	cmdFlags.Int64Var(&task.seed, "seed", 12345678, "*全局随机数种子(设置为0是使用当前时间作为随机数种子)")

	// 运行参数
	cmdFlags.IntVar(&task.batchSize, "batch-size", 100, "1个http请求中携带Point个数")
	cmdFlags.IntVar(&task.useGzip, "gzip", 1, "是否使用gzip,level[0-9],小于0表示不使用")
	cmdFlags.IntVar(&task.workers, "workers", 1, "并发的http个数")
	cmdFlags.StringVar(&task.mixMode, "mix-mode", "parallel", "混合模式，支持parallel(按线程比例混合)、request(按请求比例混合)")
	cmdFlags.IntVar(&task.queryType, "query-type", 1, "查询类型")
	cmdFlags.IntVar(&task.queryPercent, "query-percent", 0, "查询请求所占百分比 (default 0)")
	cmdFlags.Int64Var(&task.queryCount, "query-count", 1000, "生成的查询语句数量")
	cmdFlags.DurationVar(&task.timeLimit, "time-limit", -1, "最大测试时间(-1表示不生效)，>0会使参数timestamp-end失效")
	cmdFlags.BoolVar(&task.debug, "debug", false, "是否打印详细日志(default false).")

	// 高级参数
	cmdFlags.StringVar(&task.cpuProfile, "cpu-profile", "", "将cpu-profile信息写入文件的地址，用于自测此工具")
	cmdFlags.BoolVar(&task.doDBCreate, "do-db-create", true, "是否创建数据库")
}

func InitWrite(task *BasicBenchTask, cmd *cobra.Command) {
	cmdFlags := cmd.Flags()
	cmdFlags.SortFlags = false
	// 信息参数
	// writeFlag.StringVar(&d.format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))
	cmdFlags.StringVar(&task.csvDaemonUrls, "urls", "http://localhost:8086", "*被测数据库的地址")
	cmdFlags.StringVar(&task.dbName, "db", "benchmark_db", "*数据库的database名称")
	cmdFlags.StringVar(&task.useCase, "use-case", common.CaseChoices[0], fmt.Sprintf("*使用的测试场景(可选场景: %s)", strings.Join(common.CaseChoices, ", ")))
	cmdFlags.Int64Var(&task.scaleVar, "scale-var", 1, "*场景的变量，一般情况下是场景中模拟机的数量")
	cmdFlags.Int64Var(&task.scaleVarOffset, "scale-var-offset", 0, "*场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)")
	cmdFlags.DurationVar(&task.samplingInterval, "sampling-interval", time.Second, "*模拟机的采样时间")
	cmdFlags.StringVar(&task.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "*模拟机开始采样的时间 (RFC3339)")
	cmdFlags.StringVar(&task.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "*模拟机采样结束数据 (RFC3339)")
	cmdFlags.Int64Var(&task.seed, "seed", 12345678, "*全局随机数种子(设置为0是使用当前时间作为随机数种子)")

	// 运行参数
	cmdFlags.IntVar(&task.batchSize, "batch-size", 100, "1个http请求中携带Point个数")
	cmdFlags.IntVar(&task.useGzip, "gzip", 1, "是否使用gzip,level[0-9],小于0表示不使用")
	cmdFlags.IntVar(&task.workers, "workers", 1, "并发的http个数")
	cmdFlags.DurationVar(&task.timeLimit, "time-limit", 300*time.Second, "最大测试时间")
	cmdFlags.BoolVar(&task.debug, "debug", false, "是否打印详细日志(default false).")

	// 高级参数
	cmdFlags.StringVar(&task.cpuProfile, "cpu-profile", "", "将cpu-profile信息写入文件的地址，用于自测此工具")
	cmdFlags.BoolVar(&task.doDBCreate, "do-db-create", true, "是否创建数据库")

}

func InitQuery(task *BasicBenchTask, cmd *cobra.Command) {
	cmdFlags := cmd.Flags()
	cmdFlags.SortFlags = false
	// 信息参数
	// writeFlag.StringVar(&d.format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))
	cmdFlags.StringVar(&task.csvDaemonUrls, "urls", "http://localhost:8086", "*被测数据库的地址")
	cmdFlags.StringVar(&task.dbName, "db", "benchmark_db", "*数据库的database名称")
	cmdFlags.StringVar(&task.useCase, "use-case", common.CaseChoices[0], fmt.Sprintf("*使用的测试场景(可选场景: %s)", strings.Join(common.CaseChoices, ", ")))
	cmdFlags.Int64Var(&task.scaleVar, "scale-var", 1, "*场景的变量，一般情况下是场景中模拟机的数量")
	cmdFlags.Int64Var(&task.scaleVarOffset, "scale-var-offset", 0, "*场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)")
	cmdFlags.DurationVar(&task.samplingInterval, "sampling-interval", time.Second, "*模拟机的采样时间")
	cmdFlags.StringVar(&task.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "*模拟机开始采样的时间 (RFC3339)")
	cmdFlags.StringVar(&task.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "*模拟机采样结束数据 (RFC3339)")
	cmdFlags.Int64Var(&task.seed, "seed", 12345678, "*全局随机数种子(设置为0是使用当前时间作为随机数种子)")

	// 运行参数
	cmdFlags.IntVar(&task.batchSize, "batch-size", 1, "1个http请求中携带查询语句个数")
	cmdFlags.IntVar(&task.useGzip, "gzip", 1, "是否使用gzip,level[0-9],小于0表示不使用")
	cmdFlags.IntVar(&task.workers, "workers", 1, "并发的http个数")
	cmdFlags.IntVar(&task.queryType, "query-type", 1, "查询类型")
	cmdFlags.Int64Var(&task.queryCount, "query-count", 1000, "生成的查询语句数量")
	cmdFlags.DurationVar(&task.timeLimit, "time-limit", -1, "最大测试时间(-1表示不生效)，>0会使query-count参数失效")
	cmdFlags.BoolVar(&task.debug, "debug", false, "是否打印详细日志(default false).")

	// 高级参数
	cmdFlags.StringVar(&task.cpuProfile, "cpu-profile", "", "将cpu-profile信息写入文件的地址，用于自测此工具")
	cmdFlags.BoolVar(&task.doDBCreate, "do-db-create", true, "是否创建数据库")
}
