package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/fctsdb_query_gen"
	"github.com/spf13/cobra"
)

var (
	scheduleCmd = &cobra.Command{
		Use:   "schedule",
		Short: "从配置文件中读取执行任务并顺序执行",
		Run: func(cmd *cobra.Command, args []string) {
			ScheduleBenchTask()
		},
	}

	showCmd = &cobra.Command{
		Use:   "list",
		Short: "展示内置的调度器配置",
		Run: func(cmd *cobra.Command, args []string) {
			// b, _ := json.Marshal(BasicBenchTaskConfig{})
			buf := bytes.NewBuffer(make([]byte, 0, 1024))
			jsonEncoder := json.NewEncoder(buf)
			jsonEncoder.SetEscapeHTML(false)
			for _, config := range buildinConfigs {
				err := jsonEncoder.Encode(config)
				if err != nil {
					fmt.Println("marshal buildin config faild:", err.Error())
				} else {
					fmt.Print(buf.String())
				}
				buf.Reset()
			}
		},
		// Hidden: true, // 隐藏此命令，不对外使用，内部测试使用
	}

	csvDaemonUrls string
	configsPath   string
	agentEndpoint string
	nmonEndpoint  string
	debug         bool
)

var (
	defaultTimeLimite = "5m"

	// buildinConfigs = []BasicBenchTaskConfig{buildinConfig_1, buildinConfig_2, buildinConfig_3, buildinConfig_4, buildinConfig_5, buildinConfig_6, buildinConfig_7, buildinConfig_8, buildinConfig_9, buildinConfig_10}
	// buildinConfigs = []BasicBenchTaskConfig{BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[i+1].RawSql}}}
	buildinConfigs []BasicBenchTaskConfig
)

func init() {
	scheduleCmd.PersistentFlags().StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "被测数据库的地址")
	scheduleCmd.PersistentFlags().StringVar(&configsPath, "config-file", "", "调度器配置文件地址 (默认不使用)")
	scheduleCmd.PersistentFlags().StringVar(&agentEndpoint, "agent", "", "数据库代理服务地址，为空表示不使用 (默认不使用)")
	scheduleCmd.PersistentFlags().StringVar(&nmonEndpoint, "easy-nmon", "", "easy-nmon地址，为空表示不使用监控 (默认不使用)")
	scheduleCmd.PersistentFlags().BoolVar(&debug, "debug", false, "是否打印详细日志(default false).")
	rootCmd.AddCommand(scheduleCmd)
	scheduleCmd.AddCommand(showCmd)
	AddBuildinConfigs()
}

func ScheduleBenchTask() {

	fileName := time.Now().Format("benchmark_0102_150405") + ".csv"
	if configsPath != "" {
		configsFile, err := os.Open(configsPath)
		if err != nil {
			log.Fatal("Invalid config path:", configsFile)
		}
		var config BasicBenchTaskConfig
		scanner := bufio.NewScanner(bufio.NewReaderSize(configsFile, 4*1024*1024))
		lindID := 0

		for scanner.Scan() {
			line := scanner.Bytes()
			lindID++
			err := json.Unmarshal(line, &config)
			if err != nil {
				log.Println("cannot unmarshal the config line:", lindID)
			}
			err = runBenchTaskByConfig(lindID, fileName, &config)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	} else {
		for i, config := range buildinConfigs {
			err := runBenchTaskByConfig(i+1, fileName, &config)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
}

func runBenchTaskByConfig(index int, fileName string, config *BasicBenchTaskConfig) error {
	log.Printf("---index %d ------------------------------------------------------------\n", index)
	if agentEndpoint != "" {
		var err error
		if config.Clean {
			err = CleanRemoteFalconTSDB(agentEndpoint)
		} else {
			err = StopRemoteFalconTSDB(agentEndpoint)
		}
		if err != nil {
			log.Println("request agent error:", err.Error())
		}
		err = StartRemoteFalconTSDB(agentEndpoint)
		if err != nil {
			log.Println("request agent error:", err.Error())
		}
	}

	basicBenchTask, err := NewBasicBenchTask(csvDaemonUrls, config)
	if err != nil {
		return err
	}
	if nmonEndpoint != "" {
		SendStartMonitorSignal(nmonEndpoint, fileName[:len(fileName)-4], fmt.Sprintf("testcase_%03d", index))
	}
	result := RunBenchTask(basicBenchTask)
	if nmonEndpoint != "" {
		SendStopAllMonitorSignal(nmonEndpoint)
	}

	var writeHead = true
	if index > 1 {
		writeHead = false
	}
	writeResultToCsv(fileName, result, writeHead)

	return nil
}

type BasicBenchTaskConfig struct {
	MixMode          string
	UseCase          string
	Workers          int
	BatchSize        int
	ScaleVar         int64
	SamplingInterval string
	TimeLimit        string
	UseGzip          int
	QueryPercent     int
	PrePareData      string
	NeedPrePare      bool
	Clean            bool
	SqlTemplate      []string
}

func NewBasicBenchTask(csvDaemonUrls string, conf *BasicBenchTaskConfig) (*BasicBenchTask, error) {
	sampInter, err := ParseDuration(conf.SamplingInterval)
	if err != nil {
		return nil, fmt.Errorf("can not parse the SamplingInterval")
	}
	var timeLimit time.Duration
	var timestampEndStr string
	if conf.TimeLimit != "" {
		timeLimit, err = ParseDuration(conf.TimeLimit)
		if err != nil {
			return nil, fmt.Errorf("can not parse the SamplingInterval")
		}
	}
	if conf.PrePareData != "" {
		dataDuration, err := ParseDuration(conf.PrePareData)
		if err != nil {
			return nil, fmt.Errorf("can not parse the DataDuration")
		}
		defaultTimestampStart, _ := time.Parse(time.RFC3339, common.DefaultDateTimeStart)
		timestampEndStr = defaultTimestampStart.Add(dataDuration).Format(time.RFC3339)
	} else {
		timestampEndStr = common.DefaultDateTimeEnd
	}
	for _, temp := range conf.SqlTemplate {
		_, err := common.NewSqlTemplate(temp)
		if err != nil {
			return nil, fmt.Errorf("can not parse sql template: %s", err.Error())
		}
	}
	return &BasicBenchTask{
		csvDaemonUrls:     csvDaemonUrls,
		mixMode:           conf.MixMode,
		useCase:           conf.UseCase,
		workers:           conf.Workers,
		batchSize:         conf.BatchSize,
		scaleVar:          conf.ScaleVar,
		samplingInterval:  sampInter,
		timeLimit:         timeLimit,
		sqlTemplate:       conf.SqlTemplate,
		useGzip:           conf.UseGzip,
		timestampStartStr: common.DefaultDateTimeStart,
		timestampEndStr:   timestampEndStr,
		seed:              12345678,
		doDBCreate:        true,
		queryPercent:      conf.QueryPercent,
		queryCount:        100,
		debug:             debug,
		dbName:            "benchmark_db",
		needPrePare:       conf.NeedPrePare,
	}, nil
}

func writeResultToCsv(fileName string, info map[string]string, writeHead bool) {
	csvFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("open result csv failed, error:", err.Error())
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	// var heads []string
	// for key := range info {
	// 	heads = append(heads, key)
	// }
	heads := []string{"Mod", "UseCase", "Cardinality", "Workers", "BatchSize", "QueryPercent", "SamplingTime",
		"P50(r)", "P90(r)", "P95(r)", "P99(r)", "Min(r)", "Max(r)", "Avg(r)", "Fail(r)", "Total(r)", "Qps(r)",
		"P50(w)", "P90(w)", "P95(w)", "P99(w)", "Min(w)", "Max(w)", "Avg(w)", "Fail(w)", "Total(w)", "Qps(w)", "PointRate(p/s)", "ValueRate(v/s)", "TotalPoints",
		"RunSec", "Sql"}

	if writeHead {
		err := csvWriter.Write(heads)
		if err != nil {
			log.Println("write result csv failed, error:", err.Error())
		}
	}

	oneLine := make([]string, len(heads))
	for i := 0; i < len(heads); i++ {
		value, ok := info[heads[i]]
		if ok {
			oneLine[i] = value
		} else {
			oneLine[i] = ""
		}

	}
	err = csvWriter.Write(oneLine)
	if err != nil {
		log.Println("write result csv failed, error:", err.Error())
	}
	csvWriter.Flush()
}

func AddBuildinConfigs() {
	// 纯写
	// cardinality 变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 1, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 1000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// batchsize 变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 10, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 100, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 5000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 采样时间变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "10s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "30s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 并发数变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 8, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 16, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 32, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})

	// cardinality 变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 1, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 1000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// batchsize 变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 10, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 100, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 5000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 采样时间变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "10s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "30s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 并发数变化
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 8, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 16, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 32, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})

	// 纯读 air-quality
	// 先写数据， 第一个用例在开始前要清理所有数据和写入准备数据， NeedPrePare和Clean必须为ture，之后都不需要
	needPrePareAndClean := true
	for i := 0; i < fctsdb_query_gen.AirQuality.Count; i++ {
		buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: "90d", NeedPrePare: needPrePareAndClean, UseGzip: 1, Clean: needPrePareAndClean, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[i+1].RawSql}})
		needPrePareAndClean = false // 不用在准备数据
	}

	// 混合读写
	// 不同的sql
	for i := 0; i < fctsdb_query_gen.AirQuality.Count; i++ {
		buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[i+1].RawSql}})
	}
	// 不同的混合模式
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "request", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "request", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[5].RawSql}})

	// 不同的混合模式
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})

	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[5].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[5].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[5].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[5].RawSql}})

	// 固定写入线程数
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 30, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 40, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 60, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 120, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	// 固定查询线程数
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 120, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 60, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 40, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 30, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})

	// 混合读写
	// 不同的sql
	for i := 0; i < fctsdb_query_gen.Vehicle.Count; i++ {
		buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[i+1].RawSql}})
	}
	// 不同的混合模式
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "request", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "request", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[5].RawSql}})

	// 不同的混合模式
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[1].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[1].RawSql}})

	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[6].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[6].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[6].RawSql}})
	buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: "10m", NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{fctsdb_query_gen.Vehicle.Types[6].RawSql}})

	// buildinConfigs = append(buildinConfigs, BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{fctsdb_query_gen.AirQuality.Types[1].RawSql}})
}
