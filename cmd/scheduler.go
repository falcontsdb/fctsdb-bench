package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/agent"
	"git.querycap.com/falcontsdb/fctsdb-bench/buildin_testcase"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"github.com/spf13/cobra"
)

var (
	heads = []string{"Group", "Mod", "UseCase", "Cardinality", "Workers", "BatchSize", "QueryPercent", "SamplingTime",
		"P50(r)", "P90(r)", "P95(r)", "P99(r)", "Min(r)", "Max(r)", "Avg(r)", "Fail(r)", "Total(r)", "Qps(r)",
		"P50(w)", "P90(w)", "P95(w)", "P99(w)", "Min(w)", "Max(w)", "Avg(w)", "Fail(w)", "Total(w)", "Qps(w)", "PointRate(p/s)", "ValueRate(v/s)", "TotalPoints",
		"RunSec", "Gzip", "Sql", "Monitor"}

	scheduleCmd = &cobra.Command{
		Use:   "schedule",
		Short: "从配置文件中读取执行任务并顺序执行",
		Run: func(cmd *cobra.Command, args []string) {
			scheduler.ScheduleBenchTask()
		},
		Hidden: !FullFunction,
	}

	scheduler = &Scheduler{}

	showCmd = &cobra.Command{
		Use:   "list",
		Short: "展示内置的调度器配置",
		Run: func(cmd *cobra.Command, args []string) {
			// b, _ := json.Marshal(BasicBenchTaskConfig{})
			buf := bytes.NewBuffer(make([]byte, 0, 1024))
			jsonEncoder := json.NewEncoder(buf)
			jsonEncoder.SetEscapeHTML(false)
			for _, config := range buildin_testcase.BuildinConfigs {
				err := jsonEncoder.Encode(config)
				if err != nil {
					fmt.Println("marshal buildin config faild:", err.Error())
				} else {
					fmt.Print(buf.String())
				}
				buf.Reset()
			}
		},
	}

	outFile        string
	creatReportCmd = &cobra.Command{
		Use:   "create",
		Short: "根据csv文件生成测试报告",
		Run: func(cmd *cobra.Command, args []string) {
			fileNames := make([]string, 0)
			for _, arg := range args {
				fileNames = append(fileNames, strings.TrimSuffix(arg, ".csv"))
			}
			report := buildin_testcase.CreateReport(fileNames...)
			var f *os.File
			if outFile == "" {
				f, _ = os.Create(fileNames[len(fileNames)-1] + ".html")
				defer f.Close()
			} else {
				f, _ = os.Create(outFile)
			}
			report.ToHtmlOneFile(f)
		},
	}
)

type Scheduler struct {
	csvDaemonUrls   string
	configsPath     string
	agentEndpoints  []string
	grafanaEndpoint string
	debug           bool
	format          string
	username        string
	password        string
	withEncryption  bool
}

func init() {

	scheduleCmd.Flags().StringVar(&scheduler.csvDaemonUrls, "urls", "http://localhost:8086", "被测数据库的地址")
	scheduleCmd.Flags().StringVar(&scheduler.configsPath, "config-file", "", "调度器配置文件地址 (默认不使用)")
	scheduleCmd.Flags().StringSliceVar(&scheduler.agentEndpoints, "agent", nil, "数据库代理服务地址，为空表示不使用 (默认不使用)")
	scheduleCmd.Flags().StringVar(&scheduler.grafanaEndpoint, "grafana", "", "grafana的dashboard地址，例如: http://124.71.230.36:4000/sources/1/dashboards/4")
	scheduleCmd.Flags().StringVar(&scheduler.format, "format", "fctsdb", "目标数据库类型，当前仅支持fctsdb和mysql")
	scheduleCmd.Flags().BoolVar(&scheduler.withEncryption, "withEncryption", false, "是否采用加密数据库进行测试")
	scheduleCmd.Flags().StringVar(&scheduler.username, "username", "", "用户名")
	scheduleCmd.Flags().StringVar(&scheduler.password, "password", "", "密码")
	scheduleCmd.Flags().BoolVar(&scheduler.debug, "debug", false, "是否打印详细日志(default false).")

	scheduleCmd.AddCommand(showCmd)

	creatReportCmd.Flags().StringVar(&outFile, "out", "", "生成的目标文件名字")
	scheduleCmd.AddCommand(creatReportCmd)
}

func (s *Scheduler) ScheduleBenchTask() {

	fileName := time.Now().Format("benchmark_0102_150405")
	// 开始时还原agent的配置并检查是否正确

	if len(s.agentEndpoints) != 0 {
		for _, agentEndpoint := range s.agentEndpoints {
			err := agent.ResetAgent(agentEndpoint)
			if err != nil {
				log.Fatalln(agentEndpoint, "reset failed, please check the anget config:", err.Error())
			}
		}
	}
	// 根据配置文件执行测试
	if s.configsPath != "" {
		actions := s.checkConfigsFile()
		var index = 0
		for _, action := range actions {
			switch action.act {
			case "set":
				err := agent.SetAgent(s.agentEndpoints[0], action.object.(map[string]string))
				if err != nil {
					log.Fatalln("set agent failed:", err.Error())
				}
			case "stop":
				err := agent.StopRemoteDatabase(s.agentEndpoints[0])
				if err != nil {
					log.Fatalln("stop remote database failed:", err.Error())
				}
			case "run":
				index += 1
				err := s.runBenchTaskByConfig(index, fileName, action.object.(buildin_testcase.BasicBenchTaskConfig))
				if err != nil {
					log.Fatalln("run testcase failed:", err.Error())
				}
			}
		}
	} else { // 执行内置测试
		for i, config := range buildin_testcase.BuildinConfigs {
			err := s.runBenchTaskByConfig(i+1, fileName, config)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
	// 记录环境信息到最后一行
	if len(s.agentEndpoints) != 0 {
		envBytes, err := agent.GetEnvironment(s.agentEndpoints[0])
		log.Println(string(envBytes))
		if err == nil {
			csvFile, err := os.OpenFile(fileName+".csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				log.Println("open result csv failed, error:", err.Error())
			}
			defer csvFile.Close()
			csvWriter := csv.NewWriter(csvFile)
			oneLine := make([]string, len(heads))
			oneLine[0] = "test-env"
			oneLine[1] = string(envBytes)
			csvWriter.Write(oneLine)
			csvWriter.Flush()
		}
	}
	// 如果是执行内置测试，生成测试报告，根据配置文件执行的测试，需要手动生成测试报告
	if s.configsPath == "" {
		report := buildin_testcase.CreateReport(fileName)
		f, _ := os.Create(fileName + ".html")
		defer f.Close()
		report.ToHtmlOneFile(f)
	}
}

type Action struct {
	act    string
	object interface{}
}

func (s *Scheduler) checkConfigsFile() []Action {

	configsFile, err := os.Open(s.configsPath)
	if err != nil {
		log.Fatal("Invalid config path:", s.configsPath)
	}
	defer configsFile.Close()
	scanner := bufio.NewScanner(bufio.NewReaderSize(configsFile, 4*1024*1024))
	lindID := 0

	actions := make([]Action, 0)
	for scanner.Scan() {
		lindID++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) < 4 {
			continue
		}
		if bytes.HasPrefix(line, []byte("$Set")) {
			param := make(map[string]string)
			line := bytes.TrimSpace(line[4:])
			err := json.Unmarshal(line, &param)
			if err != nil {
				log.Fatalln("cannot unmarshal the $Set line:", lindID, "error:", err.Error())
			}
			err = agent.SetAgent(s.agentEndpoints[0], param)
			if err != nil {
				log.Fatalln("$Set action can not be execute, line:", lindID, "error:", err.Error())
			}
			actions = append(actions, Action{act: "set", object: param})
		} else if bytes.HasPrefix(line, []byte("$Stop")) {
			actions = append(actions, Action{act: "stop"})
		} else {
			config := buildin_testcase.BasicBenchTaskConfig{}
			err := json.Unmarshal(line, &config)
			if err != nil {
				log.Fatalln("cannot unmarshal the config line:", lindID, "error:", err.Error())
			}
			actions = append(actions, Action{act: "run", object: config})
		}
	}
	return actions
}

func (s *Scheduler) runBenchTaskByConfig(index int, fileName string, config buildin_testcase.BasicBenchTaskConfig) error {
	log.Printf("---index %d ------------------------------------------------------------\n", index)
	if len(s.agentEndpoints) != 0 {
		for _, agentEndpoint := range s.agentEndpoints {
			err := agent.StopRemoteDatabase(agentEndpoint)
			if err != nil {
				log.Println("request agent error:", err.Error())
			}
			log.Println(agentEndpoint, "Stop the fctsdb")
		}
		if config.Clean {
			for _, agentEndpoint := range s.agentEndpoints {
				err := agent.CleanRemoteDatabase(agentEndpoint)
				if err != nil {
					log.Println("request agent error:", err.Error())
				}
				log.Println(agentEndpoint, "Clean the fctsdb data")
			}
		}
		for _, agentEndpoint := range s.agentEndpoints {
			err := agent.StartRemoteDatabase(agentEndpoint)
			if err != nil {
				log.Println("request agent error:", err.Error())
			}
			log.Println(agentEndpoint, "Start the fctsdb")
		}
	}

	basicBenchTask, err := s.NewBasicBenchTask(config)
	if err != nil {
		return err
	}
	result := RunBenchTask(basicBenchTask)
	var writeHead = true
	if index > 1 {
		writeHead = false
	}
	result["Group"] = config.Group
	s.writeResultToCsv(fileName, result, writeHead)

	return nil
}

func (s *Scheduler) writeResultToCsv(fileName string, info map[string]string, writeHead bool) {
	csvFile, err := os.OpenFile(fileName+".csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("open result csv failed, error:", err.Error())
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)

	if writeHead {
		err := csvWriter.Write(heads)
		if err != nil {
			log.Println("write result csv failed, error:", err.Error())
		}
	}

	if s.grafanaEndpoint != "" {
		u, err := url.Parse(s.grafanaEndpoint)
		if err != nil {
			log.Println("The grafana url is error:", err.Error())
		} else {
			fu, _ := url.Parse(s.csvDaemonUrls)
			u.RawQuery = fmt.Sprintf("refresh=Paused&tempVars[host]=%s&lower=%s&upper=%s",
				strings.Split(fu.Host, ":")[0], info["Start"], info["End"])
			info["Monitor"] = u.String()
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

func (s *Scheduler) NewBasicBenchTask(conf buildin_testcase.BasicBenchTaskConfig) (*BasicBenchTask, error) {
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
		csvDaemonUrls:     s.csvDaemonUrls,
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
		debug:             s.debug,
		dbName:            "benchmark_db",
		needPrePare:       conf.NeedPrePare,
		format:            s.format,
		username:          s.username,
		password:          s.password,
		withEncryption:    s.withEncryption,
	}, nil
}

func ParseDuration(s string) (time.Duration, error) {
	// Return an error if the string is blank or one character
	if len(s) < 2 {
		return 0, errors.New("invalid duration")
	}

	// Split string into individual runes.
	a := []rune(s)

	// Start with a zero duration.
	var d time.Duration
	i := 0

	// Check for a negative.
	isNegative := false
	if a[i] == '-' {
		isNegative = true
		i++
	}

	var measure int64
	var unit string

	// Parsing loop.
	for i < len(a) {
		// Find the number portion.
		start := i
		for ; i < len(a) && isDigit(a[i]); i++ {
			// Scan for the digits.
		}

		// Check if we reached the end of the string prematurely.
		if i >= len(a) || i == start {
			return 0, errors.New("invalid duration")
		}

		// Parse the numeric part.
		n, err := strconv.ParseInt(string(a[start:i]), 10, 64)
		if err != nil {
			return 0, errors.New("invalid duration")
		}
		measure = n

		// Extract the unit of measure.
		// If the last two characters are "ms" then parse as milliseconds.
		// Otherwise just use the last character as the unit of measure.
		unit = string(a[i])
		switch a[i] {
		case 'n':
			if i+1 < len(a) && a[i+1] == 's' {
				unit = string(a[i : i+2])
				d += time.Duration(n)
				i += 2
				continue
			}
			return 0, errors.New("invalid duration")
		case 'u', 'µ':
			d += time.Duration(n) * time.Microsecond
		case 'm':
			if i+1 < len(a) && a[i+1] == 's' {
				unit = string(a[i : i+2])
				d += time.Duration(n) * time.Millisecond
				i += 2
				continue
			}
			d += time.Duration(n) * time.Minute
		case 's':
			d += time.Duration(n) * time.Second
		case 'h':
			d += time.Duration(n) * time.Hour
		case 'd':
			d += time.Duration(n) * 24 * time.Hour
		case 'w':
			d += time.Duration(n) * 7 * 24 * time.Hour
		default:
			return 0, errors.New("invalid duration")
		}
		i++
	}

	// Check to see if we overflowed a duration
	if d < 0 && !isNegative {
		return 0, fmt.Errorf("overflowed duration %d%s: choose a smaller duration or INF", measure, unit)
	}

	if isNegative {
		d = -d
	}
	return d, nil
}

func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }
