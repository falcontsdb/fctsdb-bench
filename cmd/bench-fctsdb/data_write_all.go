package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"syscall"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/cobra"
)

type DataWriteAll struct {
	configsPath   string
	agentEndpoint string
}

// 一个简化版的配置，用来指定连续运行的
type DataWriteConfig struct {
	UseCase          string
	Workers          int
	BatchSize        int
	ScaleVar         int64
	SamplingInterval string
	DataDuration     string
	UseGzip          bool
}

var (
	buildinConfig_1 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "vehicle", ScaleVar: 1, SamplingInterval: "10s", DataDuration: "3650d", UseGzip: true}
	buildinConfig_2 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "vehicle", ScaleVar: 1000, SamplingInterval: "10s", DataDuration: "30d", UseGzip: true}
	buildinConfig_3 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "10s", DataDuration: "1d", UseGzip: true}
	buildinConfig_4 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "vehicle", ScaleVar: 100000, SamplingInterval: "10s", DataDuration: "1h", UseGzip: true}
	buildinConfig_5 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "air-quality", ScaleVar: 1, SamplingInterval: "10s", DataDuration: "3650d", UseGzip: true}
	buildinConfig_6 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "air-quality", ScaleVar: 1000, SamplingInterval: "10s", DataDuration: "30d", UseGzip: true}
	buildinConfig_7 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "10s", DataDuration: "1d", UseGzip: true}
	buildinConfig_8 = DataWriteConfig{Workers: 64, BatchSize: 5000, UseCase: "air-quality", ScaleVar: 100000, SamplingInterval: "10s", DataDuration: "1h", UseGzip: true}
	buildinConfigs  = []DataWriteConfig{buildinConfig_1, buildinConfig_2, buildinConfig_3, buildinConfig_4, buildinConfig_5, buildinConfig_6, buildinConfig_7, buildinConfig_8}

	dataWriteAll    = DataWriteAll{}
	dataWriteAllCmd = &cobra.Command{
		Use:   "all",
		Short: "隐藏命令，可以连续运行内置的写入用例",
		Run: func(cmd *cobra.Command, args []string) {
			// b, _ := json.Marshal(DataWriteConfig{})
			dataWriteAll.Run()
		},
		Hidden: true, // 隐藏此命令，不对外使用，内部测试使用
	}

	dataWriteShowCmd = &cobra.Command{
		Use:   "show",
		Short: "隐藏命令，展示内置的写入用例",
		Run: func(cmd *cobra.Command, args []string) {
			// b, _ := json.Marshal(DataWriteConfig{})
			for _, config := range buildinConfigs {
				configShow, err := json.Marshal(config)
				if err != nil {
					fmt.Println("marshal buildin config faild:", err.Error())
				} else {
					fmt.Println(string(configShow))
				}

			}
		},
		Hidden: true, // 隐藏此命令，不对外使用，内部测试使用
	}

	dataWriteAgentCmd = &cobra.Command{
		Use:   "agent",
		Short: "隐藏命令，远程控制",
		Run: func(cmd *cobra.Command, args []string) {
			RunAsAgent()
			// GetPidOnLinux("fctsdb")
		},
		Hidden: true, // 隐藏此命令，不对外使用，内部测试使用
	}

	fctsdbWriteAgent = FctsdbAgent{}
)

func init() {

	dataWriteAll.Init(dataWriteAllCmd)
	dataWriteCmd.AddCommand(dataWriteAllCmd)
	dataWriteCmd.AddCommand(dataWriteShowCmd)
	fctsdbWriteAgent.Init(dataWriteAgentCmd)
	dataWriteCmd.AddCommand(dataWriteAgentCmd)
	// 隐藏agent上级命令的flag
	dataWriteAgentCmd.SetUsageTemplate(`Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}

Global Flags:
  -h, --help                   查看帮助信息
`)
}

func (d *DataWriteConfig) CopyToDataWrite(dw *DataWrite) error {
	dw.useGzip = d.UseGzip
	dw.workers = d.Workers
	dw.batchSize = d.BatchSize
	dw.useCase = d.UseCase
	dw.scaleVar = d.ScaleVar
	sampInter, err := ParseDuration(d.SamplingInterval)
	if err != nil {
		return fmt.Errorf("can not parse the SamplingInterval")
	}
	dw.samplingInterval = sampInter
	dataDuration, err := ParseDuration(d.DataDuration)
	if err != nil {
		return fmt.Errorf("can not parse the DataDuration")
	}
	defaultTimestampStart, _ := time.Parse(time.RFC3339, common.DefaultDateTimeStart)
	dw.timestampEndStr = defaultTimestampStart.Add(dataDuration).Format(time.RFC3339)
	return nil
}

func (d *DataWriteAll) Init(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(&d.configsPath, "config-file", "", "config路径")
	flags.StringVar(&d.agentEndpoint, "agent", "http://localhost:8966", "数据库代理服务地址")
}

func (d *DataWriteAll) Validate() {
}

func (d *DataWriteAll) Run() {
	fileName := time.Now().Format("W-Jan2(15-04-05)") + ".csv"
	if d.configsPath != "" {
		configsFile, err := os.Open(d.configsPath)
		if err != nil {
			log.Fatal("Invalid config path:", configsFile)
		}
		var config DataWriteConfig
		scanner := bufio.NewScanner(bufio.NewReaderSize(configsFile, 4*1024*1024))
		lindID := 0
		for scanner.Scan() {
			line := scanner.Bytes()
			lindID++
			err := json.Unmarshal(line, &config)
			if err != nil {
				log.Println("cannot unmarshal the config line:", lindID)
			}
			err = config.CopyToDataWrite(dataWrite)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			result := RunWrite()
			var writeHead bool = true
			if lindID > 1 {
				writeHead = false
			}
			d.WriteResult(fileName, result, config, writeHead)
			d.AfterRun()

		}
	} else {
		i := 0
		for _, config := range buildinConfigs {
			i++
			err := config.CopyToDataWrite(dataWrite)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			result := RunWrite()
			var writeHead bool = true
			if i > 1 {
				writeHead = false
			}
			d.WriteResult(fileName, result, config, writeHead)
			d.AfterRun()
		}
	}
}

func (d *DataWriteAll) WriteResult(fileName string, r map[string]string, c DataWriteConfig, writeHead bool) {
	csvFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("open result csv failed, error:", err.Error())
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	tag := reflect.TypeOf(c)
	value := reflect.ValueOf(c)
	for k := 0; k < tag.NumField(); k++ {
		r[tag.Field(k).Name] = fmt.Sprintf("%v", value.Field(k).Interface())
	}

	heads := []string{"UseCase", "Workers", "BatchSize", "ScaleVar", "SamplingInterval", "TimestampEndStr",
		"UseGzip", "Points", "PointRate(p/s)", "ValueRate(v/s)", "BytesRate(MB/s)", "RunSec(s)"}

	if writeHead {
		err := csvWriter.Write(heads)
		if err != nil {
			log.Println("write result csv failed, error:", err.Error())
		}
	}

	oneLine := make([]string, len(heads))
	for i := 0; i < len(heads); i++ {
		oneLine[i] = r[heads[i]]
	}
	err = csvWriter.Write(oneLine)
	if err != nil {
		log.Println("write result csv failed, error:", err.Error())
	}
	csvWriter.Flush()
}

func (d *DataWriteAll) AfterRun() {
	d.HttpGet("/clean")
	time.Sleep(time.Second * 2)
	d.HttpGet("/start")
	time.Sleep(time.Second * 10)
}

func (d *DataWriteAll) HttpGet(path string) ([]byte, error) {

	u, err := url.Parse(d.agentEndpoint)
	if err != nil {
		log.Fatal("Invalid agent address:", d.agentEndpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("Invalid agent address:", d.agentEndpoint)
	}
	u.Path = path

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	rData, err := ioutil.ReadAll(resp.Body)
	return rData, err
}

// agent 代码

type FctsdbAgent struct {
	port       string
	fctsdbPath string
	configPath string

	//run var
	dbConfig *DbConfig
}

func RunAsAgent() {
	fctsdbWriteAgent.Validate()
	fctsdbWriteAgent.ListenAndServe()
}

func (f *FctsdbAgent) Init(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(&f.port, "port", "8966", "监听端口")
	flags.StringVar(&f.fctsdbPath, "fctsdb-path", "./fctsdb", "数据库二进制文件地址")
	flags.StringVar(&f.configPath, "fctsdb-config", "./config", "数据库config文件地址")
}

func (f *FctsdbAgent) Validate() {
	f.ParseConfig()
	f.StartFalconTSDB()
}

func (f *FctsdbAgent) ParseConfig() {
	var err error
	f.dbConfig, err = DecodeFromConfigFile(f.configPath)
	if err != nil {
		log.Fatal("Decode fctsdb config failed, error:", err.Error())
	}
}

func (f *FctsdbAgent) ListenAndServe() {

	http.HandleFunc("/clean", f.CleanHandler)
	http.HandleFunc("/start", f.StartDBHandler)
	log.Println("Start service 0.0.0.0:" + f.port)
	err := http.ListenAndServe("0.0.0.0:"+f.port, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func (f *FctsdbAgent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Host)
	if r.Method == "GET" {
		err := f.StartFalconTSDB()
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: " + "start falconTSDB successful")
	}
}

func (f *FctsdbAgent) CleanHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := f.CleanFalconTSDB()
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: clean falconTSDB successful")
	}
}

func (f *FctsdbAgent) CleanFalconTSDB() error {
	pid, err := GetPidOnLinux("fctsdb")
	if err != nil {
		log.Println("Clean fctsdb failed, error: " + err.Error())
		return err
	}
	if err = KillOnLinux(pid); err != nil {
		log.Println("Clean fctsdb failed, error: " + err.Error())
		return err
	}

	//check that fctsdb exists or not
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		pid, _ = GetPidOnLinux("fctsdb")
		if pid == "" {
			log.Println("Stop falconTSDB succeed")
			//clear data directory of database
			for _, dir := range []string{f.dbConfig.Meta.Dir, f.dbConfig.Data.Dir, f.dbConfig.Data.SnapshotDir, f.dbConfig.Data.WalDir} {
				err := os.RemoveAll(dir)
				if err != nil {
					return err
				}
			}
			return nil
		}
	}

	err = errors.New("clean failed, there is another fctsdb running, pid is %s" + pid)
	log.Println(err.Error())
	return err
}

func (f *FctsdbAgent) StartFalconTSDB() error {
	pid, err := GetPidOnLinux("fctsdb")
	if err != nil {
		log.Println("Start db failed, error: " + err.Error())
		return err
	}
	if pid != "" {
		err = errors.New("you already have the same process")
		log.Println("Start db failed, error: " + err.Error())
		return err
	} else {
		cmd := `nohup ` + f.fctsdbPath + ` -config ` + f.configPath + ` 1>/dev/null 2>&1 &`
		log.Println(cmd)
		execCmd := exec.Command("bash", "-c", cmd)
		execCmd.SysProcAttr = &syscall.SysProcAttr{
			Pgid:    0,
			Setpgid: true,
		}
		err = execCmd.Run()
		if err != nil {
			log.Println("Start falconTSDB failed, error:" + err.Error())
			return err
		}
		log.Println("Start falconTSDB succeed")
	}
	return nil
}

func CheckFalconTSDB(addr string) error {
	for i := 0; i < 12; i++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		time.Sleep(5 * time.Second)
		if err != nil {
			log.Println("FalconTSDB is not started, error: " + err.Error())
		} else {
			log.Println("FalconTSDB is started")
			conn.Close()
			return nil
		}
	}
	return errors.New("FalconTSDB is not started in 60s")
}

func KillOnLinux(pid string) error {
	cmd := `kill -9 ` + pid
	log.Println("Running linux cmd :" + cmd)
	_, err := exec.Command("bash", "-c", cmd).Output()
	return err
}

//find Pid according to server name
func GetPidOnLinux(serverName string) (string, error) {
	ps, err := process.Processes()
	if err != nil {
		log.Println(err.Error())
	}
	for _, p := range ps {
		name, _ := p.Name()
		if name == serverName {
			return strconv.Itoa(int(p.Pid)), nil
		}
	}
	// cmd := `ps ux | awk '/` + serverName + `/ && !/awk/ {print $2}'`
	// log.Println("Running linux cmd :" + cmd)
	// res, err := exec.Command("bash", "-c", cmd).Output()
	// if err != nil {
	// 	return "", err
	// }
	return "", err
}

func RemoveFolder(folder string) error {
	cmd := `rm ` + folder + ` -rf`

	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		log.Printf("Remove folder(%s) failed, error: %s\n", folder, err.Error())
		return err
	}
	log.Printf("Remove folder(%s) succeed\n", folder)
	return nil
}

func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// ParseDuration parses a time duration from a string.
// This is needed instead of time.ParseDuration because this will support
// the full syntax that fql supports for specifying durations
// including weeks and days.
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
