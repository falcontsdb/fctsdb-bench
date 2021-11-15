package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/cobra"
)

type DbConfig struct {
	BindAddress  string             `toml:"bind-address"`
	Meta         MetaConfig         `toml:"meta"`
	Data         DataConfig         `toml:"data"`
	RpcServer    RpcServerConfig    `toml:"rpc-server"`
	OplogReplica OplogReplicaConfig `toml:"oplog-replica"`
	Coordinator  CoordinatorConfig  `toml:"coordinator"`
	Subscriber   SubscriberConfig   `toml:"subscriber"`
	Http         HttpConfig         `toml:"http"`
	Logging      LoggingConfig      `toml:"logging"`
	Tls          TlsConfig          `toml:"tls"`
	Security     SecurityConfig     `toml:"security"`
	License      LicenseConfig      `toml:"license"`
}
type MetaConfig struct {
	Dir        string `toml:"dir"`
	AutoCreate bool   `toml:"retention-autocreate"`
}
type DataConfig struct {
	Dir                string `toml:"dir"`
	SnapshotDir        string `toml:"snapshot-dir"`
	IndexVersion       string `toml:"index-version"`
	TimeIndexEnabled   bool   `toml:"series-time-index-enabled"`
	WalDir             string `toml:"wal-dir"`
	QueryLogEnabled    bool   `toml:"query-log-enabled"`
	LazyLoadingEnabled bool   `toml:"lazy-loading-enabled"`
	HotShards          int    `toml:"hot-shards"`
}
type RpcServerConfig struct {
	BindAddress string `toml:"bind-address"`
}

type OplogReplicaConfig struct {
	Role             string `toml:"role"`
	ReplicaEnabled   bool   `toml:"replica-enabled"`
	RemoteServerAddr string `toml:"remote-server-addr"`
	AuthEnabled      bool   `toml:"auth-enabled"`
	Certificate      string `toml:"certificate"`
	PrivateKey       string `toml:"private-key"`
}
type CoordinatorConfig struct {
	LogQueriesAfter    string `toml:"log-queries-after"`
	SlowQueriesLogFile string `toml:"slow-queries-log-file"`
}
type SubscriberConfig struct {
	InsecureSkipVerify bool   `toml:"insecure-skip-verify"`
	CaCerts            string `toml:"ca-certs"`
}

type HttpConfig struct {
	BindAddress           string `toml:"bind-address"`
	PprofEnable           bool   `toml:"pprof-enabled"`
	PprofBindAddress      string `toml:"pprof-bind-address"`
	LogEnabled            bool   `toml:"log-enabled"`
	SuppressWriteLog      bool   `toml:"suppress-write-log"`
	HttpsEnabled          bool   `toml:"https-enabled"`
	HttpsCertificate      string `toml:"https-certificate"`
	HttpsPrivateKey       string `toml:"https-private-key"`
	UnixSocketEnabled     bool   `toml:"unix-socket-enabled"`
	UnixSocketPermissions string `toml:"unix-socket-permissions"`
	BindSocket            string `toml:"bind-socket"`
	AccessLogPath         string `toml:"access-log-path"`
}

type LoggingConfig struct {
	Format          string `toml:"format"`
	Level           string `toml:"level"`
	SuppressLogo    bool   `toml:"suppress-logo"`
	EnableLogToFile bool   `toml:"enable-log-to-file"`
	Filename        string `toml:"filename"`
	MaxSize         int    `toml:"max-size"`
	MaxDays         int    `toml:"max-days"`
	MaxBackups      int    `toml:"max-backups"`
}

type TlsConfig struct {
	MinVersion string `toml:"min-version"`
	MaxVersion string `toml:"max-version"`
}

type SecurityConfig struct {
	SafeModeEnabled bool `toml:"safe-mode-enabled"`
}

type LicenseConfig struct {
	LicensePath string `toml:"license-path"`
}

//NewConfig returns a new dbconfig instance
func NewConfig() *DbConfig {
	dbconfig := &DbConfig{}
	dbconfig.Meta = MetaConfig{}
	dbconfig.Data = DataConfig{}
	dbconfig.RpcServer = RpcServerConfig{}
	dbconfig.OplogReplica = OplogReplicaConfig{}
	dbconfig.Coordinator = CoordinatorConfig{}
	dbconfig.Subscriber = SubscriberConfig{}
	dbconfig.Http = HttpConfig{}
	dbconfig.Logging = LoggingConfig{}
	dbconfig.Tls = TlsConfig{}
	dbconfig.Security = SecurityConfig{}
	dbconfig.License = LicenseConfig{}
	return dbconfig
}

//DecodeFromConfigFile generates a DbConfig by using the config file
func DecodeFromConfigFile(configFile string) (*DbConfig, error) {
	dbconfig := NewConfig()
	if _, err := toml.DecodeFile(configFile, dbconfig); err != nil {
		return nil, err
	}
	return dbconfig, nil
}

type FctsdbAgent struct {
	port       string
	fctsdbPath string
	configPath string

	//run var
	dbConfig *DbConfig
}

var (
	dataWriteAgentCmd = &cobra.Command{
		Use:   "agent",
		Short: "代理程序，和数据库运行在一起，支持被远程调用开启关闭数据库（开发团队内部使用）",
		Run: func(cmd *cobra.Command, args []string) {
			RunAsAgent()
			// GetPidOnLinux("fctsdb")
		},
		Hidden: !FullFunction, // 隐藏此命令，不对外使用，内部测试使用
	}

	fctsdbWriteAgent = FctsdbAgent{}
	cleanPath        = "/clean"
	startPath        = "/start"
	stopPath         = "/stop"
)

func init() {
	fctsdbWriteAgent.Init(dataWriteAgentCmd)
	rootCmd.AddCommand(dataWriteAgentCmd)
}
func StartRemoteFalconTSDB(endpoint string) error {
	_, err := httpGet(endpoint, startPath)
	return err
}
func StopRemoteFalconTSDB(endpoint string) error {
	_, err := httpGet(endpoint, stopPath)
	return err
}
func CleanRemoteFalconTSDB(endpoint string) error {
	_, err := httpGet(endpoint, cleanPath)
	return err
}
func httpGet(endpoint, path string) ([]byte, error) {

	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatal("Invalid agent address:", endpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("Invalid agent address:", endpoint)
	}
	u.Path = path

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	rData, err := ioutil.ReadAll(resp.Body)
	return rData, err
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

	http.HandleFunc(cleanPath, f.CleanHandler)
	http.HandleFunc(startPath, f.StartDBHandler)
	http.HandleFunc(stopPath, f.StopDBHandler)
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
		err := f.CleanFalconTSDB(true)
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

func (f *FctsdbAgent) StopDBHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := f.CleanFalconTSDB(false)
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: stop falconTSDB successful")
	}
}

func (f *FctsdbAgent) CleanFalconTSDB(deleteData bool) error {
	pid, err := GetPidOnLinux("fctsdb")
	if err != nil {
		log.Println("Get fctsdb failed, error: " + err.Error())
		return err
	}
	if err = KillOnLinux(pid); err != nil {
		log.Println("Kill fctsdb failed, error: " + err.Error())
		return err
	}

	//check that fctsdb exists or not
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		pid, _ = GetPidOnLinux("fctsdb")
		if pid == "" {
			log.Println("Stop falconTSDB succeed")
			if deleteData {
				//clear data directory of database
				for _, dir := range []string{f.dbConfig.Meta.Dir, f.dbConfig.Data.Dir, f.dbConfig.Data.SnapshotDir, f.dbConfig.Data.WalDir} {
					err := os.RemoveAll(dir)
					if err != nil {
						return err
					}
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
