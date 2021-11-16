package agent

import (
	"errors"
	"github.com/BurntSushi/toml"
	"github.com/shirou/gopsutil/process"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

const (
	CLEANPATH   = "/clean"
	STARTPATH   = "/start"
	STOPPATH    = "/stop"
	RESTARTPATH = "/restart"
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
	Port       string
	FctsdbPath string
	ConfigPath string

	//run var
	DbConfig *DbConfig
}

func (f *FctsdbAgent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Host)
	if r.Method == "GET" {
		err := f.StartDB()
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
		err := f.RestartDB(true)
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
		err := f.StopDB()
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

func (f *FctsdbAgent) RestartDBHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := f.RestartDB(false)
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

func (f *FctsdbAgent) RestartDB(deleteData bool) error {
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
				for _, dir := range []string{f.DbConfig.Meta.Dir, f.DbConfig.Data.Dir, f.DbConfig.Data.SnapshotDir, f.DbConfig.Data.WalDir} {
					err := os.RemoveAll(dir)
					if err != nil {
						return err
					}
				}
			}
			return f.StartDB()
		}
	}

	err = errors.New("clean failed, there is another fctsdb running, pid is %s" + pid)
	log.Println(err.Error())
	return err
}

func (f *FctsdbAgent) StartDB() error {
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
		cmd := `nohup ` + f.FctsdbPath + ` -config ` + f.ConfigPath + ` 1>/dev/null 2>&1 &`
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

func (f *FctsdbAgent) StopDB() error {
	pid, err := GetPidOnLinux("fctsdb")
	if err != nil {
		log.Println("stop db failed, error: " + err.Error())
		return err
	}
	if pid == "" {
		err = errors.New("no fctsdb database running,please make a check")
		log.Println("stop db failed, error: " + err.Error())
		return err
	}
	err = KillOnLinux(pid)
	if err != nil {
		return err
	}
	log.Println("Stop falconTSDB succeed")
	return nil

}

func (f *FctsdbAgent) ParseConfig() {
	var err error
	f.DbConfig, err = DecodeFromConfigFile(f.ConfigPath)
	if err != nil {
		log.Fatal("Decode fctsdb config failed, error:", err.Error())
	}
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
