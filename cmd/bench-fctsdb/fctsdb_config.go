package main

import "github.com/BurntSushi/toml"

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
