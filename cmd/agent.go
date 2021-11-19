package main

import (
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"git.querycap.com/falcontsdb/fctsdb-bench/agent"
	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"github.com/spf13/cobra"
)

type AgentConfig struct {
	port       string
	fctsdbPath string
	configPath string
	format     string

	//run var
	dbConfig *agent.DbConfig
}

var (
	agentCmd = &cobra.Command{
		Use:   "agent",
		Short: "代理程序，和数据库运行在一起，支持被远程调用开启关闭数据库（开发团队内部使用）",
		Run: func(cmd *cobra.Command, args []string) {
			RunAsAgent()
			// GetPidOnLinux("fctsdb")
		},
		Hidden: !FullFunction, // 隐藏此命令，不对外使用，内部测试使用
	}

	a           common.Agent
	agentConfig AgentConfig
)

func init() {
	Init(agentCmd)
	rootCmd.AddCommand(agentCmd)
}
func StartRemoteDatabase(endpoint string) error {
	_, err := httpGet(endpoint, agent.STARTPATH)
	return err
}
func StopRemoteDatabase(endpoint string) error {
	_, err := httpGet(endpoint, agent.STOPPATH)
	return err
}
func CleanRemoteDatabase(endpoint string) error {
	_, err := httpGet(endpoint, agent.CLEANPATH)
	return err
}
func RestartRemoteDatabase(endpoint string) error {
	_, err := httpGet(endpoint, agent.RESTARTPATH)
	return err
}
func GetEnvironment(endpoint string) ([]byte, error) {
	return httpGet(endpoint, agent.GETENVPATH)
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
	err := Validate()
	if err != nil {
		return
	}
	ListenAndServe()
}

func Init(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(&agentConfig.port, "port", "8966", "监听端口")
	flags.StringVar(&agentConfig.fctsdbPath, "fctsdb-path", "./fctsdb", "数据库二进制文件地址")
	flags.StringVar(&agentConfig.configPath, "fctsdb-config", "./config", "数据库config文件地址")
	flags.StringVar(&agentConfig.format, "format", "fctsdb", "数据库的类型，当前仅支持fctsdb和mysql，默认为fctsdb")
}

func Validate() error {
	if agentConfig.format == "mysql" && (agentConfig.fctsdbPath != "" || agentConfig.configPath != "") {
		return errors.New("do not set format as mysql and set fctsdbpath or fctsdb-config in the same time")
	}
	if agentConfig.format == "fctsdb" && (agentConfig.fctsdbPath == "" || agentConfig.configPath == "") {
		return errors.New("set format as fctsdb and set fctsdbpath or fctsdb-config in the same time")
	}
	switch agentConfig.format {
	case "mysql":
		a = &agent.MysqlAgent{}
		log.Println("start with mysql format")
	case "fctsdb":
		log.Println("start with fctsdb format")
		a = &agent.FctsdbAgent{
			Port:       agentConfig.port,
			FctsdbPath: agentConfig.fctsdbPath,
			ConfigPath: agentConfig.configPath,
			DbConfig:   agentConfig.dbConfig,
		}
	}

	a.ParseConfig()
	a.StartDB()
	return nil
}

func ListenAndServe() {
	http.HandleFunc(agent.CLEANPATH, a.CleanHandler)
	http.HandleFunc(agent.STARTPATH, a.StartDBHandler)
	http.HandleFunc(agent.STOPPATH, a.StopDBHandler)
	http.HandleFunc(agent.RESTARTPATH, a.RestartDBHandler)
	http.HandleFunc(agent.GETENVPATH, a.GetEnvHandler)
	log.Println("Start service 0.0.0.0:" + agentConfig.port)
	err := http.ListenAndServe("0.0.0.0:"+agentConfig.port, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
}
