package agent

import (
	"errors"
	"log"
	"net/http"
)

const (
	AGENT_CLEAN_PATH     = "/clean"
	AGENT_START_PATH     = "/start"
	AGENT_STOP_PATH      = "/stop"
	AGENT_SET_PATH       = "/set" //参数BinPath, ConfigPath分别设置数据库的二进制文件路径和config路径
	AGENT_RESET_PATH     = "/reset"
	AGENT_GET_ENV_PATH   = "/env"
	AGENT_CHECK_TELEGRAF = "/telegraf"
)

type AgentHandlers interface {
	StartDBHandler(w http.ResponseWriter, r *http.Request)
	CleanHandler(w http.ResponseWriter, r *http.Request)
	StopDBHandler(w http.ResponseWriter, r *http.Request)
	GetEnvHandler(w http.ResponseWriter, r *http.Request)
	SetHandler(w http.ResponseWriter, r *http.Request)
	ResetHandler(w http.ResponseWriter, r *http.Request)
	CheckTelegrafHandler(w http.ResponseWriter, r *http.Request)
}

type AgentService struct {
	Port       string
	BinPath    string
	ConfigPath string
	Format     string

	//run var
	handlers AgentHandlers
}

func (s *AgentService) Validate() error {
	if s.Format == "mysql" && (s.BinPath != "" || s.ConfigPath != "") {
		return errors.New("do not set format as mysql and set fctsdbpath or fctsdb-config in the same time")
	}
	if s.Format == "fctsdb" && (s.BinPath == "" || s.ConfigPath == "") {
		return errors.New("set format as fctsdb and set fctsdbpath or fctsdb-config in the same time")
	}
	switch s.Format {
	case "mysql":
		s.handlers = NewMysqlAgent()
		log.Println("start with mysql format")
	case "fctsdb":
		log.Println("start with fctsdb format")
		s.handlers = NewFctsdbAgent(s.BinPath, s.ConfigPath)
	case "influxdbv2":
		log.Println("start with influxdbv2 format")
		s.handlers = NewInfluxdbV2Agent(s.BinPath, s.ConfigPath)
	case "matrixdb":
		log.Println("start with matrixdb format")
		handlers := NewMatrixdbAgent(s.BinPath, s.ConfigPath)
		s.handlers = handlers
		http.HandleFunc("/startMxgate", handlers.StartMxgateHandler)
	case "opentsdb":
		log.Println("start with opentsdb format")
		s.handlers = NewOpentsdbAgent(s.BinPath, s.ConfigPath)
	}
	return nil
}

func (s *AgentService) ListenAndServe() {
	err := s.Validate()
	if err != nil {
		log.Fatal(err.Error())
	}
	http.HandleFunc(AGENT_CLEAN_PATH, s.handlers.CleanHandler)
	http.HandleFunc(AGENT_START_PATH, s.handlers.StartDBHandler)
	http.HandleFunc(AGENT_STOP_PATH, s.handlers.StopDBHandler)
	http.HandleFunc(AGENT_SET_PATH, s.handlers.SetHandler)
	http.HandleFunc(AGENT_RESET_PATH, s.handlers.ResetHandler)
	http.HandleFunc(AGENT_GET_ENV_PATH, s.handlers.GetEnvHandler)
	http.HandleFunc(AGENT_CHECK_TELEGRAF, s.handlers.CheckTelegrafHandler)
	log.Println("Start service 0.0.0.0:" + s.Port)
	err = http.ListenAndServe("0.0.0.0:"+s.Port, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
}
