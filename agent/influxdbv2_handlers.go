package agent

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

type InfluxdbV2Config struct {
	BindAddress string `toml:"bind-address"`
	DataPath    string `toml:"data-path"`
}

func DecodeInfluxdbV2Config(configFile string) (*InfluxdbV2Config, error) {
	dbconfig := InfluxdbV2Config{}
	if _, err := toml.DecodeFile(configFile, &dbconfig); err != nil {
		return nil, err
	}
	return &dbconfig, nil
}

//DecodeFromConfigFile generates a DbConfig by using the config file
type InfluxdbV2Agent struct {
	BinPath    string
	ConfigPath string

	//run var
	dbConfig          *InfluxdbV2Config
	defaultBinPath    string
	defaultConfigPath string
	binName           string
}

func NewInfluxdbV2Agent(binPath, configPath string) *InfluxdbV2Agent {
	fa := &InfluxdbV2Agent{}
	err := fa.setBinaryPath(binPath)
	if err != nil {
		log.Fatalln(err.Error())
	}
	fa.defaultBinPath = binPath
	err = fa.setConifgPath(configPath)
	if err != nil {
		log.Fatalln(err.Error())
	}
	fa.defaultConfigPath = configPath
	_, fa.binName = path.Split(binPath)
	return fa
}

func (f *InfluxdbV2Agent) setBinaryPath(binPath string) error {
	_, err := GetInfluxdbV2Version(binPath)
	if err != nil {
		return fmt.Errorf("can't run InfluxdbV2 binary, error: %s", err.Error())
	}
	f.BinPath = binPath
	_, f.binName = path.Split(binPath)
	return nil
}

func (f *InfluxdbV2Agent) setConifgPath(configPath string) error {
	log.Println("Decode the InfluxdbV2 config")
	dbConfig, err := DecodeInfluxdbV2Config(configPath)
	if err != nil {
		return fmt.Errorf("decode InfluxdbV2 config failed, error: %s", err.Error())
	}
	f.ConfigPath = configPath
	f.dbConfig = dbConfig
	return nil
}

func (f *InfluxdbV2Agent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Host)
	if r.Method == "GET" {
		err := f.startDB()
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

func (f *InfluxdbV2Agent) CleanHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := f.cleanData()
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

func (f *InfluxdbV2Agent) StopDBHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := f.stopDB()
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

func (f *InfluxdbV2Agent) SetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		binPath := r.URL.Query().Get("BinPath")
		if binPath != "" {
			err := f.setBinaryPath(binPath)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
		}
		configPath := r.URL.Query().Get("ConfigPath")
		if configPath != "" {
			err := f.setConifgPath(configPath)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func (f *InfluxdbV2Agent) ResetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := f.setBinaryPath(f.defaultBinPath)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		err = f.setConifgPath(f.defaultConfigPath)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func (f *InfluxdbV2Agent) GetEnvHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		version, _ := GetInfluxdbV2Version(f.BinPath)
		msg := getEnv()
		msg = append(msg, "**数据库版本: "...)
		msg = append(msg, version...)
		w.Write(msg)
		w.WriteHeader(http.StatusOK)
	}
}

func (f *InfluxdbV2Agent) CheckTelegrafHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		pid, err := GetPidOnLinux("telegraf")
		if err != nil {
			log.Println("error: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
		} else if pid == "" {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.Write([]byte("OK"))
			w.WriteHeader(http.StatusOK)
		}
	}
}

func (f *InfluxdbV2Agent) stopDB() error {
	pid, err := GetPidOnLinux(f.binName)
	if err != nil {
		log.Println("Get InfluxdbV2 failed, error: " + err.Error())
		return err
	}
	if err = KillOnLinux(pid); err != nil {
		log.Println("Kill InfluxdbV2 failed, error: " + err.Error())
		return err
	}

	//check that InfluxdbV2 exists or not
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		pid, _ = GetPidOnLinux(f.binName)
		if pid == "" {
			log.Println("Stop falconTSDB succeed")
			return nil
		}
	}

	err = errors.New("clean failed, there is another InfluxdbV2 running, pid is %s" + pid)
	log.Println(err.Error())
	return err
}

func (f *InfluxdbV2Agent) cleanData() error {
	//clear data directory of databas
	return os.RemoveAll(f.dbConfig.DataPath)
}

func (f *InfluxdbV2Agent) startDB() error {
	pid, err := GetPidOnLinux(f.binName)
	if err != nil {
		log.Println("Start db failed, error: " + err.Error())
		return err
	}
	if pid != "" {
		err = errors.New("you already have the same process")
		log.Println("Start db failed, error: " + err.Error())
		return err
	} else {
		boltPath := path.Join(f.dbConfig.DataPath, "influxd.bolt")
		enginePath := path.Join(f.dbConfig.DataPath, "engine")
		cmd := `nohup ` + f.BinPath + ` --bolt-path ` + boltPath + ` --engine-path ` + enginePath + ` --http-bind-address ` + f.dbConfig.BindAddress + ` 1>/dev/null 2>&1 &`
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

//find Pid according to server name
func GetInfluxdbV2Version(binPath string) ([]byte, error) {
	cmd := binPath + ` version`
	log.Println("Running linux cmd :" + cmd)
	return exec.Command("bash", "-c", cmd).Output()
}
