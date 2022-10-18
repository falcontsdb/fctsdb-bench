package agent

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"

	"github.com/BurntSushi/toml"
)

//NewConfig returns a new dbconfig instance
type OpentsdbConfig struct {
	ContainerName string `toml:"container-name"`
	DataPath      string `toml:"data-path"`
}

func DecodeOpentsdbConfig(configFile string) (*OpentsdbConfig, error) {
	dbconfig := OpentsdbConfig{}
	if _, err := toml.DecodeFile(configFile, &dbconfig); err != nil {
		return nil, err
	}
	return &dbconfig, nil
}

type OpentsdbAgent struct {
	BinPath    string
	ConfigPath string

	//run var
	dbConfig          *OpentsdbConfig
	defaultBinPath    string
	defaultConfigPath string
	binName           string
}

func NewOpentsdbAgent(binPath, configPath string) *OpentsdbAgent {
	fa := &OpentsdbAgent{}
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

func (f *OpentsdbAgent) setBinaryPath(binPath string) error {
	_, err := GetOpentsdbVersion(binPath)
	if err != nil {
		return fmt.Errorf("can't run Opentsdb binary, error: %s", err.Error())
	}
	f.BinPath = binPath
	_, f.binName = path.Split(binPath)
	return nil
}

func (f *OpentsdbAgent) setConifgPath(configPath string) error {
	log.Println("Decode the Opentsdb config")
	dbConfig, err := DecodeOpentsdbConfig(configPath)
	if err != nil {
		return fmt.Errorf("decode Opentsdb config failed, error: %s", err.Error())
	}
	f.ConfigPath = configPath
	f.dbConfig = dbConfig
	return nil
}

func (f *OpentsdbAgent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *OpentsdbAgent) CleanHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *OpentsdbAgent) StopDBHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *OpentsdbAgent) SetHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *OpentsdbAgent) ResetHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *OpentsdbAgent) GetEnvHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		version, _ := GetOpentsdbVersion(f.BinPath)
		msg := getEnv()
		msg = append(msg, "**数据库版本: "...)
		msg = append(msg, version...)
		w.Write(msg)
		w.WriteHeader(http.StatusOK)
	}
}

func (f *OpentsdbAgent) CheckTelegrafHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		pid, err := GetPidOnLinux("telegraf")
		if err != nil {
			log.Println("error: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
		} else if pid == "" {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}
}

func (f *OpentsdbAgent) stopDB() error {

	//check that Opentsdb exists or not
	cmd := "docker rm " + f.dbConfig.ContainerName + " --force"
	log.Println("Running linux cmd :" + cmd)
	_, err := exec.Command("bash", "-c", cmd).Output()
	return err
}

func (f *OpentsdbAgent) cleanData() error {
	//clear data directory of database

	err := os.RemoveAll(f.dbConfig.DataPath)
	return err
}

func (f *OpentsdbAgent) startDB() error {
	cmd := `docker run --name ` + f.dbConfig.ContainerName + ` -d -v ` + f.dbConfig.DataPath + `:/data/hbase --net="host" opentsdb`
	log.Println("Running linux cmd :" + cmd)
	_, err := exec.Command("bash", "-c", cmd).Output()
	return err
}

func GetOpentsdbVersion(binPath string) ([]byte, error) {
	return []byte("2.4"), nil
}
