package agent

import (
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"path"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/db_client"
	"github.com/BurntSushi/toml"
)

type MatrixdbConfig struct {
	Table string `toml:"table"`
}

func DecodeMatrixdbConfig(configFile string) (*MatrixdbConfig, error) {
	dbconfig := MatrixdbConfig{}
	if _, err := toml.DecodeFile(configFile, &dbconfig); err != nil {
		return nil, err
	}
	return &dbconfig, nil
}

//DecodeFromConfigFile generates a DbConfig by using the config file
type MatrixdbAgent struct {
	BinPath    string
	ConfigPath string

	//run var
	dbConfig          *MatrixdbConfig
	defaultBinPath    string
	defaultConfigPath string
	binName           string
}

func NewMatrixdbAgent(binPath, configPath string) *MatrixdbAgent {
	fa := &MatrixdbAgent{}
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

func (f *MatrixdbAgent) setBinaryPath(binPath string) error {
	_, err := GetInfluxdbV2Version(binPath)
	if err != nil {
		return fmt.Errorf("can't run InfluxdbV2 binary, error: %s", err.Error())
	}
	f.BinPath = binPath
	_, f.binName = path.Split(binPath)
	return nil
}

func (f *MatrixdbAgent) setConifgPath(configPath string) error {
	log.Println("Decode the InfluxdbV2 config")
	dbConfig, err := DecodeMatrixdbConfig(configPath)
	if err != nil {
		return fmt.Errorf("decode InfluxdbV2 config failed, error: %s", err.Error())
	}
	f.ConfigPath = configPath
	f.dbConfig = dbConfig
	return nil
}

func (f *MatrixdbAgent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
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
		log.Println("HTTP: " + "start matrixdb successful")
	}
}

func (f *MatrixdbAgent) CleanHandler(w http.ResponseWriter, r *http.Request) {
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
		log.Println("HTTP: clean matrixdb successful")
	}
}

func (f *MatrixdbAgent) StopDBHandler(w http.ResponseWriter, r *http.Request) {
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
		log.Println("HTTP: stop matrixdb successful")
	}
}

func (f *MatrixdbAgent) SetHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *MatrixdbAgent) ResetHandler(w http.ResponseWriter, r *http.Request) {
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

func (f *MatrixdbAgent) StartMxgateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		f.startMxgate()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func (f *MatrixdbAgent) GetEnvHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		version, _ := GetMatrixdbVersion(f.BinPath)
		msg := getEnv()
		msg = append(msg, "**数据库版本: "...)
		msg = append(msg, version...)
		w.Write(msg)
		w.WriteHeader(http.StatusOK)
	}
}

func (f *MatrixdbAgent) CheckTelegrafHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		w.Write([]byte("OK"))
		w.WriteHeader(http.StatusOK)
	}
}

func (f *MatrixdbAgent) stopDB() error {
	cmd := fmt.Sprintf("%s stop -f", f.BinPath)
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		fmt.Println("mxgate start error:", err.Error())
	}
	return nil
}

func (f *MatrixdbAgent) startMxgate() {
	cmd := fmt.Sprintf("%s start --config %s", f.BinPath, f.ConfigPath)
	info, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		fmt.Println("mxgate start error:", err.Error(), string(info))
	}
}

func (f *MatrixdbAgent) cleanData() error {
	//clear data directory of databas

	info, err := exec.Command("bash", "-c", "gpstop -ar -M fast").Output()
	if err != nil {
		fmt.Println("gpstop restart error:", err.Error(), string(info))
	}

	client := db_client.NewMatrixdbClient(db_client.ClientConfig{
		Host:     "localhost",
		User:     "mxadmin",
		Password: "Abc_123456"})
	err = client.DropDatabase("benchmark_db")
	time.Sleep(time.Second * 10)
	return err
}

func (f *MatrixdbAgent) startDB() error {
	// cmd := `gpstart -a`
	// _, err := exec.Command("bash", "-c", cmd).Output()
	// return err
	return nil
}

//find Pid according to server name
func GetMatrixdbVersion(binPath string) ([]byte, error) {
	cmd := binPath + ` version`
	log.Println("Running linux cmd :" + cmd)
	return exec.Command("bash", "-c", cmd).Output()
}
