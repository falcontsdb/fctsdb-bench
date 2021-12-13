package agent

import (
	"log"
	"net/http"
)

type MysqlAgent struct {
}

func NewMysqlAgent() *MysqlAgent {
	return &MysqlAgent{}
}

func (m MysqlAgent) SetHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (m MysqlAgent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Host)
	if r.Method == "GET" {
		err := m.startDB()
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: " + "start mysql successful")
	}
}

func (m MysqlAgent) CleanHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := m.cleanData()
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: clean mysql successful")
	}
}

func (m MysqlAgent) StopDBHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := m.stopDB()
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: stop mysql successful")
	}
}

func (m MysqlAgent) GetEnvHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		w.Write(getEnv())
		w.WriteHeader(http.StatusOK)
	}
}

func (m MysqlAgent) cleanData() error {
	return nil
}

func (m MysqlAgent) startDB() error {
	return nil
}

func (m MysqlAgent) stopDB() error {
	return nil
}

func (m MysqlAgent) ParseConfig() {
}