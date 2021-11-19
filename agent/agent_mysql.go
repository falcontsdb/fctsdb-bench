package agent

import (
	"log"
	"net/http"
)

type MysqlAgent struct {
}

func (m MysqlAgent) StartDBHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Host)
	if r.Method == "GET" {
		err := m.StartDB()
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
		err := m.RestartDB(true)
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
		err := m.RestartDB(false)
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

func (m MysqlAgent) RestartDBHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		err := m.RestartDB(false)
		if err != nil {
			log.Println("HTTP: " + err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		log.Println("HTTP: restart mysql successful")
	}
}

func (m MysqlAgent) GetEnvHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		w.Write(getEnv())
		w.WriteHeader(http.StatusOK)
	}
}

func (m MysqlAgent) RestartDB(deleteData bool) error {
	return nil
}

func (m MysqlAgent) StartDB() error {
	return nil
}

func (m MysqlAgent) StopDB() error {
	return nil
}

func (m MysqlAgent) ParseConfig() {
}
