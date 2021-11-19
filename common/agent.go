package common

import (
	"net/http"
)

type Agent interface {
	StartDBHandler(w http.ResponseWriter, r *http.Request)
	CleanHandler(w http.ResponseWriter, r *http.Request)
	StopDBHandler(w http.ResponseWriter, r *http.Request)
	RestartDBHandler(w http.ResponseWriter, r *http.Request)
	GetEnvHandler(w http.ResponseWriter, r *http.Request)
	RestartDB(deleteData bool) error
	StartDB() error
	StopDB() error
	ParseConfig()
}
