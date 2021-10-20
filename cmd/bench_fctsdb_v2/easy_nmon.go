package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"
)

//start monitor

func SendStartMonitorSignal(endpoint string, reportName string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatal("Invalid easy nmon address:", endpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("Invalid easy nmon address:", endpoint)
	}
	u.Path = "/start"
	q := u.Query()
	q.Set("n", reportName)
	q.Set("t", "86400")
	q.Set("f", "1")
	// q.Set("dn", dirName)
	u.RawQuery = q.Encode()
	log.Println(u.String())
	cli := http.Client{}
	resp, err := cli.Get(u.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

//stop monitor
func SendStopAllMonitorSignal(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatal("Invalid easy nmon address:", endpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("Invalid easy nmon address:", endpoint)
	}
	u.Path = "/stop"
	cli := http.Client{}
	resp, err := cli.Get(u.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

//check monitor serve running or not
func CheckMonitorServe(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	time.Sleep(5 * time.Second)
	if err != nil {
		return errors.New("monitor server is not started: " + err.Error())
	} else {
		log.Println("monitor server is started")
		conn.Close()
		return nil
	}
}
