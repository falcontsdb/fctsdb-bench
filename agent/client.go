package agent

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

func StartRemoteDatabase(endpoint string) error {
	log.Println("start the remote database")
	_, err := httpGet(endpoint, AGENT_START_PATH)
	if err != nil {
		return fmt.Errorf("start remote database error: %s", err.Error())
	}
	return nil
}
func StopRemoteDatabase(endpoint string) error {
	log.Println("stop the remote database")
	_, err := httpGet(endpoint, AGENT_STOP_PATH)
	if err != nil {
		return fmt.Errorf("stop remote database error: %s", err.Error())
	}
	return nil
}
func CleanRemoteDatabase(endpoint string) error {
	log.Println("clean the remote database data")
	_, err := httpGet(endpoint, AGENT_CLEAN_PATH)
	if err != nil {
		return fmt.Errorf("clean remote database error: %s", err.Error())
	}
	return nil
}
func GetEnvironment(endpoint string) ([]byte, error) {
	resp, err := httpGet(endpoint, AGENT_GET_ENV_PATH)
	if err != nil {
		return nil, fmt.Errorf("get environment error: %s", err.Error())
	}
	return resp, nil
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
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return []byte{}, fmt.Errorf(string(body))
	}
	rData, err := ioutil.ReadAll(resp.Body)
	return rData, err
}

func SetAgent(endpoint string, param map[string]string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatal("set agent error: Invalid agent address:", endpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("set agent error: Invalid agent address:", endpoint)
	}
	u.Path = AGENT_SET_PATH
	q := u.Query()
	for key, value := range param {
		q.Add(key, value)
	}
	u.RawQuery = q.Encode()
	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(string(body))
	}
	return nil
}

func ResetAgent(endpoint string) error {
	_, err := httpGet(endpoint, AGENT_RESET_PATH)
	if err != nil {
		return fmt.Errorf("reset agent error: %s", err.Error())
	}
	return nil
}

func CheckTelegraf(endpoint string) {
	_, err := httpGet(endpoint, AGENT_CHECK_TELEGRAF)
	if err != nil {
		log.Fatalln("can not find telegraf, please check")
	}
}
