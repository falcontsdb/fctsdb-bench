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
	return err
}
func StopRemoteDatabase(endpoint string) error {
	log.Println("stop the remote database")
	_, err := httpGet(endpoint, AGENT_STOP_PATH)
	return err
}
func CleanRemoteDatabase(endpoint string) error {
	log.Println("clean the remote database data")
	_, err := httpGet(endpoint, AGENT_CLEAN_PATH)
	return err
}
func GetEnvironment(endpoint string) ([]byte, error) {
	return httpGet(endpoint, AGENT_GET_ENV_PATH)
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
		log.Fatal("Invalid agent address:", endpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("Invalid agent address:", endpoint)
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
	return err
}
