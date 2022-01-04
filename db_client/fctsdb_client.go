package db_client

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"github.com/valyala/fasthttp"
)

const DefaultIdleConnectionTimeout = 90 * time.Second

var (
	ErrorBackoff        error  = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  []byte = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  []byte = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a []byte = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b []byte = []byte("i/o timeout")
	backoffMagicWords3  []byte = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  []byte = []byte("timeout")
	backoffMagicWords5  []byte = []byte("write failed: can not exceed max connections of 500")
)

// FctsdbClient is a Writer that writes to a fctsdb HTTP server.
type FctsdbClient struct {
	client   fasthttp.Client
	c        common.ClientConfig
	writeUrl []byte
	queryUrl []byte
	host     []byte
	buf      *bytes.Buffer
}

// NewFctsdbClient returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewFctsdbClient(c common.ClientConfig) *FctsdbClient {
	var host []byte
	writeUrl := make([]byte, 0)
	if c.Host[len(c.Host)-1] == '/' {
		host = []byte(c.Host)[:len(c.Host)-1]
	} else {
		host = []byte(c.Host)
	}
	writeUrl = append(writeUrl, host...)
	writeUrl = append(writeUrl, "/write?db="...)
	writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Database))
	if c.User != "" {
		writeUrl = append(writeUrl, "&u="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.User))
		writeUrl = append(writeUrl, "&p="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Password))
	}

	queryUrl := make([]byte, 0)
	queryUrl = append(queryUrl, c.Host...)
	queryUrl = append(queryUrl, "/query?db="...)
	queryUrl = fasthttp.AppendQuotedArg(queryUrl, []byte(c.Database))
	if c.User != "" {
		writeUrl = append(writeUrl, "&u="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.User))
		writeUrl = append(writeUrl, "&p="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Password))
	}
	queryUrl = append(queryUrl, "&q="...)

	return &FctsdbClient{
		client: fasthttp.Client{
			Name:                "bulk_load_influx",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},
		c:        c,
		queryUrl: queryUrl,
		writeUrl: writeUrl,
		host:     host,
		buf:      bytes.NewBuffer(make([]byte, 0, 8*1024)),
	}
}

var (
	post                = []byte("POST")
	get                 = []byte("GET")
	textPlain           = []byte("text/plain")
	responseMustContain = []byte(`"time"`)
)

// Write writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *FctsdbClient) Write(body []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.writeUrl)
	if w.c.Gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}
	// fmt.Println(string(body))
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		// fmt.Println("status code ", sc)
		if sc == 500 && backpressurePred(resp.Body()) {
			err = ErrorBackoff
			log.Printf("backoff suggested, reason: %s", resp.Body())
		} else if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (w *FctsdbClient) Query(lines []byte) (int64, error) {
	uri := fasthttp.AppendQuotedArg(w.queryUrl, lines)
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)
	if w.c.Gzip {
		req.Header.Add("Accept-Encoding", "gzip")
	}
	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		var body []byte
		if string(resp.Header.Peek("Content-Encoding")) == "gzip" {
			_, err := fasthttp.WriteGunzip(w.buf, resp.Body())
			if err != nil {
				log.Printf("[ParseGzip] NewReader error: %v, maybe data is ungzip\n", err)
			}
			body = w.buf.Bytes()
			w.buf.Reset()
		} else {
			body = resp.Body()
		}
		if sc != fasthttp.StatusOK {
			err = fmt.Errorf("[DebugInfo: %s] Invalid query response (status %d): %s", w.c.DebugInfo, sc, string(body))
		} else {
			if !bytes.Contains(body, responseMustContain) {
				err = fmt.Errorf("[DebugInfo: %s] Invalid query response (status %d): %s", w.c.DebugInfo, sc, string(body))
			}
		}
		if w.c.Debug {
			fmt.Println(string(uri))
			var r Response
			err := json.Unmarshal(body, &r)
			if err != nil {
				log.Println("unmarshal response error", err.Error())
				log.Println(string(body))
			} else {
				if len(r.Results) == 0 {
					log.Println("result is 0:", string(uri))
				} else {
					if len(r.Results[0].Series) == 0 {
						log.Println("row is 0:", string(uri))
					}
				}
			}
			// fmt.Fprintln(os.Stdout, string(resp.Body()))
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (d *FctsdbClient) CreateDb(withEncryption bool) error {
	u, _ := url.Parse(string(d.host))

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	if withEncryption {
		v.Set("q", fmt.Sprintf("CREATE DATABASE %s with encryption on", d.c.Database))
	} else {
		v.Set("q", fmt.Sprintf("CREATE DATABASE %s", d.c.Database))
	}
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("createDb returned status code: %v", resp.StatusCode)
	}
	return nil
}

// listDatabases lists the existing databases in InfluxDB.
func (d *FctsdbClient) ListDatabases() ([]string, error) {

	u := fmt.Sprintf("%s/query?q=show%%20databases", d.host)

	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases get error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listDatabases returned status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("listDatabases readAll error: %s", err.Error())
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	// {"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]} for 1.8.4
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]interface{}
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, fmt.Errorf("listDatabases unmarshal error: %s", err.Error())
	}

	ret := make([]string, 0)
	for _, nestedName := range listing.Results[0].Series[0].Values {
		ret = append(ret, nestedName[0].(string))
	}
	return ret, nil
}

func (d *FctsdbClient) Ping() error {
	u := fmt.Sprintf("%s/ping", d.host)
	req, err := http.NewRequest(string(get), u, nil)
	if err != nil {
		return err
	}
	client := http.Client{
		Timeout: time.Second * 2,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("ping response statues is %d, not 204", resp.StatusCode)
	}
	return nil
}

func backpressurePred(body []byte) bool {
	if bytes.Contains(body, backoffMagicWords0) {
		return true
	} else if bytes.Contains(body, backoffMagicWords1) {
		return true
	} else if bytes.Contains(body, backoffMagicWords2a) && bytes.Contains(body, backoffMagicWords2b) {
		return true
	} else if bytes.Contains(body, backoffMagicWords3) {
		return true
	} else if bytes.Contains(body, backoffMagicWords4) {
		return true
	} else if bytes.Contains(body, backoffMagicWords5) {
		return true
	} else {
		return false
	}
}

type Response struct {
	Results []Result
	Err     string `json:"error,omitempty"`
}

// Message represents a user message.
type Message struct {
	Level string
	Text  string
}

// Result represents a resultset returned from a single statement.
type Result struct {
	StatementId int `json:"statement_id"`
	Series      []Row
	Messages    []*Message
	Err         string `json:"error,omitempty"`
}

type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Partial bool              `json:"partial,omitempty"`
}
