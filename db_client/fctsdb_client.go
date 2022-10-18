package db_client

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const DefaultIdleConnectionTimeout = 90 * time.Second

var (
	post                = []byte("POST")
	get                 = []byte("GET")
	textPlain           = []byte("text/plain")
	responseMustContain = []byte(`"time"`)
)

// FctsdbClient is a Writer that writes to a fctsdb HTTP server.
type FctsdbClient struct {
	client   fasthttp.Client
	c        ClientConfig
	writeUrl []byte
	queryUrl []byte
	host     []byte
	buf      *bytes.Buffer
}

// NewFctsdbClient returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewFctsdbClient(c ClientConfig) *FctsdbClient {
	var host []byte
	writeUrl := make([]byte, 0)
	queryUrl := make([]byte, 0)
	if c.Host != "" {
		if c.Host[len(c.Host)-1] == '/' {
			host = []byte(c.Host)[:len(c.Host)-1]
		} else {
			host = []byte(c.Host)
		}

		// example: http://localhost:8086/write?db=db&u=user&p=password
		writeUrl = append(writeUrl, host...)
		writeUrl = append(writeUrl, "/write?db="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Database))
		if c.User != "" {
			writeUrl = append(writeUrl, "&u="...)
			writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.User))
			writeUrl = append(writeUrl, "&p="...)
			writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Password))
		}

		// example: http://localhost:8086/query?db=db&u=user&p=password&q=select * from cpu
		queryUrl = append(queryUrl, host...)
		queryUrl = append(queryUrl, "/query?db="...)
		queryUrl = fasthttp.AppendQuotedArg(queryUrl, []byte(c.Database))
		if c.User != "" {
			writeUrl = append(writeUrl, "&u="...)
			writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.User))
			writeUrl = append(writeUrl, "&p="...)
			writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Password))
		}
		queryUrl = append(queryUrl, "&q="...)
	}
	return &FctsdbClient{
		client: fasthttp.Client{
			Name:                "fctsdb",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},
		c:        c,
		queryUrl: queryUrl,
		writeUrl: writeUrl,
		host:     host,
		buf:      bytes.NewBuffer(make([]byte, 0, 8*1024)),
	}
}

// Write writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (f *FctsdbClient) Write(body []byte) (int64, error) {
	log.Debug("Write body", string(body))
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(f.writeUrl)
	if f.c.Gzip > 0 {
		req.Header.Add("Content-Encoding", "gzip")
		compressedBatch := bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		fasthttp.WriteGzipLevel(compressedBatch, body, f.c.Gzip)
		req.SetBody(compressedBatch.Bytes())
	} else {
		req.SetBody(body)
	}

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := f.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("invalid write response (status %d): %s", sc, string(resp.Body()))
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (f *FctsdbClient) Query(body []byte) (int64, error) {
	uri := fasthttp.AppendQuotedArg(f.queryUrl, body)
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)
	if f.c.Gzip > 0 {
		req.Header.Add("Accept-Encoding", "gzip")
	}

	log.Debug("Query url:", string(uri))

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := f.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		var body []byte
		if string(resp.Header.Peek("Content-Encoding")) == "gzip" {
			_, err := fasthttp.WriteGunzip(f.buf, resp.Body())
			if err != nil {
				log.Errorf("[ParseGzip] NewReader error: %v, maybe data is ungzip\n", err)
			}
			body = f.buf.Bytes()
			f.buf.Reset()
		} else {
			body = resp.Body()
		}

		log.Debug("Query response body", string(body))

		if sc != fasthttp.StatusOK || !bytes.Contains(body, responseMustContain) {
			err = fmt.Errorf("invalid query response (status %d, db %s): %s", sc, f.c.Database, string(body))
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (f *FctsdbClient) otherQuery(body []byte) (int, []byte, error) {
	uri := fasthttp.AppendQuotedArg(f.queryUrl, body)
	log.Debug(string(uri))
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	err := f.client.Do(req, resp)
	if err != nil {
		return 0, nil, err
	}
	code := resp.StatusCode()
	respBody := resp.Body()

	return code, respBody, nil
}

func (f *FctsdbClient) InitUser() error {
	return nil
}

func (f *FctsdbClient) LoginUser() error {
	return nil
}

func (f *FctsdbClient) CreateDatabase(name string, withEncryption bool) error {

	log.Infof("create database %s", name)
	existingDatabases, err := f.listDatabases()
	if err != nil {
		return err
	}

	for _, existingDatabase := range existingDatabases {
		if name == existingDatabase {
			log.Warnf("The following database \"%s\" already exist in the data store, do'not need create.", name)
			return nil
		}
	}

	var statusCode int
	var response []byte

	if withEncryption {
		statusCode, response, err = f.otherQuery([]byte(fmt.Sprintf("CREATE DATABASE %s with encryption on", f.c.Database)))
	} else {
		statusCode, response, err = f.otherQuery([]byte(fmt.Sprintf("CREATE DATABASE %s", f.c.Database)))
	}
	if err != nil {
		return fmt.Errorf("create database error: %s", err.Error())
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("create database returned status code: %d, body: %s", statusCode, string(response))
	}
	return nil
}

// listDatabases lists the existing databases in InfluxDB.
func (f *FctsdbClient) listDatabases() ([]string, error) {

	statusCode, response, err := f.otherQuery([]byte("SHOW DATABASES"))
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("list databases returned status code: %v", statusCode)
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
	err = json.Unmarshal(response, &listing)
	if err != nil {
		return nil, fmt.Errorf("list databases unmarshal error: %s", err.Error())
	}

	databases := make([]string, 0)
	for _, nestedName := range listing.Results[0].Series[0].Values {
		databases = append(databases, nestedName[0].(string))
	}

	log.Info("The following databases already exist in the data store: ", strings.Join(databases, ", "))
	return databases, nil
}

func (f *FctsdbClient) CreateMeasurement(p *common.Point) error {
	return nil
}

func (f *FctsdbClient) CheckConnection(timeout time.Duration) bool {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURI(fmt.Sprintf("%s/ping", f.host))

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	clientWithTimeout := fasthttp.Client{ReadTimeout: time.Second, WriteTimeout: time.Second}

	endTime := time.Now().Add(timeout)
	log.Info("checking connection ")
	fmt.Print("checking .")
	defer fmt.Println()
	for time.Now().Before(endTime) {
		err := clientWithTimeout.Do(req, resp)
		if err == nil && resp.StatusCode() == fasthttp.StatusNoContent {
			return true
		}
		time.Sleep(2 * time.Second)
		fmt.Print(".")
	}
	return false
}

// SerializeInfluxBulk writes Point data to the given writer, conforming to the
// InfluxDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
//
func (m *FctsdbClient) BeforeSerializePoints(buf []byte, p *common.Point) []byte {
	return buf
}

func (s *FctsdbClient) SerializeAndAppendPoint(buf []byte, p *common.Point) []byte {
	// buf := make([]byte, 0, 4*1024)
	buf = append(buf, p.MeasurementName...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, p.TagValues[i]...)
	}

	if len(p.FieldKeys)+len(p.Int64FiledKeys) > 0 {
		buf = append(buf, ' ')
	}

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, '=')

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)

		// Influx uses 'i' to indicate integers:
		switch v.(type) {
		case int, int64:
			buf = append(buf, 'i')
		}

		if i+1 < len(p.FieldKeys) {
			buf = append(buf, ',')
		}
	}

	if i > 0 && len(p.Int64FiledKeys) > 0 {
		buf = append(buf, ',')
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		buf = append(buf, p.Int64FiledKeys[i]...)
		buf = append(buf, '=')

		v := p.Int64FiledValues[i]
		buf = strconv.AppendInt(buf, v, 10)
		// Influx uses 'i' to indicate integers:
		buf = append(buf, 'i')
		if i+1 < len(p.Int64FiledKeys) {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, ' ')
	buf = fastFormatAppend(p.Timestamp.UTC().UnixNano(), buf, true)
	buf = append(buf, '\n')

	return buf
}

func (m *FctsdbClient) AfterSerializePoints(buf []byte, p *common.Point) []byte {
	return buf
}
