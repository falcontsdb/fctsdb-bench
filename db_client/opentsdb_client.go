package db_client

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var applicationJsonHeader = []byte("application/json")

// OpentsdbClient is a Writer that writes to a fctsdb HTTP server.
type OpentsdbClient struct {
	client   fasthttp.Client
	config   ClientConfig
	writeUrl []byte
	queryUrl []byte
	host     []byte
	buf      *bytes.Buffer
}

// NewOpentsdbClient returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewOpentsdbClient(c ClientConfig) *OpentsdbClient {
	var host []byte
	if c.Host[len(c.Host)-1] == '/' {
		host = []byte(c.Host)[:len(c.Host)-1]
	} else {
		host = []byte(c.Host)
	}

	// example: http://localhost:8086/api/put
	writeUrl := append(host, "/api/put"...)

	// example: http://localhost:8086/api/query
	queryUrl := append(host, "/api/query"...)

	return &OpentsdbClient{
		client: fasthttp.Client{
			Name:                "opentsdb",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},
		config:   c,
		queryUrl: queryUrl,
		writeUrl: writeUrl,
		host:     host,
		buf:      bytes.NewBuffer(make([]byte, 0, 8*1024)),
	}
}

// Write writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (f *OpentsdbClient) Write(body []byte) (int64, error) {

	log.Debug("Write body", string(body))
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(applicationJsonHeader)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(f.writeUrl)
	if f.config.Gzip > 0 {
		req.Header.Add("Content-Encoding", "gzip")
		compressedBatch := bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		fasthttp.WriteGzipLevel(compressedBatch, body, f.config.Gzip)
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

func (f *OpentsdbClient) Query(body []byte) (int64, error) {
	uri := fasthttp.AppendQuotedArg(f.queryUrl, body)
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(applicationJsonHeader)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)
	if f.config.Gzip > 0 {
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
			err = fmt.Errorf("invalid query response (status %d, db %s): %s", sc, f.config.Database, string(body))
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (f *OpentsdbClient) InitUser() error {
	return nil
}

func (f *OpentsdbClient) LoginUser() error {
	return nil
}

func (f *OpentsdbClient) CreateDatabase(name string, withEncryption bool) error {
	return nil
}

func (f *OpentsdbClient) CreateMeasurement(p *common.Point) error {
	return nil
}

func (f *OpentsdbClient) CheckConnection(timeout time.Duration) bool {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURI(fmt.Sprintf("%s/api/version", f.host))

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	clientWithTimeout := fasthttp.Client{ReadTimeout: time.Second, WriteTimeout: time.Second}

	endTime := time.Now().Add(timeout)
	log.Info("checking connection ")
	fmt.Print("checking .")
	defer fmt.Println()
	for time.Now().Before(endTime) {
		err := clientWithTimeout.Do(req, resp)
		if err == nil && resp.StatusCode() == fasthttp.StatusOK {
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
func (m *OpentsdbClient) BeforeSerializePoints(buf []byte, p *common.Point) []byte {
	return append(buf, '[')
}

func (s *OpentsdbClient) SerializeAndAppendPoint(buf []byte, p *common.Point) []byte {

	for i := 0; i < len(p.FieldKeys); i++ {
		var value float64
		switch x := p.FieldValues[i].(type) {
		case int:
			value = float64(x)
		case int64:
			value = float64(x)
		case float32:
			value = float64(x)
		case float64:
			value = x
		default:
			// panic("bad numeric value for OpenTSDB serialization")
		}

		buf = append(buf, []byte(`{"metric":"`)...)
		buf = append(buf, p.MeasurementName...)
		buf = append(buf, '.')
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, []byte(`","timestamp":`)...)
		buf = strconv.AppendInt(buf, p.Timestamp.UTC().UnixNano()/1e6, 10)
		buf = append(buf, []byte(`,"value":`)...)
		buf = strconv.AppendFloat(buf, value, 'f', 16, 64)
		buf = append(buf, []byte(`,"tags":{`)...)
		for i := 0; i < len(p.TagKeys); i++ {
			buf = append(buf, []byte(`"`)...)
			buf = append(buf, p.TagKeys[i]...)
			buf = append(buf, []byte(`":`)...)
			buf = append(buf, strings.ReplaceAll(strconv.QuoteToASCII(string(p.TagValues[i])), "\\", "")...)
			// buf = append(buf, []byte(`"`)...)
			if i+1 != len(p.TagValues) {
				buf = append(buf, ',')
			}
		}
		buf = append(buf, "}},\n"...)
	}
	for i := 0; i < len(p.Int64FiledKeys); i++ {

		value := float64(p.Int64FiledValues[i])
		buf = append(buf, []byte(`{"metric":"`)...)
		buf = append(buf, p.MeasurementName...)
		buf = append(buf, '.')
		buf = append(buf, p.Int64FiledKeys[i]...)
		buf = append(buf, []byte(`","timestamp":`)...)
		buf = strconv.AppendInt(buf, p.Timestamp.UTC().UnixNano()/1e6, 10)
		buf = append(buf, []byte(`,"value":`)...)
		buf = strconv.AppendFloat(buf, value, 'f', 16, 64)
		buf = append(buf, []byte(`,"tags":{`)...)
		for i := 0; i < len(p.TagKeys); i++ {
			buf = append(buf, []byte(`"`)...)
			buf = append(buf, p.TagKeys[i]...)
			buf = append(buf, []byte(`":"`)...)
			buf = append(buf, p.TagValues[i]...)
			buf = append(buf, []byte(`"`)...)
			if i+1 != len(p.TagValues) {
				buf = append(buf, ',')
			}
		}
		buf = append(buf, "}},\n"...)
	}
	return buf
}

func (m *OpentsdbClient) AfterSerializePoints(buf []byte, p *common.Point) []byte {
	buf = buf[:len(buf)-2]
	return append(buf, "]\n"...)
}
