package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

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

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	Host     string
	Database string

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client   fasthttp.Client
	c        HTTPWriterConfig
	writeUrl []byte
	queryUrl []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig) *HTTPWriter {
	writeUrl := make([]byte, 0)
	if c.Host[len(c.Host)-1] == '/' {
		writeUrl = append(writeUrl, c.Host...)
		writeUrl = append(writeUrl, "write?db="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Database))
	} else {
		writeUrl = append(writeUrl, c.Host...)
		writeUrl = append(writeUrl, "/write?db="...)
		writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Database))
	}

	queryUrl := make([]byte, 0)
	if c.Host[len(c.Host)-1] == '/' {
		queryUrl = append(queryUrl, c.Host...)
		queryUrl = append(queryUrl, "query?db="...)
		queryUrl = fasthttp.AppendQuotedArg(queryUrl, []byte(c.Database))
		queryUrl = append(queryUrl, "&q="...)
	} else {
		queryUrl = append(queryUrl, c.Host...)
		queryUrl = append(queryUrl, "/query?db="...)
		queryUrl = fasthttp.AppendQuotedArg(queryUrl, []byte(c.Database))
		queryUrl = append(queryUrl, "&q="...)
	}

	return &HTTPWriter{
		client: fasthttp.Client{
			Name:                "bulk_load_influx",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},
		c:        c,
		queryUrl: queryUrl,
		writeUrl: writeUrl,
	}
}

var (
	post      = []byte("POST")
	get       = []byte("GET")
	textPlain = []byte("text/plain")
)

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte, isGzip, debug bool) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.writeUrl)
	if isGzip {
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
		if debug {
			fmt.Println(string(w.writeUrl))
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (w *HTTPWriter) QueryLineProtocol(lines []byte, isGzip, debug bool) (int64, error) {
	uri := fasthttp.AppendQuotedArg(w.queryUrl, lines)
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)
	if isGzip {
		req.Header.Add("Accept-Encoding", "gzip")
	}
	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc == 500 && backpressurePred(resp.Body()) {
			err = ErrorBackoff
			log.Printf("backoff suggested, reason: %s", resp.Body())
		} else if sc != fasthttp.StatusOK {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
		if debug {
			fmt.Println(string(uri))
			fmt.Fprintln(os.Stdout, string(resp.Body()))
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
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
