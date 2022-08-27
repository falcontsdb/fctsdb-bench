package db_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/valyala/fasthttp"
)

var organization = "benchmark"

// InfluxdbV2Client is a Writer that writes to a fctsdb HTTP server.
type InfluxdbV2Client struct {
	client   fasthttp.Client
	c        ClientConfig
	writeUrl []byte
	queryUrl []byte
	host     []byte
	buf      *bytes.Buffer
	token    string
}

// NewFctsdbClient returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewInfluxdbV2Client(c ClientConfig) *InfluxdbV2Client {
	var host []byte
	writeUrl := make([]byte, 0)
	if c.Host[len(c.Host)-1] == '/' {
		host = []byte(c.Host)[:len(c.Host)-1]
	} else {
		host = []byte(c.Host)
	}
	writeUrl = append(writeUrl, host...)
	writeUrl = append(writeUrl, "/api/v2/write?bucket="...)
	writeUrl = fasthttp.AppendQuotedArg(writeUrl, []byte(c.Database))
	writeUrl = append(writeUrl, "&org="...)
	writeUrl = append(writeUrl, organization...)
	writeUrl = append(writeUrl, "&precision=ns"...)

	queryUrl := make([]byte, 0)
	queryUrl = append(queryUrl, host...)
	queryUrl = append(queryUrl, "/query?db="...)
	queryUrl = fasthttp.AppendQuotedArg(queryUrl, []byte(c.Database))
	queryUrl = append(queryUrl, "&q="...)

	return &InfluxdbV2Client{
		client: fasthttp.Client{
			Name:                "influxdbv2",
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
func (w *InfluxdbV2Client) Write(body []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.writeUrl)
	if w.c.Gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}
	req.Header.Add("Authorization", "Token "+w.token)
	// fmt.Println(string(body))
	req.SetBody(body)
	// fmt.Println("-------------------------------------------------")
	// fmt.Println(string(body))

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func (w *InfluxdbV2Client) Query(lines []byte) (int64, error) {
	uri := fasthttp.AppendQuotedArg(w.queryUrl, lines)
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)
	if w.c.Gzip {
		req.Header.Add("Accept-Encoding", "gzip")
	}
	req.Header.Add("Authorization", "Token "+w.token)
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

func (d *InfluxdbV2Client) Setup() error {
	client := influxdb2.NewClient(d.c.Host, "")
	defer client.Close()
	resp, err := client.Setup(context.Background(), d.c.User, d.c.Password, organization, "nothing", 0)
	if err != nil {
		return err
	}
	d.token = *resp.Auth.Token
	return nil
}

func (d *InfluxdbV2Client) Login() error {
	client := influxdb2.NewClient(d.c.Host, d.token)
	defer client.Close()
	err := client.UsersAPI().SignIn(context.Background(), d.c.User, d.c.Password)
	if err != nil {
		return err
	}
	auths, err := client.AuthorizationsAPI().FindAuthorizationsByUserName(context.Background(), d.c.User)
	if err != nil {
		return err
	} else {
		for _, auth := range *auths {
			d.token = *auth.Token
		}
	}
	err = client.UsersAPI().SignOut(context.Background())
	return err
}

func (d *InfluxdbV2Client) MapBucket() error {
	client := influxdb2.NewClient(d.c.Host, d.token)
	defer client.Close()
	org, err := client.OrganizationsAPI().FindOrganizationByName(context.Background(), organization)
	if err != nil {
		return err
	}
	bucket, err := client.BucketsAPI().FindBucketByName(context.Background(), d.c.Database)
	if err != nil {
		return err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(append(d.host, "/api/v2/dbrps"...))
	req.Header.Add("Authorization", "token "+d.token)
	req.Header.Add("Content-type", "application/json")
	// fmt.Println(string(body))
	req.SetBody([]byte(fmt.Sprintf(`{
        "bucketID": "%s",
        "database": "%s",
        "default": true,
        "orgID": "%s",
        "retention_policy": "default"
      }`, *bucket.Id, d.c.Database, *org.Id)))
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err = d.client.Do(req, resp)
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusOK {
			err = fmt.Errorf("map bucket failed, status %d: %s", sc, resp.Body())
			return err
		}
	}

	return nil
}

func (d *InfluxdbV2Client) CreateDb(name string, withEncryption bool) error {

	client := influxdb2.NewClient(d.c.Host, d.token)
	defer client.Close()
	orgID, err := client.OrganizationsAPI().FindOrganizationByName(context.Background(), organization)
	if err != nil {
		return err
	}
	_, err = client.BucketsAPI().CreateBucketWithNameWithID(context.Background(), *orgID.Id, name)
	return err
}

// listDatabases lists the existing databases in InfluxDB.
func (d *InfluxdbV2Client) ListDatabases() ([]string, error) {
	//var u url.URL
	//u.Host = string(d.host)
	client := influxdb2.NewClient(d.c.Host, d.token)
	defer client.Close()
	api := client.BucketsAPI()
	resp, err := api.GetBuckets(context.Background())
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0)
	for _, bucket := range *resp {
		ret = append(ret, bucket.Name)
	}
	return ret, nil
}

func (d *InfluxdbV2Client) Ping() error {
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
