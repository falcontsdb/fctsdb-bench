package db_client

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
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

// NewInfluxdbV2Client returns a new HTTPWriter from the supplied HTTPWriterConfig.
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
func (f *InfluxdbV2Client) Write(body []byte) (int64, error) {
	log.Debug("Write body", string(body))
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(f.writeUrl)
	req.Header.Add("Authorization", "Token "+f.token)
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

func (f *InfluxdbV2Client) Query(body []byte) (int64, error) {
	uri := fasthttp.AppendQuotedArg(f.queryUrl, body)
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURIBytes(uri)
	req.Header.Add("Authorization", "Token "+f.token)
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
				log.Error("[ParseGzip] NewReader error: %v, maybe data is ungzip\n", err)
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

func (d *InfluxdbV2Client) InitUser() error {

	client := influxdb2.NewClient(string(d.host), "")
	defer client.Close()
	resp, err := client.Setup(context.Background(), d.c.User, d.c.Password, organization, "nothing", 0)
	if err != nil {
		return err
	}
	d.token = *resp.Auth.Token
	return nil
}

func (d *InfluxdbV2Client) LoginUser() error {

	client := influxdb2.NewClient(string(d.host), d.token)
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
	log.Debug("token: ", d.token)
	err = client.UsersAPI().SignOut(context.Background())
	return err
}

func (d *InfluxdbV2Client) MapBucket() error {
	client := influxdb2.NewClient(string(d.host), d.token)
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

func (d *InfluxdbV2Client) CreateDatabase(name string, withEncryption bool) error {

	existingDatabases, err := d.listDatabases()
	if err != nil {
		return err
	}

	for _, existingDatabase := range existingDatabases {
		if name == existingDatabase {
			log.Warn("The following database \"%s\" already exist in the data store, do'not need create.", name)
			return nil
		}
	}

	client := influxdb2.NewClient(string(d.host), d.token)
	defer client.Close()
	orgID, err := client.OrganizationsAPI().FindOrganizationByName(context.Background(), organization)
	if err != nil {
		return err
	}
	_, err = client.BucketsAPI().CreateBucketWithNameWithID(context.Background(), *orgID.Id, name)
	return err
}

// listDatabases lists the existing databases in InfluxDB.
func (d *InfluxdbV2Client) listDatabases() ([]string, error) {

	client := influxdb2.NewClient(string(d.host), d.token)
	defer client.Close()
	api := client.BucketsAPI()
	resp, err := api.GetBuckets(context.Background())
	if err != nil {
		return nil, err
	}
	databases := make([]string, 0)
	for _, bucket := range *resp {
		databases = append(databases, bucket.Name)
	}
	log.Info("The following databases already exist in the data store: ", strings.Join(databases, ", "))
	return databases, nil
}

func (f *InfluxdbV2Client) CreateMeasurement(p *common.Point) error {
	return nil
}

func (f *InfluxdbV2Client) CheckConnection(timeout time.Duration) bool {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethodBytes(get)
	req.Header.SetRequestURI(fmt.Sprintf("%s/ping", f.host))

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	// client := http.Client{}
	clientWithTimeout := fasthttp.Client{WriteTimeout: time.Second, ReadTimeout: time.Second, MaxConnWaitTimeout: time.Second}
	endTime := time.Now().Add(timeout)
	log.Info("checking connection ")
	fmt.Print("checking .")
	defer fmt.Println()
	for time.Now().Before(endTime) {
		err := clientWithTimeout.DoTimeout(req, resp, time.Second)
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
func (m *InfluxdbV2Client) BeforeSerializePoints(buf []byte, p *common.Point) []byte {
	return buf
}

func (s *InfluxdbV2Client) SerializeAndAppendPoint(buf []byte, p *common.Point) []byte {
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

func (m *InfluxdbV2Client) AfterSerializePoints(buf []byte, p *common.Point) []byte {
	return buf
}
