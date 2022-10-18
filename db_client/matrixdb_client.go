package db_client

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

// MatrixdbWithMxgateClient is a Writer that writes to a fctsdb HTTP server.
type MatrixdbWithMxgateClient struct {
	httpclient fasthttp.Client
	c          ClientConfig
	writeUrl   []byte
	buf        *bytes.Buffer
	sqlDB      *sql.DB
}

// NewMatrixdbClient returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewMatrixdbClient(c ClientConfig) *MatrixdbWithMxgateClient {

	// mxgate api: http://localhost:8086/
	writeUrl := make([]byte, 0)
	writeUrl = append(writeUrl, "http://"...)
	writeUrl = append(writeUrl, c.Host...)
	writeUrl = append(writeUrl, ":8086"...)
	writeUrl = append(writeUrl, "/"...)

	return &MatrixdbWithMxgateClient{
		httpclient: fasthttp.Client{
			Name:                "fctsdb",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},
		c:        c,
		writeUrl: writeUrl,
		buf:      bytes.NewBuffer(make([]byte, 0, 8*1024)),
	}
}

// Write writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (f *MatrixdbWithMxgateClient) Write(body []byte) (int64, error) {
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
	err := f.httpclient.Do(req, resp)
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

func (f *MatrixdbWithMxgateClient) Query(body []byte) (int64, error) {
	conn, err := f.sqlDB.Conn(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	log.Debug(string(body))
	start := time.Now()
	rows, err := conn.QueryContext(context.Background(), string(body))
	if err != nil {
		return 0, err
	}

	defer rows.Close()
	count := 0
	for rows.Next() {
		count += 1
	}
	lat := time.Since(start).Nanoseconds()
	if count == 0 {
		return lat, fmt.Errorf("query result is empty")
	}
	// fmt.Println("count:", count)
	return lat, err
}

func (f *MatrixdbWithMxgateClient) InitUser() error {
	return nil
}

func (f *MatrixdbWithMxgateClient) LoginUser() error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", f.c.Host, 5432, f.c.User, f.c.Password, f.c.Database)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	f.sqlDB = db
	return nil
}

func (f *MatrixdbWithMxgateClient) Close() {
	if f.sqlDB != nil {
		f.sqlDB.Close()
	}
}

func (f *MatrixdbWithMxgateClient) DropDatabase(name string) error {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", f.c.Host, 5432, f.c.User, f.c.Password)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Infof("drop database %s", name)
	existingDatabases, err := f.listDatabases()
	if err != nil {
		return err
	}

	for _, existingDatabase := range existingDatabases {
		if name == existingDatabase {
			_, err = db.Exec(fmt.Sprintf("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname='%s' AND pid<>pg_backend_pid();", name))
			if err != nil {
				return fmt.Errorf("close database error: %s", err.Error())
			}
			time.Sleep(time.Second)
			_, err = db.Exec(fmt.Sprintf("DROP DATABASE %s;", name))
			if err != nil {
				return fmt.Errorf("drop database error: %s", err.Error())
			}
			return nil
		}
	}

	return nil
}

func (f *MatrixdbWithMxgateClient) CreateDatabase(name string, withEncryption bool) error {

	log.Infof("start create database %s", name)
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", f.c.Host, 5432, f.c.User, f.c.Password)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	defer db.Close()

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

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s;", name))
	if err != nil {
		return fmt.Errorf("create database error: %s", err.Error())
	}

	return nil
}

// listDatabases lists the existing databases in InfluxDB.
func (f *MatrixdbWithMxgateClient) listDatabases() ([]string, error) {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", f.c.Host, 5432, f.c.User, f.c.Password)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	r, err := db.Query("select datname from pg_database;")
	if err != nil {
		return nil, err
	}

	databases := make([]string, 0)
	for r.Next() {
		var name string
		r.Scan(&name)
		databases = append(databases, name)
	}

	log.Info("The following databases already exist in the data store: ", strings.Join(databases, ", "))
	return databases, nil
}

func (f *MatrixdbWithMxgateClient) CreateMeasurement(p *common.Point) error {

	log.Info("start create measurement: ", string(p.MeasurementName))
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", f.c.Host, 5432, f.c.User, f.c.Password, f.c.Database)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	defer db.Close()
	db.Exec("CREATE EXTENSION matrixts;") // *** mars2 ***

	buf := make([]byte, 0, 4*1024)
	buf = append(buf, "create table public."...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, " ("...)

	// add the timestamp
	buf = append(buf, "time timestamp"...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, " text"...)
	}
	buf = append(buf, ',')

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		k := p.FieldKeys[i]
		v := p.FieldValues[i]
		buf = append(buf, k...)
		switch v.(type) {
		case int, int64:
			buf = append(buf, " bigint"...)
		case float64, float32:
			buf = append(buf, " double precision"...)
		case []byte:
			buf = append(buf, " text"...)
		case string:
			buf = append(buf, " text"...)
		case bool:
			buf = append(buf, " boolean"...)
		default:
			return fmt.Errorf("unknown field type for %#v", v)
		}
		buf = append(buf, ',')
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		buf = append(buf, p.Int64FiledKeys[i]...)
		buf = append(buf, " bigint"...)
		buf = append(buf, ',')
	}
	// *** heap ***
	// buf = append(buf, "PRIMARY KEY (time,"...)
	// for i := 0; i < len(p.TagKeys); i++ {
	// 	buf = append(buf, p.TagKeys[i]...)
	// 	if i+1 < len(p.TagKeys) {
	// 		buf = append(buf, ',')
	// 	}
	// }
	// buf = append(buf, ')')
	// *** heap ***

	// *** mars2 ***
	if buf[len(buf)-1] == ',' {
		buf = buf[0 : len(buf)-1]
	}
	// *** mars2 ***

	buf = append(buf, ')')

	switch string(p.MeasurementName) {
	case "city_air_quality":
		buf = append(buf, " USING mars2 WITH (compresstype='lz4', compresslevel=1) DISTRIBUTED BY (site_id);"...) // *** mars2 ***
		// buf = append(buf, " DISTRIBUTED BY (site_id);"...) // *** heap ***
		buf = append(buf, "CREATE INDEX time ON public."...)
		buf = append(buf, p.MeasurementName...)
		buf = append(buf, " USING mars2_btree (site_id, time);"...) // *** mars2 ***
		// buf = append(buf, " (site_id, time);"...) // *** heap ***
	case "vehicle":
		buf = append(buf, " USING mars2 WITH (compresstype='lz4', compresslevel=1) DISTRIBUTED BY (VIN);"...) // *** mars2 ***
		// buf = append(buf, " DISTRIBUTED BY (VIN);"...) // *** heap ***
		buf = append(buf, "CREATE INDEX time ON public."...)
		buf = append(buf, p.MeasurementName...)
		buf = append(buf, " USING mars2_btree (VIN, time);"...) // *** mars2 ***
		// buf = append(buf, " (VIN, time);"...) // *** heap ***
	}

	// *** heap ***
	// for _, tag := range p.TagKeys {
	// 	buf = append(buf, "CREATE INDEX "...)
	// 	buf = append(buf, tag...)
	// 	buf = append(buf, " ON public."...)
	// 	buf = append(buf, p.MeasurementName...)
	// 	buf = append(buf, " ("...)
	// 	buf = append(buf, tag...)
	// 	buf = append(buf, ");"...)
	// }
	// *** heap ***

	// fmt.Println(string(buf))
	// 创建表

	log.Debug(string(buf))
	_, err = db.Exec(string(buf))
	return err
}

func (f *MatrixdbWithMxgateClient) CheckConnection(timeout time.Duration) bool {
	endTime := time.Now().Add(timeout)
	log.Info("checking connection ")
	fmt.Print("checking .")
	defer fmt.Println()
	for time.Now().Before(endTime) {
		_, err := net.DialTimeout("tcp", f.c.Host+":5432", 1*time.Second)
		if err == nil {
			return true
		}
		time.Sleep(2 * time.Second)
		fmt.Print(".")
	}
	return false
}

func (m *MatrixdbWithMxgateClient) BeforeSerializePoints(buf []byte, p *common.Point) []byte {
	buf = append(buf, "public."...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, "\n"...)
	return buf
}

func (s *MatrixdbWithMxgateClient) SerializeAndAppendPoint(buf []byte, p *common.Point) []byte {

	// add the timestamp

	buf = strconv.AppendInt(buf, p.Timestamp.Unix(), 10)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagValues[i]...)

	}
	buf = append(buf, ',')

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)
		if i+1 < len(p.FieldKeys) || len(p.Int64FiledKeys) != 0 {
			buf = append(buf, ',')
		}
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		v := p.Int64FiledValues[i]
		buf = strconv.AppendInt(buf, v, 10)
		if i+1 < len(p.Int64FiledKeys) {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, '\n')

	return buf
}

func (m *MatrixdbWithMxgateClient) AfterSerializePoints(buf []byte, p *common.Point) []byte {

	return buf
}
