package db_client

import (
	"fmt"
	"strconv"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

var SupportedFormat []string = []string{"fctsdb", "mysql", "influxdbv2", "matrixdb", "opentsdb"}

type ClientConfig struct {
	Host     string
	Database string
	Gzip     int
	User     string
	Password string
	// Debug label for more informative errors.
	DebugInfo string
}

type DBClient interface {
	Write(body []byte) (int64, error)
	Query(body []byte) (int64, error)

	InitUser() error
	LoginUser() error
	// ListDatabases() ([]string, error)
	CreateDatabase(name string, withException bool) error
	CreateMeasurement(p *common.Point) error
	CheckConnection(timeout time.Duration) bool

	BeforeSerializePoints(buf []byte, p *common.Point) []byte
	SerializeAndAppendPoint(buf []byte, p *common.Point) []byte
	AfterSerializePoints(buf []byte, p *common.Point) []byte
}

func fastFormatAppend(v interface{}, buf []byte, singleQuotesForString bool) []byte {
	var quotationChar = "\""
	if singleQuotesForString {
		quotationChar = "'"
	}
	switch v := v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v), 10)
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case float64:
		return strconv.AppendFloat(buf, v, 'f', 16, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v), 'f', 16, 32)
	case bool:
		return strconv.AppendBool(buf, v)
	case []byte:
		buf = append(buf, quotationChar...)
		buf = append(buf, v...)
		buf = append(buf, quotationChar...)
		return buf
	case string:
		buf = append(buf, quotationChar...)
		buf = append(buf, v...)
		buf = append(buf, quotationChar...)
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %v", v))
	}
}

func IsSupportedFormat(format string) bool {
	for _, f := range SupportedFormat {
		if format == f {
			return true
		}
	}
	return false
}
