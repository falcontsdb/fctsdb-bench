package db_client

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/valyala/fasthttp"
)

func TestMysqlClient_CreateDb(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22",
		Database:  "test1",
		Gzip:      false,
		Debug:     false,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	defer mc.Close()
	err = mc.CreateDb("test1", true)
	if err != nil {
		fmt.Println(err)
	}
}

func TestMysqlClient_ListDatabases(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host: "172.17.2.22",
		//Database:  "benchmark_db",
		Gzip:      false,
		Debug:     false,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	defer mc.Close()
	if err != nil {
		fmt.Println(err)
	}
	dbs, err := mc.ListDatabases()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(dbs)
}

func TestMysqlClient_Ping(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22:3306",
		Database:  "test",
		Gzip:      false,
		Debug:     false,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	defer mc.Close()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(mc.Ping())
}

func TestMysqlClient_Query(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22",
		Database:  "test",
		Gzip:      false,
		Debug:     false,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	defer mc.Close()
	if err != nil {
		fmt.Println(err)
	}
	executeTime, err := mc.Query([]byte("select * from datax;"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(executeTime)
}

func TestMysqlClient_Write(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22",
		Database:  "test",
		Gzip:      false,
		Debug:     false,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})

	if err != nil {
		fmt.Println(err)
	}
	defer mc.Close()
	executeTime, err := mc.Write([]byte("create table vehicle (time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,VIN char(64) NOT NULL DEFAULT '',value60 bigint,value59 bigint,value58 bigint,value57 bigint,value56 bigint,value55 bigint,value54 bigint,value53 bigint,value52 bigint,value51 bigint,value50 bigint,value49 bigint,value48 bigint,value47 bigint,value46 bigint,value45 bigint,value44 bigint,value43 bigint,value42 bigint,value41 bigint,value40 bigint,value39 bigint,value38 bigint,value37 bigint,value36 bigint,value35 bigint,value34 bigint,value33 bigint,value32 bigint,value31 bigint,value30 bigint,value29 bigint,value28 bigint,value27 bigint,value26 bigint,value25 bigint,value24 bigint,value23 bigint,value22 bigint,value21 bigint,value20 bigint,value19 bigint,value18 bigint,value17 bigint,value16 bigint,value15 bigint,value14 bigint,value13 bigint,value12 bigint,value11 bigint,value10 bigint,value9 bigint,value8 bigint,value7 bigint,value6 bigint,value5 bigint,value4 bigint,value3 bigint,value2 bigint,value1 bigint,PRIMARY KEY pk_name_gender_ctime(time,VIN))"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(executeTime)
}

func TestFastHttpUrl(t *testing.T) {
	uri := fasthttp.AcquireURI()
	uri.Parse(nil, []byte("172.17.2.22:8086"))

	// uri, _ := url.Parse("172.17.2.22:8086")
	fmt.Println(uri.String())
}

func TestHttpUrl(t *testing.T) {
	uri, err := url.Parse("http://172.17.2.22:8086")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(uri.Host)
	}

	// uri, _ := url.Parse("172.17.2.22:8086")

}
