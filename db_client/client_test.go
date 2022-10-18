package db_client

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	_ "github.com/lib/pq"
	"github.com/valyala/fasthttp"
)

func TestMysqlClient_CreateDb(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22",
		Database:  "test1",
		Gzip:      3,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	defer mc.Close()
	err = mc.CreateDatabase("test1", true)
	if err != nil {
		fmt.Println(err)
	}
}

func TestMysqlClient_ListDatabases(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host: "172.17.2.22",
		//Database:  "benchmark_db",
		Gzip:      3,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	defer mc.Close()
	// dbs, err := mc.ListDatabases()
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println(dbs)
}

func TestMysqlClient_Ping(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22:3306",
		Database:  "test",
		Gzip:      3,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	defer mc.Close()
	fmt.Println(mc.CheckConnection(time.Minute))
}

func TestMysqlClient_Query(t *testing.T) {
	mc, err := NewMysqlClient(ClientConfig{
		Host:      "172.17.2.22",
		Database:  "test",
		Gzip:      3,
		User:      "root",
		Password:  "123456",
		DebugInfo: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	defer mc.Close()
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
		Gzip:      3,
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

func TestInfluxdbv2Client_CreateDb(t *testing.T) {
	// client := influxdb2.NewClient("http://10.10.2.29:8086", "2ziUmLRFWB_yhaJSS0Wg11hkizyIqzG5QsRvarxxUopbjXl5IoNj8QouthtTpDLdaFWbkYNDTiW8RsyoSfPhWw==")
	// client.Setup(context.Background(), "root", "Abc_123456", "test", "buck1", 0)
	// always close client at the end
	client := influxdb2.NewClient("http://10.10.2.29:8086", "8wW-pwH8qzizqRQrA4E4tL2Kd6YJTaq7xhwJu7CDacZEcfwCbldzQc9GQ3XoEOA1V1GBrDgRkgtw8lGK4y9Omg==")
	// ok, err := client.Ping(context.Background())
	// fmt.Println(ok, err)

	orgs, err := client.OrganizationsAPI().GetOrganizations(context.Background())
	if err != nil {
		fmt.Println(err.Error())
	} else {
		for _, org := range *orgs {
			fmt.Println(org.Name, *org.Id)
		}
	}
	defer client.Close()

	// userApi := client.UsersAPI()
	// // err := userApi.SignIn(context.Background(), "root", "Abc_123456")
	// me, err := userApi.Me(context.Background())
	// if err != nil {
	// 	fmt.Println(err.Error())
	// } else {
	// 	fmt.Println(me.Name)
	// }
	// // userApi.SignOut(context.Background())
	// authApi := client.AuthorizationsAPI()
	// resp, err := authApi.GetAuthorizations(context.Background())
	// fmt.Println(err)
	// if err == nil {
	// 	for _, a := range *resp {
	// 		fmt.Println(*a.Token)
	// 	}
	// }
	// qApi := client.QueryAPI(organization)
	// qApi.

	// writeAPI := client.WriteAPI("test", "buck1")
	// writeAPI.WritePoint()
	// buckAPI := client.BucketsAPI()
	// buckets, err := buckAPI.GetBuckets(context.Background())
	// fmt.Println(len(*buckets))
	// if err == nil {
	// }
	// 	for _, buck := range *buckets {
	// 		fmt.Println(buck.Name)
	// 	}
	// }

	// // write line protocol
	// writeAPI.WriteRecord(fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0))
	// Flush writes
	// writeAPI.Flush()
}

func TestMatrixdb(t *testing.T) {
	mc := NewMatrixdbClient(ClientConfig{
		Host:     "10.10.2.29",
		Database: "benchmark_db",
		Gzip:     3,
		User:     "mxadmin",
		Password: "Abc_123456",
	})
	err := mc.LoginUser()
	if err != nil {
		fmt.Println(err.Error())
	}

	mc.DropDatabase("benchmark_db")
	mc.CreateDatabase("benchmark_db", true)

	// err = mc.CreateDatabase("testdb", true)
	if err != nil {
		fmt.Println(err.Error())
	}

	now := time.Now()
	cfg := &airq.AirqSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      1000,
		DeviceOffset:     1,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()
	// // buf := make([]byte, 0, 4*1024)
	sim.Next(point)
	err = mc.CreateMeasurement(point)
	if err != nil {
		fmt.Println(err.Error())
	}
	// buf = mc.BeforeSerializePoints(buf, point)
	// buf = mc.SerializeAndAppendPoint(buf, point)
	// point.Reset()

	// for i := 0; i < 10; i++ {
	// 	sim.Next(point)
	// 	buf = mc.SerializeAndAppendPoint(buf, point)
	// 	point.Reset()
	// }
	// buf = mc.AfterSerializePoints(buf, point)
	// // fmt.Println(string(buf))

	// lat, err := mc.Write(buf)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	// fmt.Println(lat)

	// mc.Query([]byte("select aqi from city_air_quality where site_id = 'DEV000000980' order by time desc limit 1;"))
	// mc.Close()
	// err = mc.CreateMeasurement(point)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }

	// mc.listDatabases()
	// err = mc.DropDatabase("testdb")
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }

}

func BenchmarkOpentsdb(b *testing.B) {
	oc := NewOpentsdbClient(ClientConfig{
		Host:     "http://10.10.2.29:8087/",
		Database: "benchmark_db",
		Gzip:     3,
		User:     "mxadmin",
		Password: "Abc_123456",
	})
	// fmt.Println(oc.CheckConnection(time.Second * 10))
	now := time.Now()
	cfg := &vehicle.VehicleSimulatorConfig{
		Start:            now.Add(time.Hour * -24000),
		End:              now,
		SamplingInterval: time.Second,
		DeviceCount:      1000,
		DeviceOffset:     1,
	}
	sim := cfg.ToSimulator()

	point := common.MakeUsablePoint()

	buf := make([]byte, 0, 4*1024)
	buf = oc.BeforeSerializePoints(buf, point)
	buf = oc.SerializeAndAppendPoint(buf, point)
	point.Reset()

	for i := 0; i < b.N; i++ {
		sim.Next(point)
		buf = oc.SerializeAndAppendPoint(buf, point)
		point.Reset()
		buf = buf[:0]
	}
	// buf = oc.AfterSerializePoints(buf, point)
	// fmt.Println(string(buf))

}
