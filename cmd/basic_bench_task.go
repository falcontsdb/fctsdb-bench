package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/live"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/universal"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/db_client"
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/query_generator"
	"git.querycap.com/falcontsdb/fctsdb-bench/serializers"
	"github.com/valyala/fasthttp"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	},
}

type BasicBenchTask struct {
	// Program option vars:
	CsvDaemonUrls     string
	UseGzip           int
	Workers           int
	BatchSize         int
	DBName            string
	TimeLimit         time.Duration
	Format            string
	UseCase           string
	ScaleVar          int64
	ScaleVarOffset    int64
	SamplingInterval  time.Duration
	TimestampStartStr string
	TimestampEndStr   string
	Seed              int64
	Debug             bool
	CpuProfile        string
	DoDBCreate        bool
	MixMode           string
	QueryPercent      int
	QueryType         int
	QueryCount        int64
	NeedPrePare       bool
	Username          string
	Password          string
	WithEncryption    bool

	//runtime vars
	timestampStart  time.Time
	timestampEnd    time.Time
	daemonUrls      []string
	workerProcess   []Worker
	databaseNames   []string
	resultCollector *ResultCollector
	sqlTemplate     []string
}

func (d *BasicBenchTask) Validate() {
	d.daemonUrls = strings.Split(d.CsvDaemonUrls, ",")
	d.databaseNames = strings.Split(d.DBName, ",")
	if d.Format != "fctsdb" && d.Format != "mysql" {
		log.Fatal("wrong database format, support fctsdb or mysql ")
	}
	if len(d.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", d.daemonUrls)
	log.Println("Using mix mode", d.MixMode)

	// the default seed is the current timestamp:
	if d.Seed == 0 {
		d.Seed = int64(time.Now().Nanosecond())
	}
	log.Printf("using random seed %d\n", d.Seed)
	rand.Seed(d.Seed)

	// Parse timestamps:
	var err error
	d.timestampStart, err = time.Parse(time.RFC3339, d.TimestampStartStr)
	if err != nil {
		log.Fatalln("parse start error: ", err)
	}
	d.timestampStart = d.timestampStart.UTC()
	d.timestampEnd, err = time.Parse(time.RFC3339, d.TimestampEndStr)
	if err != nil {
		log.Fatalln("parse end error: ", err)
	}
	d.timestampEnd = d.timestampEnd.UTC()

	// samplingInterval and gzip
	if d.SamplingInterval <= 0 {
		log.Fatal("Invalid sampling interval")
	}
	log.Printf("Using sampling interval %v\n", d.SamplingInterval)
	if d.UseGzip < 0 || d.UseGzip > 9 {
		log.Fatal("Invalid gzip level, must bu in 0-9")
	}
	if d.UseGzip == 0 {
		log.Println("Close the gzip")
	} else {
		log.Println("Using gzip: level", d.UseGzip)
	}

	// query命令case和id对应相关处理
	log.Println("Use case", d.UseCase)
	if d.MixMode != "write_only" {
		var queryCase *fctsdb.QueryCase
		switch d.UseCase {
		case fctsdb.AirQuality.CaseName:
			queryCase = fctsdb.AirQuality
		case fctsdb.Vehicle.CaseName:
			queryCase = fctsdb.Vehicle
		default:
			log.Fatal("the use-case is unsupported")
		}

		if d.QueryType > 0 {
			if d.QueryType <= queryCase.Count {
				d.sqlTemplate = []string{queryCase.Types[d.QueryType].RawSql}
			} else {
				log.Fatalln("the query-type is out of range")
			}
		}

		if len(d.sqlTemplate) < 1 {
			log.Fatalln("the sql template is empty")
		} else {
			for _, sql := range d.sqlTemplate {
				log.Println("Use sql:", sql)
			}
		}
	}
}

func (d *BasicBenchTask) PrepareWorkers() {

	d.workerProcess = make([]Worker, 0)
	d.resultCollector = &ResultCollector{}

	// 建一个最小客户端，检查连接和创建数据库
	var cli db_client.DBClient
	var err error
	miniConfig := db_client.ClientConfig{
		Host:     d.daemonUrls[0],
		User:     d.Username,
		Password: d.Password,
	}
	switch d.Format {
	case "fctsdb":
		cli = db_client.NewFctsdbClient(miniConfig)
	case "mysql":
		cli, err = db_client.NewMysqlClient(miniConfig)
		if err != nil {
			log.Fatalln("open mysql failed" + err.Error())
		}
	}
	err = d.checkDbConnection(cli)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if d.DoDBCreate {
		d.createDb(cli)
	}

	// 根据dbName准备workers
	for _, dbName := range d.databaseNames {
		d.prepareWorkersOnEachDB(dbName)
	}
}

func (d *BasicBenchTask) prepareWorkersOnEachDB(dbName string) {

	// 每个database有Workers个线程
	workersEachDB := make([]Worker, d.Workers)

	// 每个database共享一个生成器
	var simulator common.Simulator
	switch d.UseCase {
	case common.UseCaseVehicle:
		cfg := vehicle.VehicleSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.SamplingInterval,
			DeviceCount:      d.ScaleVar,
			DeviceOffset:     d.ScaleVarOffset,
			SqlTemplates:     d.sqlTemplate,
		}
		simulator = cfg.ToSimulator()
	case common.UseCaseAirQuality:
		cfg := airq.AirqSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.SamplingInterval,
			DeviceCount:      d.ScaleVar,
			DeviceOffset:     d.ScaleVarOffset,
			SqlTemplates:     d.sqlTemplate,
		}
		simulator = cfg.ToSimulator()
	case common.UseCaseLiveCharge:
		cfg := live.LiveChargeSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.SamplingInterval,
			DeviceCount:      d.ScaleVar,
			DeviceOffset:     d.ScaleVarOffset,
			SqlTemplates:     d.sqlTemplate,
		}
		simulator = cfg.ToSimulator()
	case common.UseCaseDevOps:
		devops.EpochDuration = d.SamplingInterval
		cfg := &devops.DevopsSimulatorConfig{
			Start: d.timestampStart,
			End:   d.timestampEnd,
			// SamplingInterval: d.samplingInterval,
			HostCount:  d.ScaleVar,
			HostOffset: d.ScaleVarOffset,
		}
		simulator = cfg.ToSimulator()
	default:

		ucase := universal.UniversalCase{}
		err := json.Unmarshal([]byte(d.UseCase), &ucase)
		if err != nil {
			log.Fatalln("the case is not supported")
		}
		cfg := universal.UniversalSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.SamplingInterval,
			DeviceCount:      d.ScaleVar,
			DeviceOffset:     d.ScaleVarOffset,
			TagKeyCount:      ucase.TagKeyCount,
			FieldsDefine:     ucase.FieldsDefine,
		}
		simulator = cfg.ToSimulator()
	}

	// 只测试查询时，需要将模拟器的WrittenPoints设置为最大值，这样保证生成的sql在数据范围内。
	if d.MixMode == "read_only" {
		simulator.SetWrittenPoints(simulator.Total())
	}

	// 创建workers
	for j := 0; j < len(workersEachDB); j++ {
		// fmt.Println(j)
		worker := Worker{}
		worker.simulator = simulator // 共享生成器

		// 每个worker绑定一个序列化器
		switch d.Format {
		case "mysql":
			worker.serializer = serializers.NewSerializerMysql()
		case "fctsdb":
			worker.serializer = serializers.NewSerializerInflux()
		}

		// 每个worker绑定一个db client
		c := db_client.ClientConfig{
			Host:      d.daemonUrls[j%len(d.daemonUrls)],
			Database:  dbName,
			Gzip:      d.UseGzip > 0,
			Debug:     d.Debug,
			DebugInfo: fmt.Sprintf("worker #%d", j),
			User:      d.Username,
			Password:  d.Password,
		}
		switch d.Format {
		case "fctsdb":
			worker.writer = db_client.NewFctsdbClient(c)
		case "mysql":
			cli, err := db_client.NewMysqlClient(c)
			if err != nil {
				log.Fatalln("open mysql failed" + err.Error())
			}
			worker.writer = cli
		}

		// worker的其他必要参数
		worker.resultCollector = d.resultCollector
		worker.Debug = d.Debug
		worker.UseGzip = d.UseGzip
		worker.BatchSize = d.BatchSize
		switch d.MixMode {
		case "write_only":
			worker.Mode = "write"
		case "read_only":
			worker.Mode = "query"
		case "parallel":
			if j >= d.QueryPercent*d.Workers/100 {
				worker.Mode = "write"
			} else {
				worker.Mode = "query"
				// 混合测试时，查询的batch size设置为1
				worker.BatchSize = 1
			}
		}
		workersEachDB[j] = worker
	}

	// 如果是mysql还需要创建表
	if d.DoDBCreate && d.Format == "mysql" {
		w := workersEachDB[0].writer.(*db_client.MysqlClient)
		s := serializers.NewSerializerMysql()
		point := common.MakeUsablePoint()
		createdMeasurement := make(map[string]bool)
		buf := make([]byte, 0, 1024)
		for {
			simulator.Next(point)
			if _, ok := createdMeasurement[string(point.MeasurementName)]; ok {
				break
			}
			buf = s.CreateTableFromPoint(buf, point)
			_, err := w.Write(buf)
			if err != nil {
				log.Fatalln(err)
			}
			createdMeasurement[string(point.MeasurementName)] = true
		}
	}

	d.workerProcess = append(d.workerProcess, workersEachDB...)

}

func (d *BasicBenchTask) Run() {

	// 如果需要准备数据
	if d.NeedPrePare {
		log.Printf("We will prepare %d points", d.workerProcess[0].simulator.Total()*int64(len(d.databaseNames)))
		wg := sync.WaitGroup{}
		ctx, cancel := context.WithCancel(context.Background())
		for i := range d.workerProcess {
			wg.Add(1)
			go func(i int) {
				d.workerProcess[i].Prepare(&wg)
			}(i)
		}
		d.SyncShowStatics("prepare", ctx)
		wg.Wait()
		cancel()
	}

	// cpu profile
	if d.CpuProfile != "" {
		f, err := os.Create(d.CpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// 打印一些信息
	if d.TimeLimit > 0 {
		log.Printf("We will run about %s", d.TimeLimit)
	} else {
		if d.MixMode != "read_only" {
			log.Printf("We will write %d points", d.workerProcess[0].simulator.Total()*int64(len(d.databaseNames)))
		} else {
			log.Printf("We will query %d sql", d.QueryCount*int64(len(d.databaseNames)))
		}
	}
	log.Printf("Start run with %d workers", len(d.workerProcess))

	// 运行测试
	d.resultCollector.Reset()
	d.resultCollector.SetStartTime(time.Now())
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	for i := range d.workerProcess {
		wg.Add(1)
		go func(i int) {
			d.workerProcess[i].StartRun(d.TimeLimit, &wg)
		}(i)
	}
	d.SyncShowStatics(d.MixMode, ctx)
	wg.Wait()
	cancel()
	d.resultCollector.SetEndTime(time.Now())
}

func (d *BasicBenchTask) Report() map[string]string {
	took := d.resultCollector.endTime.Sub(d.resultCollector.startTime)
	pointsRead, bytesRead, valuesRead, queryRead := d.resultCollector.GetPoints(), d.resultCollector.GetBytes(), d.resultCollector.GetValues(), d.resultCollector.GetQueries()
	pointsRate := float64(pointsRead) / took.Seconds()
	bytesRate := float64(bytesRead) / took.Seconds()
	valuesRate := float64(valuesRead) / took.Seconds()
	queryRate := float64(queryRead) / took.Seconds()

	convertedBytesRate := bytesRate / (1 << 20)
	log.Printf("Has writen %d point, %.2fMB, %d queries in %0.2f sec (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec, %.2f q/sec)\n",
		pointsRead, float64(bytesRead)/(1<<20), queryRead, took.Seconds(), pointsRate, valuesRate, bytesRate/(1<<20), queryRate)

	groupResult := d.resultCollector.GetGroupDetail()
	groupResult.Show()
	result := groupResult.ToMap()
	result["PointRate(p/s)"] = fmt.Sprintf("%.2f", pointsRate)
	result["ValueRate(v/s)"] = fmt.Sprintf("%.2f", valuesRate)
	result["BytesRate(MB/s)"] = fmt.Sprintf("%.2f", convertedBytesRate)
	result["TotalPoints"] = fmt.Sprintf("%d", pointsRead)
	result["UseCase"] = d.UseCase
	result["Mod"] = d.MixMode
	result["BatchSize"] = fmt.Sprintf("%d", d.BatchSize)
	result["Workers"] = fmt.Sprintf("%d", d.Workers)
	result["QueryPercent"] = fmt.Sprintf("%d", d.QueryPercent)
	result["Cardinality"] = fmt.Sprintf("%d", d.ScaleVar)
	result["SamplingTime"] = d.SamplingInterval.String()
	result["Gzip"] = fmt.Sprintf("%d", d.UseGzip)

	// buf := bytes.NewBuffer(make([]byte, 0, 1024))
	// jsonEncoder := json.NewEncoder(buf)
	// jsonEncoder.SetEscapeHTML(false)
	// jsonEncoder.Encode(d.respCollector.sqlTemplate)
	if len(d.sqlTemplate) > 0 {
		result["Sql"] = d.sqlTemplate[0]
	}
	return result
}

func (d *BasicBenchTask) SyncShowStatics(status string, ctx context.Context) {
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		defer ticker.Stop()
		lastTime := time.Now()
		lastItems, lastValues, lastBytes, lastQuery := d.resultCollector.GetPoints(), d.resultCollector.GetValues(), d.resultCollector.GetBytes(), d.resultCollector.GetQueries()
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				took := now.Sub(lastTime)
				lastTime = now
				itemsRate := float64(d.resultCollector.GetPoints()-lastItems) / took.Seconds()
				bytesRate := float64(d.resultCollector.GetBytes()-lastBytes) / took.Seconds()
				valuesRate := float64(d.resultCollector.GetValues()-lastValues) / took.Seconds()
				queryRate := float64(d.resultCollector.GetQueries()-lastQuery) / took.Seconds()
				lastItems, lastValues, lastBytes, lastQuery = d.resultCollector.GetPoints(), d.resultCollector.GetValues(), d.resultCollector.GetBytes(), d.resultCollector.GetQueries()
				switch status {
				case "prepare":
					log.Printf("Has prepare %d point, %.2fMB (mean point rate %.2f/sec, %.2fMB/sec in this %0.2f sec)",
						lastItems, float64(lastBytes)/(1<<20), itemsRate, bytesRate/(1<<20), took.Seconds())
				case "write_only":
					log.Printf("Has writen %d point, %.2fMB (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec in this %0.2f sec)",
						lastItems, float64(lastBytes)/(1<<20), itemsRate, valuesRate, bytesRate/(1<<20), took.Seconds())
				case "read_only":
					log.Printf("Has writen %d queries(mean %.2f q/sec in this %0.2f sec)\n",
						lastQuery, queryRate, took.Seconds())
				default:
					log.Printf("Has writen %d point, %.2fMB, %d queries (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec, %.2f q/sec in this %0.2f sec)",
						lastItems, float64(lastBytes)/(1<<20), lastQuery, itemsRate, valuesRate, bytesRate/(1<<20), queryRate, took.Seconds())
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (d *BasicBenchTask) CleanUp() {
	if d.Format == "mysql" {
		for _, worker := range d.workerProcess {
			w := worker.writer.(*db_client.MysqlClient)
			w.Close()
		}
	}
}

func (d *BasicBenchTask) createDb(writer db_client.DBClient) {
	// this also test db connection
	existingDatabases, err := writer.ListDatabases()
	if err != nil {
		log.Println(err)
	}

	if len(existingDatabases) > 0 {
		dbs_string := strings.Join(existingDatabases, ", ")
		log.Printf("The following databases already exist in the data store: %s", dbs_string)
	}
	for _, name := range d.databaseNames {
		for _, existingDatabase := range existingDatabases {
			if existingDatabase == name {
				log.Printf("Database %s already exists", name)
				return
			}
		}
		err = writer.CreateDb(name, d.WithEncryption)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Database %s created", name)
	}
	time.Sleep(1000 * time.Millisecond)
}

func (d *BasicBenchTask) checkDbConnection(w db_client.DBClient) error {
	for i := 0; i < 30; i++ {
		err := w.Ping()
		if err != nil {
			log.Println("Ping DB error:", err.Error())
		} else {
			return nil
		}
		time.Sleep(5 * time.Second)
	}
	return errors.New("can not connect DB in 60s")
}

// 最小运行单位，包含一个模拟器、序列化器、写入器、结果收集器
type Worker struct {
	writer          db_client.DBClient
	simulator       common.Simulator
	resultCollector *ResultCollector
	serializer      serializers.Serializer
	Debug           bool
	Mode            string
	UseGzip         int
	QueryCount      int64
	BatchSize       int
}

func (w *Worker) Prepare(wg *sync.WaitGroup) {
	defer wg.Done()
	point := common.MakeUsablePoint()
	for !w.simulator.Finished() {
		err := w.runBatchAndWrite(2000, true, point)
		if err != nil && w.Debug {
			log.Println(err.Error())
		}
	}
}

func (w *Worker) StartRun(timeLimit time.Duration, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	endTime := time.Now().Add(timeLimit)
	switch w.Mode {
	case "write":
		point := common.MakeUsablePoint()
		if timeLimit > 0 {
			for time.Now().Before(endTime) {
				err := w.runBatchAndWrite(w.BatchSize, false, point)
				if err != nil && w.Debug {
					log.Println(err.Error())
				}
			}
		} else {
			for !w.simulator.Finished() {
				err := w.runBatchAndWrite(w.BatchSize, true, point)
				if err != nil && w.Debug {
					log.Println(err.Error())
				}
			}
		}
	case "query":
		if timeLimit > 0 {
			for time.Now().Before(endTime) {
				err := w.runBatchAndQuery(w.BatchSize, false)
				if err != nil && w.Debug {
					log.Println(err.Error())
				}
			}
		} else {
			for w.resultCollector.GetQueries() < w.QueryCount {
				err := w.runBatchAndQuery(w.BatchSize, true)
				if err != nil && w.Debug {
					log.Println(err.Error())
				}
			}
		}
	}
}

func (d *Worker) runBatchAndWrite(batchSize int, useCountLimit bool, point *common.Point) error {
	// var batchesSeen int64
	// 发送http write

	buf := make([]byte, 0, 1024)
	var err error
	var batchItemCount int = 1
	var vaulesWritten int = 0
	var pointMadeIndex int64
	point.Reset()
	pointMadeIndex = d.simulator.Next(point)
	buf = d.serializer.SerializePrepare(buf, point)
	buf = d.serializer.SerializePoint(buf, point)
	vaulesWritten += (len(point.FieldValues) + len(point.Int64FiledValues))
	for batchItemCount < batchSize {
		if pointMadeIndex > d.simulator.Total() && useCountLimit { // 以simulator.Finished()结束为结束
			break
		}
		point.Reset()
		pointMadeIndex = d.simulator.Next(point)
		buf = d.serializer.SerializePoint(buf, point)
		batchItemCount++
		vaulesWritten += (len(point.FieldValues) + len(point.Int64FiledValues))
	}

	if batchItemCount > 0 {
		buf = d.serializer.SerializeEnd(buf, point)
		err = d.writeToDb(buf)
		if err == nil {
			d.resultCollector.AddBytes(int64(len(buf)))
			d.resultCollector.AddValues(int64(vaulesWritten))
			d.resultCollector.AddPoints(int64(batchItemCount))
			d.simulator.SetWrittenPoints(pointMadeIndex)
		}
	}
	// buf = buf[:0]
	return err
}

func (d *Worker) writeToDb(buf []byte) error {
	// var batchesSeen int64
	// 发送http write
	var err error
	var lat int64
	if d.UseGzip > 0 {
		compressedBatch := bufferPool.Get().(*bytes.Buffer)
		fasthttp.WriteGzipLevel(compressedBatch, buf, d.UseGzip)
		//bodySize = len(compressedBatch.Bytes())
		lat, err = d.writer.Write(compressedBatch.Bytes())
		// Return the compressed batch buffer to the pool.
		compressedBatch.Reset()
		bufferPool.Put(compressedBatch)
	} else {
		//bodySize = len(batch.Bytes())
		// fmt.Println(string(buf.Bytes()))
		lat, err = d.writer.Write(buf)
	}

	if err != nil {
		d.resultCollector.AddOneResponTime("write", lat, false)
		return fmt.Errorf("error writing: %s", err.Error())
	}
	d.resultCollector.AddOneResponTime("write", lat, true)
	return nil
}

func (d *Worker) runBatchAndQuery(batchSize int, useCountLimit bool) error {
	var err error
	var lat int64
	buf := bufferPool.Get().(*bytes.Buffer)
	var batchItemCount int = 0
	for batchItemCount < batchSize {
		madeSqlCount := d.simulator.NextSql(buf)
		if madeSqlCount > d.QueryCount && useCountLimit {
			break
		}
		batchItemCount++
		if buf.Bytes()[buf.Len()-1] != ';' {
			buf.Write([]byte(";"))
		}
	}

	if batchItemCount > 0 {
		d.resultCollector.AddQueries(1)
		// atomic.AddInt64(&d.queryRead, int64(batchItemCount))
		lat, err = d.writer.Query(buf.Bytes())
		if err != nil {
			d.resultCollector.AddOneResponTime("query", lat, false)
		} else {
			d.resultCollector.AddOneResponTime("query", lat, true)
		}
	}
	buf.Reset()
	bufferPool.Put(buf)
	return err
}
