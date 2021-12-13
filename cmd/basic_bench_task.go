package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/db_client"
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/query_generator"
	"git.querycap.com/falcontsdb/fctsdb-bench/serializers"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
	"github.com/valyala/fasthttp"
)

type BasicBenchTask struct {
	// Program option vars:
	csvDaemonUrls     string
	useGzip           int
	workers           int
	batchSize         int
	dbName            string
	timeLimit         time.Duration
	format            string
	useCase           string
	scaleVar          int64
	scaleVarOffset    int64
	samplingInterval  time.Duration
	timestampStartStr string
	timestampEndStr   string
	seed              int64
	debug             bool
	cpuProfile        string
	doDBCreate        bool
	mixMode           string
	queryPercent      int
	queryType         int
	queryCount        int64
	needPrePare       bool
	username          string
	password          string
	withEncryption    bool

	//runtime vars
	timestampStart time.Time
	timestampEnd   time.Time
	daemonUrls     []string
	bufPool        sync.Pool
	inputDone      chan struct{}
	writers        []common.DBClient
	valuesRead     int64
	itemsRead      int64
	bytesRead      int64
	queryRead      int64
	simulator      common.Simulator
	respCollector  ResponseCollector
	sqlTemplate    []string
	serializer     common.Serializer
}

func (d *BasicBenchTask) Validate() {
	d.daemonUrls = strings.Split(d.csvDaemonUrls, ",")
	if d.format != "fctsdb" && d.format != "mysql" {
		log.Fatal("wrong database format,support fctsdb,mysql ")
	}
	if len(d.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", d.daemonUrls)
	log.Println("Using mix mode", d.mixMode)
	// the default seed is the current timestamp:
	if d.seed == 0 {
		d.seed = int64(time.Now().Nanosecond())
	}
	log.Printf("using random seed %d\n", d.seed)
	rand.Seed(d.seed)

	// Parse timestamps:
	var err error
	d.timestampStart, err = time.Parse(time.RFC3339, d.timestampStartStr)
	if err != nil {
		log.Fatalln("parse start error: ", err)
	}
	d.timestampStart = d.timestampStart.UTC()
	d.timestampEnd, err = time.Parse(time.RFC3339, d.timestampEndStr)
	if err != nil {
		log.Fatalln("parse end error: ", err)
	}
	d.timestampEnd = d.timestampEnd.UTC()

	if d.samplingInterval <= 0 {
		log.Fatal("Invalid sampling interval")
	}
	log.Printf("Using sampling interval %v\n", d.samplingInterval)
	if d.useGzip < 0 || d.useGzip > 9 {
		log.Fatal("Invalid gzip level, must bu in 0-9")
	}
	if d.useGzip == 0 {
		log.Println("Close the gzip")
	} else {
		log.Println("Using gzip: level", d.useGzip)
	}

	if mixReadWrite.cpuProfile != "" {
		f, err := os.Create(mixReadWrite.cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	log.Println("Use case", d.useCase)

	var queryCase *fctsdb.QueryCase
	switch d.useCase {
	case fctsdb.AirQuality.CaseName:
		queryCase = fctsdb.AirQuality
	case fctsdb.Vehicle.CaseName:
		queryCase = fctsdb.Vehicle
	default:
		log.Fatal("the use-case is unsupported")
	}

	if d.queryType > 0 {
		if d.queryType <= queryCase.Count {
			d.sqlTemplate = []string{queryCase.Types[d.queryType].RawSql}
		} else {
			log.Fatalln("the query-type is out of range")
		}
	}

	if d.mixMode != "write_only" {
		if len(d.sqlTemplate) < 1 {
			log.Fatalln("the sql template is empty")
		} else {
			for _, sql := range d.sqlTemplate {
				log.Println("Use sql:", sql)
			}
		}
	}
}

func (d *BasicBenchTask) PrepareWorkers() int {
	d.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	d.inputDone = make(chan struct{})
	d.writers = make([]common.DBClient, d.workers)
	for i := 0; i < len(d.writers); i++ {
		c := &common.ClientConfig{
			Host:      d.daemonUrls[i%len(d.daemonUrls)],
			Database:  d.dbName,
			Gzip:      d.useGzip > 0,
			Debug:     d.debug,
			DebugInfo: fmt.Sprintf("worker #%d", i),
			User:      d.username,
			Password:  d.password,
		}
		switch d.format {
		case "fctsdb":
			d.writers[i] = db_client.NewFctsdbClient(*c)
		case "mysql":
			cli, err := db_client.NewMysqlClient(*c)
			if err != nil {
				log.Fatalln("open mysql failed" + err.Error())
			}
			d.writers[i] = cli
		}
	}

	switch d.useCase {
	case common.UseCaseVehicle:
		cfg := &vehicle.VehicleSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.samplingInterval,
			DeviceCount:      d.scaleVar,
			DeviceOffset:     d.scaleVarOffset,
			SqlTemplates:     d.sqlTemplate,
		}
		d.simulator = cfg.ToSimulator()
	case common.UseCaseAirQuality:
		cfg := &airq.AirqSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.samplingInterval,
			DeviceCount:      d.scaleVar,
			DeviceOffset:     d.scaleVarOffset,
			SqlTemplates:     d.sqlTemplate,
		}
		d.simulator = cfg.ToSimulator()
	case common.UseCaseDevOps:
		devops.EpochDuration = d.samplingInterval
		cfg := &devops.DevopsSimulatorConfig{
			Start: d.timestampStart,
			End:   d.timestampEnd,
			// SamplingInterval: d.samplingInterval,
			HostCount:  d.scaleVar,
			HostOffset: d.scaleVarOffset,
		}
		d.simulator = cfg.ToSimulator()
	default:
		log.Fatalln("the case is not supported")
	}

	if d.timeLimit > 0 {
		log.Printf("We will run about %s", d.timeLimit)
	} else {
		if d.mixMode != "read_only" {
			log.Printf("We will write %d points", d.simulator.Total())
		} else {
			log.Printf("We will query %d sql", d.queryCount)
		}
	}

	writer := d.writers[0]

	err := d.checkDbConnection(writer)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if d.doDBCreate {
		d.createDb(writer)
	}
	switch d.format {

	case "mysql":
		d.serializer = serializers.NewSerializerMysql()
		if d.doDBCreate {
			w, ok := writer.(*db_client.MysqlClient)
			if !ok {
				log.Fatalln("wrong mysql client")
			}
			s, ok := d.serializer.(*serializers.SerializerMysql)
			if !ok {
				log.Fatalln("wrong mysql serializer")
			}
			point := common.MakeUsablePoint()
			createdMeasurement := make(map[string]bool, 0)
			buf := bytes.NewBuffer(make([]byte, 0, 1024))
			for {
				d.simulator.Next(point)
				if _, ok := createdMeasurement[string(point.MeasurementName)]; ok {
					break
				}
				s.CreateTableFromPoint(buf, point)
				_, err = w.Write(buf.Bytes())
				if err != nil {
					log.Fatalln(err)
				}
				createdMeasurement[string(point.MeasurementName)] = true
			}
		}
	case "fctsdb":
		d.serializer = serializers.NewSerializerInflux()
	}
	if d.needPrePare {
		d.runPrepareData()
	} else {
		if d.mixMode == "read_only" {
			d.simulator.SetWrittenPoints(d.simulator.Total())
		}
	}

	// 在数据准备完后，再次初始化值
	d.itemsRead = 0
	d.valuesRead = 0
	d.bytesRead = 0
	d.queryRead = 0
	d.respCollector = ResponseCollector{}

	return d.workers
}

//这里添加每个client协程中的准备工具，当前没有，后续按需进行添加
func (d *BasicBenchTask) PrepareProcess(i int) {

}

func (d *BasicBenchTask) RunProcess(i int, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	switch d.mixMode {
	case "parallel":
		point := common.MakeUsablePoint()
		endTime := time.Now().Add(d.timeLimit)
		if i >= d.queryPercent*d.workers/100 {
			for time.Now().Before(endTime) {
				err := d.processWrite(d.writers[i], d.batchSize, false, point)
				if err != nil && d.debug {
					log.Println(err.Error())
				}
			}
		} else {
			for time.Now().Before(endTime) {
				err := d.processQuery(d.writers[i], 1, false)
				if err != nil && d.debug {
					log.Println(err.Error())
				}
			}
		}
	case "request":
		point := common.MakeUsablePoint()
		endTime := time.Now().Add(d.timeLimit)
		for time.Now().Before(endTime) {
			num := fastrand.Uint32n(100)
			if num >= uint32(d.queryPercent) {
				err := d.processWrite(d.writers[i], d.batchSize, false, point)
				if err != nil {
					log.Println(err.Error())
				}
			} else {
				err := d.processQuery(d.writers[i], 1, false)
				if err != nil && d.debug {
					log.Println(err.Error())
				}
			}
		}
	case "write_only":
		d.processWriteOnly(i)
	case "read_only":
		d.processQueryOnly(i)
	}
}

func (d *BasicBenchTask) RunSyncTask() {

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		defer ticker.Stop()
		lastTime := time.Now()
		lastItems, lastValues, lastBytes, lastQuery := d.itemsRead, d.valuesRead, d.bytesRead, d.queryRead
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				took := now.Sub(lastTime)
				lastTime = now
				itemsRate := float64(d.itemsRead-lastItems) / took.Seconds()
				bytesRate := float64(d.bytesRead-lastBytes) / took.Seconds()
				valuesRate := float64(d.valuesRead-lastValues) / took.Seconds()
				queryRate := float64(d.queryRead-lastQuery) / took.Seconds()
				lastItems, lastValues, lastBytes, lastQuery = d.itemsRead, d.valuesRead, d.bytesRead, d.queryRead
				switch d.mixMode {
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
			case <-d.inputDone:
				return
			}
		}
	}()
}

func (d *BasicBenchTask) SyncEnd() {
	d.inputDone <- struct{}{}
	close(d.inputDone)
}

func (d *BasicBenchTask) Report(start, end time.Time) map[string]string {
	took := end.Sub(start)
	itemsRead, bytesRead, valuesRead, queryRead := d.itemsRead, d.bytesRead, d.valuesRead, d.queryRead
	itemsRate := float64(itemsRead) / took.Seconds()
	bytesRate := float64(bytesRead) / took.Seconds()
	valuesRate := float64(valuesRead) / took.Seconds()
	queryRate := float64(queryRead) / took.Seconds()
	loadTime := took.Seconds()
	convertedBytesRate := bytesRate / (1 << 20)
	switch d.mixMode {
	case "write_only":
		log.Printf("Total write %d points, %0.2fMB in %.2fsec (mean point rate %.2f/sec, mean value rate %.2f/s, %.2fMB/sec)\n",
			itemsRead, float64(bytesRead)/(1<<20), loadTime, itemsRate, valuesRate, convertedBytesRate)
	case "read_only":
		log.Printf("Total write %d queries in %.2fsec (mean %.2f q/sec)\n",
			queryRead, took.Seconds(), queryRate)
	default:
		log.Printf("Has writen %d point, %.2fMB, %d queries in %0.2f sec (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec, %.2f q/sec)\n",
			itemsRead, float64(bytesRead)/(1<<20), queryRead, took.Seconds(), itemsRate, valuesRate, bytesRate/(1<<20), queryRate)
	}

	d.respCollector.SetStart(start)
	d.respCollector.SetEnd(end)

	groupResult := d.respCollector.GetGroupDetail()
	groupResult.Show()
	result := groupResult.ToMap()
	result["PointRate(p/s)"] = fmt.Sprintf("%.2f", itemsRate)
	result["ValueRate(v/s)"] = fmt.Sprintf("%.2f", valuesRate)
	result["BytesRate(MB/s)"] = fmt.Sprintf("%.2f", convertedBytesRate)
	result["TotalPoints"] = fmt.Sprintf("%d", itemsRead)
	result["UseCase"] = d.useCase
	result["Mod"] = d.mixMode
	result["BatchSize"] = fmt.Sprintf("%d", d.batchSize)
	result["Workers"] = fmt.Sprintf("%d", d.workers)
	result["QueryPercent"] = fmt.Sprintf("%d", d.queryPercent)
	result["Cardinality"] = fmt.Sprintf("%d", d.scaleVar)
	result["SamplingTime"] = d.samplingInterval.String()
	result["Gzip"] = fmt.Sprintf("%d", d.useGzip)

	// buf := bytes.NewBuffer(make([]byte, 0, 1024))
	// jsonEncoder := json.NewEncoder(buf)
	// jsonEncoder.SetEscapeHTML(false)
	// jsonEncoder.Encode(d.sqlTemplate)
	if len(d.sqlTemplate) > 0 {
		result["Sql"] = d.sqlTemplate[0]
	}
	return result
}

func (d *BasicBenchTask) CleanUp() {
	if d.format == "mysql" {
		for i := 0; i < len(d.writers); i++ {
			w := d.writers[i].(*db_client.MysqlClient)
			w.Close()
		}
	}
}

// processWrite reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (d *BasicBenchTask) processWrite(w common.DBClient, batchSize int, useCountLimit bool, point *common.Point) error {
	// var batchesSeen int64
	// 发送http write

	buf := d.bufPool.Get().(*bytes.Buffer)
	var err error
	var batchItemCount int = 0
	var pointMadeIndex int64
	var vaulesWritten int = 0
	for batchItemCount < batchSize {
		pointMadeIndex = d.simulator.Next(point)
		if pointMadeIndex > d.simulator.Total() && useCountLimit { // 以point结束为结束
			break
		}
		if d.format == "mysql" {
			if batchItemCount == 0 {
				buf.Write(append(append([]byte("insert into "), point.MeasurementName...), " values"...))
			} else {
				buf.Write([]byte{','})
			}
		}
		d.serializer.SerializePoint(buf, point)
		batchItemCount++
		vaulesWritten += (len(point.FieldValues) + len(point.Int64FiledValues))
		point.Reset()
	}

	if batchItemCount > 0 {
		if d.format == "mysql" {
			buf.Write([]byte{';'})
		}
		err = d.writeToDb(w, buf)
		if err == nil {
			atomic.AddInt64(&d.bytesRead, int64(buf.Len()))
			atomic.AddInt64(&d.valuesRead, int64(vaulesWritten))
			atomic.AddInt64(&d.itemsRead, int64(batchItemCount))
			d.simulator.SetWrittenPoints(pointMadeIndex)
		}
	}
	buf.Reset()
	d.bufPool.Put(buf)
	return err
}

func (d *BasicBenchTask) processQuery(w common.DBClient, batchSize int, useCountLimit bool) error {
	var err error
	var lat int64
	buf := d.bufPool.Get().(*bytes.Buffer)
	var batchItemCount int = 0
	for batchItemCount < batchSize {
		madeSqlCount := d.simulator.NextSql(buf)
		if madeSqlCount > d.queryCount && useCountLimit {
			break
		}
		batchItemCount++
		if buf.Bytes()[buf.Len()-1] != ';' {
			buf.Write([]byte(";"))
		}
	}

	if batchItemCount > 0 {
		atomic.AddInt64(&d.queryRead, int64(batchItemCount))
		lat, err = w.Query(buf.Bytes())
		if err != nil {
			d.respCollector.AddOne("query", lat, false)
		} else {
			d.respCollector.AddOne("query", lat, true)
		}
	}
	buf.Reset()
	d.bufPool.Put(buf)
	return err
}

func (d *BasicBenchTask) processWriteOnly(i int) {
	point := common.MakeUsablePoint()
	if d.timeLimit > 0 {
		endTime := time.Now().Add(d.timeLimit)
		for time.Now().Before(endTime) {
			err := d.processWrite(d.writers[i], d.batchSize, false, point)
			if err != nil && d.debug {
				log.Println(err.Error())
			}
		}
	} else {
		for !d.simulator.Finished() {
			err := d.processWrite(d.writers[i], d.batchSize, true, point)
			if err != nil && d.debug {
				log.Println(err.Error())
			}
		}
	}
}

func (d *BasicBenchTask) processQueryOnly(i int) {
	if d.timeLimit > 0 {
		endTime := time.Now().Add(d.timeLimit)
		for time.Now().Before(endTime) {
			err := d.processQuery(d.writers[i], d.batchSize, false)
			if err != nil && d.debug {
				log.Println(err.Error())
			}
		}
	} else {
		for d.queryRead < d.queryCount {
			err := d.processQuery(d.writers[i], d.batchSize, true)
			if err != nil && d.debug {
				log.Println(err.Error())
			}
		}
	}
}

func (d *BasicBenchTask) writeToDb(w common.DBClient, buf *bytes.Buffer) error {
	// var batchesSeen int64
	// 发送http write
	var err error
	var lat int64
	if d.useGzip > 0 && d.format == "fctsdb" {
		compressedBatch := d.bufPool.Get().(*bytes.Buffer)
		fasthttp.WriteGzipLevel(compressedBatch, buf.Bytes(), d.useGzip)
		//bodySize = len(compressedBatch.Bytes())
		lat, err = w.Write(compressedBatch.Bytes())
		// Return the compressed batch buffer to the pool.
		compressedBatch.Reset()
		d.bufPool.Put(compressedBatch)
	} else {
		//bodySize = len(batch.Bytes())
		// fmt.Println(string(buf.Bytes()))
		lat, err = w.Write(buf.Bytes())
	}

	if err != nil {
		d.respCollector.AddOne("write", lat, false)
		return fmt.Errorf("error writing: %s", err.Error())
	}
	d.respCollector.AddOne("write", lat, true)
	return nil
}

func (d *BasicBenchTask) createDb(writer common.DBClient) {
	// this also test db connection
	existingDatabases, err := writer.ListDatabases()
	if err != nil {
		log.Println(err)
	}

	if len(existingDatabases) > 0 {
		dbs_string := strings.Join(existingDatabases, ", ")
		log.Printf("The following databases already exist in the data store: %s", dbs_string)
	}

	for _, existingDatabase := range existingDatabases {
		if existingDatabase == d.dbName {
			log.Printf("Database %s already exists", d.dbName)
			return
		}
	}
	err = writer.CreateDb(d.withEncryption)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1000 * time.Millisecond)
	log.Printf("Database %s created", d.dbName)
}

func (d *BasicBenchTask) checkDbConnection(w common.DBClient) error {
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

func (d *BasicBenchTask) runPrepareData() error {
	log.Printf("We will prepare %d points", d.simulator.Total())
	var workersGroup sync.WaitGroup
	prePrareChan := make(chan struct{})
	for i := 0; i < d.workers; i++ {
		workersGroup.Add(1)
		d.PrepareProcess(i)
		go func(w int) {
			defer workersGroup.Done()
			point := common.MakeUsablePoint()
			for !d.simulator.Finished() {
				err := d.processWrite(d.writers[w], 5000, true, point)
				if err != nil && d.debug {
					log.Println(err.Error())
				}
			}
		}(i)
	}
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		defer ticker.Stop()
		lastTime := time.Now()
		lastItems, lastBytes := d.itemsRead, d.bytesRead
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				took := now.Sub(lastTime)
				lastTime = now
				itemsRate := float64(d.itemsRead-lastItems) / took.Seconds()
				bytesRate := float64(d.bytesRead-lastBytes) / took.Seconds()
				lastItems, lastBytes = d.itemsRead, d.bytesRead
				log.Printf("Has prepare %d point, %.2fMB (mean point rate %.2f/sec, %.2fMB/sec in this %0.2f sec)",
					lastItems, float64(lastBytes)/(1<<20), itemsRate, bytesRate/(1<<20), took.Seconds())
			case <-prePrareChan:
				return
			}
		}
	}()
	workersGroup.Wait()
	close(prePrareChan)
	return nil
}
