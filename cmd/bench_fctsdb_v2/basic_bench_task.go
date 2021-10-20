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

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/fctsdb_query_gen"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
	"github.com/valyala/fasthttp"
)

type BasicBenchTask struct {
	// Program option vars:
	csvDaemonUrls       string
	useGzip             int
	workers             int
	batchSize           int
	dbName              string
	timeLimit           time.Duration
	format              string
	useCase             string
	scaleVar            int64
	scaleVarOffset      int64
	samplingInterval    time.Duration
	timestampStartStr   string
	timestampEndStr     string
	timestampPrepareStr string
	seed                int64
	debug               bool
	cpuProfile          string
	doDBCreate          bool
	mixMode             string
	queryPercent        int
	queryType           int
	queryCount          int64

	//runtime vars
	timestampStart   time.Time
	timestampEnd     time.Time
	timestampPrepare time.Time
	daemonUrls       []string
	bufPool          sync.Pool
	inputDone        chan struct{}
	writers          []DBWriter
	valuesRead       int64
	itemsRead        int64
	bytesRead        int64
	qureyRead        int64
	simulator        common.Simulator
	respCollector    ResponseCollector
	sqlTemplate      []string
	serializer       common.Serializer
}

func (d *BasicBenchTask) Validate() {
	d.daemonUrls = strings.Split(d.csvDaemonUrls, ",")
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
	// d.timestampPrepare, err = time.Parse(time.RFC3339, d.timestampPrepareStr)
	// if err != nil {
	// 	log.Fatalln("parse prepare error: ", err)
	// }
	// d.timestampPrepare = d.timestampPrepare.UTC()
	// if d.timestampPrepare.Before(d.timestampStart) || d.timestampPrepare.After(d.timestampEnd) {
	// 	log.Fatalln("the prepare time > ")
	// }

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
	d.writers = make([]DBWriter, d.workers)
	d.itemsRead = 0
	d.valuesRead = 0
	d.bytesRead = 0
	d.respCollector = ResponseCollector{}

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
		log.Printf("We will write %s", d.timeLimit)
	} else {
		if d.mixMode != "read_only" {
			log.Printf("We will write %d points", d.simulator.Total())
		} else {
			log.Printf("We will query %d sql", d.queryCount)
		}
	}

	if d.mixMode == "read_only" {
		d.simulator.SetWrittenPoints(d.simulator.Total())
	}

	c := &HTTPWriterConfig{
		Host:     d.daemonUrls[0],
		Database: d.dbName,
	}
	writer := NewHTTPWriter(*c)

	err := d.checkDbConnection(writer)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if d.doDBCreate {
		d.createDb(writer)
	}
	d.serializer = common.NewSerializerInflux()
	return d.workers
}

func (d *BasicBenchTask) PrepareProcess(i int) {

	c := &HTTPWriterConfig{
		Host:      d.daemonUrls[i%len(d.daemonUrls)],
		Database:  d.dbName,
		Gzip:      d.useGzip > 0,
		Debug:     d.debug,
		DebugInfo: fmt.Sprintf("worker #%d", i),
	}

	d.writers[i] = NewHTTPWriter(*c)
}

func (d *BasicBenchTask) RunProcess(i int, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	switch d.mixMode {
	case "parallel":
		point := common.MakeUsablePoint()
		if d.timeLimit > 0 {
			endTime := time.Now().Add(d.timeLimit)
			if i >= d.queryPercent*d.workers/100 {
				for time.Now().Before(endTime) {
					err := d.processWrite(d.writers[i], d.batchSize, false, point)
					if err != nil {
						log.Println(err.Error())
					}
				}
			} else {
				for time.Now().Before(endTime) {
					err := d.processQuery(d.writers[i], 1, false)
					if err != nil {
						log.Println(err.Error())
					}
				}
			}
		} else {
			if i >= d.queryPercent*d.workers/100 {
				for !d.simulator.Finished() {
					err := d.processWrite(d.writers[i], d.batchSize, true, point)
					if err != nil {
						log.Println(err.Error())
					}
				}
			} else {
				for !d.simulator.Finished() {
					err := d.processQuery(d.writers[i], 1, false)
					if err != nil {
						log.Println(err.Error())
					}
				}
			}
		}
	case "request":
		point := common.MakeUsablePoint()
		if d.timeLimit > 0 {
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
					if err != nil {
						log.Println(err.Error())
					}
				}
			}
		} else {
			for !d.simulator.Finished() {
				num := fastrand.Uint32n(100)
				if num >= uint32(d.queryPercent) {
					err := d.processWrite(d.writers[i], d.batchSize, true, point)
					if err != nil {
						log.Println(err.Error())
					}
				} else {
					err := d.processQuery(d.writers[i], 1, false)
					if err != nil {
						log.Println(err.Error())
					}
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
		lastItems, lastValues, lastBytes, lastQuery := d.itemsRead, d.valuesRead, d.bytesRead, d.qureyRead
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				took := now.Sub(lastTime)
				lastTime = now
				itemsRate := float64(d.itemsRead-lastItems) / took.Seconds()
				bytesRate := float64(d.bytesRead-lastBytes) / took.Seconds()
				valuesRate := float64(d.valuesRead-lastValues) / took.Seconds()
				queryRate := float64(d.qureyRead-lastQuery) / took.Seconds()
				lastItems, lastValues, lastBytes, lastQuery = d.itemsRead, d.valuesRead, d.bytesRead, d.qureyRead
				switch d.mixMode {
				case "write_only":
					log.Printf("Has writen %d point, %.2fMB (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec in this %0.2f sec)",
						lastItems, float64(lastBytes)/(1<<20), itemsRate, valuesRate, bytesRate/(1<<20), took.Seconds())
				case "read_only":
					log.Printf("Has writen %d queries,  (mean %.2f q/sec in this %0.2f sec)\n",
						lastQuery, queryRate, took.Seconds())
				default:
					log.Printf("Has writen %d point, %.2fMB, %d queries (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec, %.2f q/sec in this %0.2f sec)\n",
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
	itemsRead, bytesRead, valuesRead, queryRead := d.itemsRead, d.bytesRead, d.valuesRead, d.qureyRead
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
}

// processWrite reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (d *BasicBenchTask) processWrite(w DBWriter, batchSize int, useCountLimit bool, point *common.Point) error {
	// var batchesSeen int64
	// 发送http write

	buf := d.bufPool.Get().(*bytes.Buffer)
	var err error
	var batchItemCount int = 0
	var pointMadeIndex int64
	for batchItemCount < batchSize {
		pointMadeIndex = d.simulator.Next(point)
		if pointMadeIndex > d.simulator.Total() && useCountLimit { // 以point结束为结束
			break
		}
		d.serializer.SerializePoint(buf, point)
		batchItemCount++
		atomic.AddInt64(&d.valuesRead, int64(len(point.FieldValues)+len(point.Int64FiledValues)))
		point.Reset()
	}

	if batchItemCount > 0 {
		atomic.AddInt64(&d.bytesRead, int64(buf.Len()))
		atomic.AddInt64(&d.itemsRead, int64(batchItemCount))
		err = d.writeToDb(w, buf)
		if err == nil {
			d.simulator.SetWrittenPoints(pointMadeIndex)
		}
	}
	buf.Reset()
	d.bufPool.Put(buf)
	return err
}

func (d *BasicBenchTask) processQuery(w DBWriter, batchSize int, useCountLimit bool) error {
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
		atomic.AddInt64(&d.qureyRead, int64(batchItemCount))
		lat, err = w.QueryLineProtocol(buf.Bytes())
		if err != nil {
			d.respCollector.AddOne("query", lat, false)
		}
		d.respCollector.AddOne("query", lat, true)
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
			if err != nil {
				log.Println(err.Error())
			}
		}
	} else {
		for !d.simulator.Finished() {
			err := d.processWrite(d.writers[i], d.batchSize, true, point)
			if err != nil {
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
			if err != nil {
				log.Println(err.Error())
			}
		}
	} else {
		for d.qureyRead < d.queryCount {
			err := d.processQuery(d.writers[i], d.batchSize, true)
			if err != nil {
				log.Println(err.Error())
			}
		}
	}
}

func (d *BasicBenchTask) writeToDb(w DBWriter, buf *bytes.Buffer) error {
	// var batchesSeen int64
	// 发送http write
	var err error
	var lat int64
	if d.useGzip > 0 {
		compressedBatch := d.bufPool.Get().(*bytes.Buffer)
		fasthttp.WriteGzipLevel(compressedBatch, buf.Bytes(), d.useGzip)
		//bodySize = len(compressedBatch.Bytes())
		lat, err = w.WriteLineProtocol(compressedBatch.Bytes())
		// Return the compressed batch buffer to the pool.
		compressedBatch.Reset()
		d.bufPool.Put(compressedBatch)
	} else {
		//bodySize = len(batch.Bytes())
		// fmt.Println(string(buf.Bytes()))
		lat, err = w.WriteLineProtocol(buf.Bytes())
	}

	if err != nil {
		d.respCollector.AddOne("write", lat, false)
		return fmt.Errorf("error writing: %s", err.Error())
	}
	d.respCollector.AddOne("write", lat, true)
	return nil
}

func (d *BasicBenchTask) createDb(writer DBWriter) {
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
	err = writer.CreateDb()
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1000 * time.Millisecond)
	log.Printf("Database %s created", d.dbName)
}

func (d *BasicBenchTask) checkDbConnection(w DBWriter) error {
	for i := 0; i < 12; i++ {
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
