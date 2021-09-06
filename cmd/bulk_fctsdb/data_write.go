// bulk_load_fctsdb loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	neturl "net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/report"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
)

type DataWriteBenchmark struct {
	// Program option vars:
	csvDaemonUrls     string
	backoff           time.Duration
	backoffTimeOut    time.Duration
	useGzip           bool
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
	debug             int
	cpuProfile        string
	doDBCreate        bool

	//runtime vars
	timestampStart    time.Time
	timestampEnd      time.Time
	daemonUrls        []string
	bufPool           sync.Pool
	pointByteChan     chan *[]byte
	pointPool         sync.Pool
	inputDone         chan struct{}
	totalBackOffSecs  float64
	configs           []*loadWorkerConfig
	valuesRead        int64
	parallelSimulator int64
	itemsRead         int64
	bytesRead         int64
	simulator         common.Simulator
	respCollector     ResponseCollector
}

var (
	dataWriteBenchmark = &DataWriteBenchmark{}
	dataWriteCmd       = &cobra.Command{
		Use:   "write",
		Short: "generate data and write the data to db",
		Run: func(cmd *cobra.Command, args []string) {
			RunWrite()
		},
	}
)

func init() {
	dataWriteBenchmark.Init(dataWriteCmd)
	dataCmd.AddCommand(dataWriteCmd)
}

func RunWrite() {

	dataWriteBenchmark.Validate()
	if dataWriteBenchmark.doDBCreate {
		dataWriteBenchmark.CreateDb()
	}

	var workersGroup sync.WaitGroup

	dataWriteBenchmark.PrepareWorkers()
	dataWriteBenchmark.PrepareSimulator()

	for i := 0; i < dataWriteBenchmark.workers; i++ {
		dataWriteBenchmark.PrepareProcess(i)
		dataWriteBenchmark.parallelSimulator++
		workersGroup.Add(1)

		go func(w int) {
			err := dataWriteBenchmark.RunProcess(w, &workersGroup)
			if err != nil {
				log.Println(err.Error())
			}
		}(i)
		go func(w int) {
			dataWriteBenchmark.AfterRunProcess(w)
		}(i)
	}
	log.Printf("Started load with %d workers\n", dataWriteBenchmark.workers)

	start := time.Now()
	dataWriteBenchmark.RunSimulator()
	dataWriteBenchmark.respCollector.SetStart(start)
	workersGroup.Wait()
	dataWriteBenchmark.CleanUp()
	end := time.Now()
	dataWriteBenchmark.respCollector.SetEnd(end)
	took := end.Sub(start)

	itemsRead, bytesRead, valuesRead := dataWriteBenchmark.GetReadStatistics()

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	loadTime := took.Seconds()
	convertedBytesRate := bytesRate / (1 << 20)
	log.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, loadTime, dataWriteBenchmark.workers, itemsRate, valuesRate, convertedBytesRate)
	dataWriteBenchmark.respCollector.ShowDetail()
}

func (d *DataWriteBenchmark) Init(cmd *cobra.Command) {
	writeFlag := cmd.Flags()
	writeFlag.StringVar(&d.format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))
	writeFlag.StringVar(&d.useCase, "use-case", CaseChoices[0], fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(CaseChoices, ", ")))
	writeFlag.BoolVar(&d.doDBCreate, "do-db-create", true, "Whether to create the database.")
	writeFlag.Int64Var(&d.scaleVar, "scale-var", 1, "Scaling variable specific to the use case.")
	writeFlag.Int64Var(&d.scaleVarOffset, "scale-var-offset", 0, "Scaling variable offset specific to the use case.")
	writeFlag.DurationVar(&d.samplingInterval, "sampling-interval", time.Second, "Simulated sampling interval.")
	writeFlag.StringVar(&d.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	writeFlag.StringVar(&d.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	writeFlag.Int64Var(&d.seed, "seed", 12345678, "PRNG seed (default 12345678, or 0, uses the current timestamp).")
	writeFlag.IntVar(&d.debug, "debug", 0, "Debug printing (choices: 0, 1, 2) (default 0).")
	writeFlag.StringVar(&d.cpuProfile, "cpu-profile", "", "Write CPU profile to `file`")
	writeFlag.StringVar(&d.csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	writeFlag.DurationVar(&d.backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	writeFlag.DurationVar(&d.backoffTimeOut, "backoff-timeout", time.Minute*30, "Maximum time to spent when dealing with backoff messages in one shot")
	writeFlag.BoolVar(&d.useGzip, "gzip", false, "Whether to gzip encode requests (default true).")
	writeFlag.StringVar(&d.dbName, "db", "benchmark_db", "Database name.")
	writeFlag.IntVar(&d.batchSize, "batch-size", 100, "Batch size (1 line of input = 1 item).")
	writeFlag.IntVar(&d.workers, "workers", 1, "Number of parallel requests to make.")
	writeFlag.DurationVar(&d.timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
}

func (d *DataWriteBenchmark) Validate() {

	d.daemonUrls = strings.Split(d.csvDaemonUrls, ",")
	if len(d.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", d.daemonUrls)

	if d.timeLimit > 0 && d.backoffTimeOut > d.timeLimit {
		d.backoffTimeOut = d.timeLimit
	}

	validFormat := false
	for _, s := range formatChoices {
		if s == d.format {
			validFormat = true
			break
		}
	}
	if !validFormat {
		log.Fatalf("invalid format specifier: %v", d.format)
	}

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
		log.Fatal(err)
	}
	d.timestampStart = d.timestampStart.UTC()
	d.timestampEnd, err = time.Parse(time.RFC3339, d.timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	d.timestampEnd = d.timestampEnd.UTC()

	if d.samplingInterval <= 0 {
		log.Fatal("Invalid sampling interval")
	}

	log.Printf("Using sampling interval %v\n", d.samplingInterval)

}

func (d *DataWriteBenchmark) CreateDb() {
	listDatabasesFn := d.listDatabases
	createDbFn := d.createDb

	// this also test db connection
	existingDatabases, err := listDatabasesFn(d.daemonUrls[0])
	if err != nil {
		log.Fatal(err)
	}

	delete(existingDatabases, "_internal")
	if len(existingDatabases) > 0 {
		var dbs []string
		for key := range existingDatabases {
			dbs = append(dbs, key)
		}
		dbs_string := strings.Join(dbs, ", ")

		log.Printf("The following databases already exist in the data store: %s", dbs_string)

	}

	var id string
	id, ok := existingDatabases[d.dbName]
	if ok {
		log.Printf("Database %s [%s] already exists", d.dbName, id)
	} else {
		id, err = createDbFn(d.daemonUrls[0], d.dbName)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1000 * time.Millisecond)
		log.Printf("Database %s [%s] created", d.dbName, id)
	}

}

func (d *DataWriteBenchmark) PrepareWorkers() {

	d.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	d.pointByteChan = make(chan *[]byte, 100*d.workers)
	d.pointPool = sync.Pool{
		New: func() interface{} {
			return common.MakeUsablePoint()
		},
	}
	d.inputDone = make(chan struct{})
	d.configs = make([]*loadWorkerConfig, d.workers)

}

func (d *DataWriteBenchmark) PrepareSimulator() {

	switch d.useCase {
	case common.UseCaseVehicle:
		cfg := &vehicle.VehicleSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.samplingInterval,
			VehicleCount:     d.scaleVar,
			VehicleOffset:    d.scaleVarOffset,
		}
		d.simulator = cfg.ToSimulator()
	case common.UseCaseAirQuality:
		cfg := &airq.AirqSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.samplingInterval,
			AirqDeviceCount:  d.scaleVar,
			AirqDeviceOffset: d.scaleVarOffset,
		}
		d.simulator = cfg.ToSimulator()
	case common.UseCaseDevOps:
		cfg := &devops.DevopsSimulatorConfig{
			Start: d.timestampStart,
			End:   d.timestampEnd,
			// SamplingInterval: d.samplingInterval,
			HostCount:  d.scaleVar,
			HostOffset: d.scaleVarOffset,
		}
		d.simulator = cfg.ToSimulator()

	default:
		panic("unreachable")
	}
}

func (d *DataWriteBenchmark) EmptyPointChanel() {
	for range d.pointByteChan {
	}
}

func (d *DataWriteBenchmark) SyncEnd() {
	<-d.inputDone
	close(d.pointByteChan)
}

func (d *DataWriteBenchmark) CleanUp() {
	for _, c := range d.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	d.totalBackOffSecs = float64(0)
	for i := 0; i < d.workers; i++ {
		d.totalBackOffSecs += d.configs[i].backingOffSecs
	}
}

func (d *DataWriteBenchmark) PrepareProcess(i int) {
	d.configs[i] = &loadWorkerConfig{
		url:            d.daemonUrls[i%len(d.daemonUrls)],
		backingOffChan: make(chan bool, 100),
		backingOffDone: make(chan struct{}),
	}
	var url string
	c := &HTTPWriterConfig{
		DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, d.configs[i].url),
		Host:           d.configs[i].url,
		Database:       d.dbName,
		BackingOffChan: d.configs[i].backingOffChan,
		BackingOffDone: d.configs[i].backingOffDone,
	}
	url = c.Host + "/write?db=" + neturl.QueryEscape(c.Database)

	d.configs[i].writer = NewHTTPWriter(*c, url)
}

func (d *DataWriteBenchmark) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	defer waitGroup.Done()

	var batchItemCount int
	var err error
	newline := []byte("\n")
	buf := d.bufPool.Get().(*bytes.Buffer)

	batchItemCount = 0
	for pointByte := range d.pointByteChan {
		// mutex.Lock()
		// fmt.Println(string(*pointByte))
		// mutex.Unlock()
		buf.Write(*pointByte)
		buf.Write(newline)
		batchItemCount++

		// 达到batchSize
		if batchItemCount >= d.batchSize {
			batchItemCount = 0
			atomic.AddInt64(&d.bytesRead, int64(buf.Len()))
			err = d.processBatches(d.configs[i].writer, buf, d.configs[i].backingOffChan, fmt.Sprintf("%d", i))

			// Return the point buffer to the pool.
			buf.Reset()
			d.bufPool.Put(buf)
			buf = d.bufPool.Get().(*bytes.Buffer)
		}

	}
	if batchItemCount > 0 {
		atomic.AddInt64(&d.bytesRead, int64(buf.Len()))
		err = d.processBatches(d.configs[i].writer, buf, d.configs[i].backingOffChan, fmt.Sprintf("%d", i))
	}
	return err
}

func (d *DataWriteBenchmark) AfterRunProcess(i int) {
	d.configs[i].backingOffSecs = processBackoffMessages(i, d.configs[i].backingOffChan, d.configs[i].backingOffDone)
}

func (d *DataWriteBenchmark) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {

	reportTags = [][2]string{{"back_off", strconv.Itoa(int(d.backoff.Seconds()))}}

	extraVals = make([]report.ExtraVal, 0)

	if d.totalBackOffSecs > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "total_backoff_secs", Value: d.totalBackOffSecs})
	}

	params.DBType = "InfluxDB"
	params.DestinationUrl = d.csvDaemonUrls
	params.IsGzip = d.useGzip

	return
}

func (d *DataWriteBenchmark) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = d.itemsRead
	bytesRead = d.bytesRead
	valuesRead = d.valuesRead
	return
}

func (d *DataWriteBenchmark) Write(p []byte) (n int, err error) {
	b := make([]byte, len(p))
	copy(b, p)
	d.pointByteChan <- &b
	// fmt.Println("write", len(p))
	return len(p), nil
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func (d *DataWriteBenchmark) RunSimulator() {

	var serializer common.Serializer
	switch d.format {
	case "influx-bulk":
		serializer = common.NewSerializerInflux()
	case "es-bulk":
		serializer = common.NewSerializerElastic("5x")
	case "es-bulk6x":
		serializer = common.NewSerializerElastic("6x")
	case "es-bulk7x":
		serializer = common.NewSerializerElastic("7x")
	case "cassandra":
		serializer = common.NewSerializerCassandra()
	case "mongo":
		serializer = common.NewSerializerMongo()
	case "opentsdb":
		serializer = common.NewSerializerOpenTSDB()
	case "timescaledb-sql":
		serializer = common.NewSerializerTimescaleSql()
	case "timescaledb-copyFrom":
		serializer = common.NewSerializerTimescaleBin()
	case "graphite-line":
		serializer = common.NewSerializerGraphiteLine()
	case "splunk-json":
		serializer = common.NewSerializerSplunkJson()
	default:
		panic("unreachable")
	}

	count := runtime.NumCPU() / 4
	if count < 1 {
		count = 1
	}
	var wg sync.WaitGroup
	pointChan := make(chan *common.Point, 1000)
	// 单倍协程生成point
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !d.simulator.Finished() {
				point := d.pointPool.Get().(*common.Point)
				d.simulator.Next(point)
				pointChan <- point
			}
		}()
	}

	var wg2 sync.WaitGroup
	// 双倍协程将point转为为[]byte
	for i := 0; i < 2*count; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for point := range pointChan {
				err := serializer.SerializePoint(d, point)
				if err != nil {
					log.Fatal(err)
				}
				point.Reset()
				d.pointPool.Put(point)
			}
		}()
	}

	wg.Wait()
	d.itemsRead = d.simulator.SeenPoints()
	d.valuesRead = d.simulator.SeenValues()
	close(pointChan)
	wg2.Wait()
	close(d.pointByteChan)

}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (d *DataWriteBenchmark) processBatches(w *HTTPWriter, buf *bytes.Buffer, backoffSrc chan bool, telemetryWorkerLabel string) error {
	// var batchesSeen int64
	// 发送http write
	var err error
	sleepTime := d.backoff
	timeStart := time.Now()
	var lat int64
	for {
		if d.useGzip {
			compressedBatch := d.bufPool.Get().(*bytes.Buffer)
			fasthttp.WriteGzip(compressedBatch, buf.Bytes())
			//bodySize = len(compressedBatch.Bytes())
			lat, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
			// Return the compressed batch buffer to the pool.
			compressedBatch.Reset()
			d.bufPool.Put(compressedBatch)
		} else {
			//bodySize = len(batch.Bytes())
			// fmt.Println(string(buf.Bytes()))
			lat, err = w.WriteLineProtocol(buf.Bytes(), false)
		}
		if err == ErrorBackoff {
			backoffSrc <- true
			// Report telemetry, if applicable:
			time.Sleep(sleepTime)
			sleepTime += d.backoff        // sleep longer if backpressure comes again
			if sleepTime > 10*d.backoff { // but not longer than 10x default backoff time
				log.Printf("[worker %s] sleeping on backoff response way too long (10x %v)", telemetryWorkerLabel, d.backoff)
				sleepTime = 10 * d.backoff
			}
			checkTime := time.Now()
			if timeStart.Add(d.backoffTimeOut).Before(checkTime) {
				log.Printf("[worker %s] Spent too much time in backoff: %ds\n", telemetryWorkerLabel, int64(checkTime.Sub(timeStart).Seconds()))
				break
			}
		} else {
			backoffSrc <- false
			break
		}
	}
	if err != nil {
		d.respCollector.AddOne(w.c.Database, lat, false)
		return fmt.Errorf("error writing: %s", err.Error())
	}
	d.respCollector.AddOne(w.c.Database, lat, true)
	return nil
}

func (d *DataWriteBenchmark) createDb(daemonUrl, dbName string) (string, error) {
	u, err := neturl.Parse(daemonUrl)
	if err != nil {
		return "", err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s", dbName))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("createDb returned status code: %v", resp.StatusCode)
	}
	return "", nil
}

// listDatabases lists the existing databases in InfluxDB.
func (d *DataWriteBenchmark) listDatabases(daemonUrl string) (map[string]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases get error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listDatabases returned status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("listDatabases readAll error: %s", err.Error())
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	// {"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]} for 1.8.4
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, fmt.Errorf("listDatabases unmarshal error: %s", err.Error())
	}

	ret := make(map[string]string)
	for _, nestedName := range listing.Results[0].Series[0].Values {
		ret[nestedName[0]] = ""
	}
	return ret, nil
}
