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
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
)

type DataWrite struct {
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
	timestampStart   time.Time
	timestampEnd     time.Time
	daemonUrls       []string
	bufPool          sync.Pool
	pointPool        sync.Pool
	inputDone        chan struct{}
	totalBackOffSecs float64
	configs          []*loadWorkerConfig
	valuesRead       int64
	itemsRead        int64
	bytesRead        int64
	simulator        common.Simulator
	respCollector    ResponseCollector
}

var (
	dataWrite    = &DataWrite{}
	dataWriteCmd = &cobra.Command{
		Use:   "write",
		Short: "生成数据并直接发送至数据库",
		Run: func(cmd *cobra.Command, args []string) {
			if dataWrite.cpuProfile != "" {
				f, err := os.Create(dataWrite.cpuProfile)
				if err != nil {
					log.Fatal("could not create CPU profile: ", err)
				}
				if err := pprof.StartCPUProfile(f); err != nil {
					log.Fatal("could not start CPU profile: ", err)
				}
				defer pprof.StopCPUProfile()
			}
			dataWrite.RunWrite()
		},
	}
)

func init() {
	dataWrite.Init(dataWriteCmd)
	rootCmd.AddCommand(dataWriteCmd)
}

func (d *DataWrite) RunWrite() map[string]string {

	d.format = "fctsdb"
	d.Validate()
	if d.doDBCreate {
		d.CreateDb()
	}

	var workersGroup sync.WaitGroup
	d.PrepareWorkers()
	d.PrepareSimulator()

	for i := 0; i < d.workers; i++ {
		d.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := d.RunProcess(w, &workersGroup)
			if err != nil {
				log.Println(err.Error())
			}
		}(i)

		go func(w int) {
			d.AfterRunProcess(w)
		}(i)
	}
	log.Printf("Started load with %d workers\n", d.workers)

	// 定时运行状态日志
	d.SyncShowStatistics()
	start := time.Now()
	d.respCollector.SetStart(start)
	workersGroup.Wait()
	d.SyncEnd()
	d.CleanUp() // 目前cleanup主要处理背压相关的channel问题
	end := time.Now()
	d.respCollector.SetEnd(end)
	took := end.Sub(start)

	// 总结果输出

	itemsRead, bytesRead, valuesRead := d.GetReadStatistics()

	itemsRate := float64(itemsRead) / took.Seconds()
	bytesRate := float64(bytesRead) / took.Seconds()
	valuesRate := float64(valuesRead) / took.Seconds()

	loadTime := took.Seconds()
	convertedBytesRate := bytesRate / (1 << 20)
	log.Printf("loaded %d items in %fsec with %d workers (mean point rate %.2f/sec, mean value rate %.2f/s, %.2fMB/sec)\n", itemsRead, loadTime, d.workers, itemsRate, valuesRate, convertedBytesRate)
	d.respCollector.GetDetail().Show()

	result := d.respCollector.GetDetail().ToMap()
	result["PointRate(p/s)"] = fmt.Sprintf("%.2f", itemsRate)
	result["ValueRate(v/s)"] = fmt.Sprintf("%.2f", valuesRate)
	result["BytesRate(MB/s)"] = fmt.Sprintf("%.2f", convertedBytesRate)
	result["Points"] = fmt.Sprintf("%d", itemsRead)
	return result
}

func (d *DataWrite) Init(cmd *cobra.Command) {
	writeFlag := cmd.PersistentFlags()
	// writeFlag.StringVar(&d.format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))
	writeFlag.StringVar(&d.useCase, "use-case", CaseChoices[0], fmt.Sprintf("使用的测试场景(可选场景: %s)", strings.Join(CaseChoices, ", ")))
	writeFlag.BoolVar(&d.doDBCreate, "do-db-create", true, "是否创建数据库")
	writeFlag.Int64Var(&d.scaleVar, "scale-var", 1, "场景的变量，一般情况下是场景中模拟机的数量")
	writeFlag.Int64Var(&d.scaleVarOffset, "scale-var-offset", 0, "场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)")
	writeFlag.DurationVar(&d.samplingInterval, "sampling-interval", time.Second, "模拟机的采样时间")
	writeFlag.StringVar(&d.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "模拟机开始采样的时间 (RFC3339)")
	writeFlag.StringVar(&d.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "模拟机采样结束数据 (RFC3339)")
	writeFlag.Int64Var(&d.seed, "seed", 12345678, "全局随机数种子(设置为0是使用当前时间作为随机数种子)")
	writeFlag.IntVar(&d.debug, "debug", 0, "debug日志级别(choices: 0, 1, 2) (default 0).")
	writeFlag.StringVar(&d.cpuProfile, "cpu-profile", "", "将cpu-profile信息写入文件的地址，用于自测此工具")
	writeFlag.StringVar(&d.csvDaemonUrls, "urls", "http://localhost:8086", "被测数据库的地址")
	writeFlag.DurationVar(&d.backoff, "backoff", time.Second, "产生背压的情况下，两次请求时间的等待时间")
	writeFlag.DurationVar(&d.backoffTimeOut, "backoff-timeout", time.Minute*30, "一次测试中，背压等待累积的最大时间")
	writeFlag.BoolVar(&d.useGzip, "gzip", true, "是否使用gzip")
	writeFlag.StringVar(&d.dbName, "db", "benchmark_db", "数据库的database名称")
	writeFlag.IntVar(&d.batchSize, "batch-size", 100, "1个http请求中携带Point个数")
	writeFlag.IntVar(&d.workers, "workers", 1, "并发的http个数")

	// writeFlag.DurationVar(&d.timeLimit, "time-limit", -1, "最大测试时间(-1表示无限制)")
}

func (d *DataWrite) Validate() {

	d.daemonUrls = strings.Split(d.csvDaemonUrls, ",")
	if len(d.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", d.daemonUrls)

	if d.timeLimit > 0 && d.backoffTimeOut > d.timeLimit {
		d.backoffTimeOut = d.timeLimit
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

func (d *DataWrite) CreateDb() {
	listDatabasesFn := d.listDatabases
	createDbFn := d.createDb

	// this also test db connection
	existingDatabases, err := listDatabasesFn(d.daemonUrls[0])
	if err != nil {
		log.Println(err)
		log.Fatal("如果被测数据库是mock的，请使用--do-db-create=false跳过此步骤")
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

func (d *DataWrite) PrepareWorkers() {

	d.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	d.pointPool = sync.Pool{
		New: func() interface{} {
			return common.MakeUsablePoint()
		},
	}
	d.inputDone = make(chan struct{})
	d.configs = make([]*loadWorkerConfig, d.workers)
	d.itemsRead = 0
	d.valuesRead = 0
	d.bytesRead = 0
	d.respCollector = ResponseCollector{}

}

func (d *DataWrite) PrepareSimulator() {

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

	log.Printf("We will write %d points", d.simulator.Total())
}

func (d *DataWrite) SyncShowStatistics() {
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				itemsRead, bytesRead, valuesRead := d.GetReadStatistics()
				log.Printf("Has writen %d point, %d values, %.2fMB", itemsRead, valuesRead, float64(bytesRead)/(1<<20))
			case <-d.inputDone:
				return
			}
		}
	}()
}

func (d *DataWrite) SyncEnd() {
	d.inputDone <- struct{}{}
	close(d.inputDone)
}

func (d *DataWrite) CleanUp() {
	for _, c := range d.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	d.totalBackOffSecs = float64(0)
	for i := 0; i < d.workers; i++ {
		d.totalBackOffSecs += d.configs[i].backingOffSecs
	}
}

func (d *DataWrite) PrepareProcess(i int) {
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

func (d *DataWrite) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	defer waitGroup.Done()

	var batchItemCount int
	var err error
	// newline := []byte("\n")
	buf := d.bufPool.Get().(*bytes.Buffer)

	batchItemCount = 0
	point := common.MakeUsablePoint()
	for d.simulator.Next(point) {
		atomic.AddInt64(&d.valuesRead, int64(len(point.FieldValues)))
		pointByte := SerializePoint(point)
		buf.Write(pointByte)
		batchItemCount++
		pointByte = pointByte[:0]
		scratchBufPool.Put(pointByte)
		point.Reset()

		// 达到batchSize
		if batchItemCount >= d.batchSize {
			err = d.processBatches(d.configs[i].writer, buf, d.configs[i].backingOffChan, fmt.Sprintf("%d", i))
			atomic.AddInt64(&d.bytesRead, int64(buf.Len()))
			atomic.AddInt64(&d.itemsRead, int64(batchItemCount))
			// Return the point buffer to the pool.
			batchItemCount = 0
			buf.Reset()
			d.bufPool.Put(buf)
			buf = d.bufPool.Get().(*bytes.Buffer)
		}

	}
	if batchItemCount > 0 {
		err = d.processBatches(d.configs[i].writer, buf, d.configs[i].backingOffChan, fmt.Sprintf("%d", i))
		atomic.AddInt64(&d.bytesRead, int64(buf.Len()))
		atomic.AddInt64(&d.itemsRead, int64(batchItemCount))
		buf.Reset()
		d.bufPool.Put(buf)
	}
	return err
}

func (d *DataWrite) AfterRunProcess(i int) {
	d.configs[i].backingOffSecs = processBackoffMessages(i, d.configs[i].backingOffChan, d.configs[i].backingOffDone)
}

func (d *DataWrite) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = d.itemsRead
	bytesRead = d.bytesRead
	valuesRead = d.valuesRead
	return
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (d *DataWrite) processBatches(w *HTTPWriter, buf *bytes.Buffer, backoffSrc chan bool, telemetryWorkerLabel string) error {
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
		// log.Println(err.Error())
		return fmt.Errorf("error writing: %s", err.Error())
	}
	d.respCollector.AddOne(w.c.Database, lat, true)
	return nil
}

func (d *DataWrite) createDb(daemonUrl, dbName string) (string, error) {
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
func (d *DataWrite) listDatabases(daemonUrl string) (map[string]string, error) {
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
				Values [][]interface{}
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
		ret[nestedName[0].(string)] = ""
	}
	return ret, nil
}

var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

func SerializePoint(p *common.Point) []byte {
	buf := scratchBufPool.Get().([]byte)
	buf = append(buf, p.MeasurementName...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, p.TagValues[i]...)
	}

	if len(p.FieldKeys) > 0 {
		buf = append(buf, ' ')
	}

	for i := 0; i < len(p.FieldKeys); i++ {
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

	buf = append(buf, ' ')
	buf = fastFormatAppend(p.Timestamp.UTC().UnixNano(), buf, true)
	buf = append(buf, '\n')

	// buf = buf[:0]
	// scratchBufPool.Put(buf)

	return buf
}

func fastFormatAppend(v interface{}, buf []byte, singleQuotesForString bool) []byte {
	var quotationChar = "\""
	if singleQuotesForString {
		quotationChar = "'"
	}
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', 16, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', 16, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, quotationChar...)
		buf = append(buf, v.([]byte)...)
		buf = append(buf, quotationChar...)
		return buf
	case string:
		buf = append(buf, quotationChar...)
		buf = append(buf, v.(string)...)
		buf = append(buf, quotationChar...)
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}