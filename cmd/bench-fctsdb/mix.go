// bulk_load_fctsdb loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bytes"
	"encoding/csv"
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
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/fctsdb_query_gen"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
)

type MixReadWrite struct {
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
	agentEndpoint       string
	nmonEndpoint        string
	mixMode             string
	queryPercent        int

	//runtime vars
	timestampStart time.Time
	timestampEnd   time.Time
	daemonUrls     []string
	bufPool        sync.Pool
	pointPool      sync.Pool
	inputDone      chan struct{}
	writers        []*HTTPWriter
	valuesRead     int64
	itemsRead      int64
	bytesRead      int64
	simulator      common.Simulator
	respCollector  ResponseCollector
	queryTypeID    int
	queryCase      *fctsdb.QueryCase
	serializer     common.Serializer
}

var (
	mixReadWrite = &MixReadWrite{}
	MixCmd       = &cobra.Command{
		Use:   "mixed",
		Short: "混合读写测试",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("缺少参数 query-type")
				fmt.Printf("使用命令\"%s query --help\"查看帮助信息\n", TOOL_NAME)
				// cmd.Help()
			} else if len(args) == 1 {
				RunMixWrite(args[0])
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
		},
	}
)

func init() {
	mixReadWrite.Init(MixCmd)
	rootCmd.AddCommand(MixCmd)
}

func RunMixWrite(arg string) {
	csvFileName := time.Now().Format("q-jan2_15-04-05") + ".csv"
	mixReadWrite.Validate()

	if arg == "all" {
		for typeID := 1; typeID <= mixReadWrite.queryCase.Count; typeID++ {
			mixReadWrite.RunWriteOne(typeID)
			if typeID == 1 {
				mixReadWrite.WriteResultToCsv(csvFileName, true)
			} else {
				mixReadWrite.WriteResultToCsv(csvFileName, false)
			}
			if mixReadWrite.agentEndpoint != "" {
				mixReadWrite.AfterRun()
			}
		}
	} else {
		for i, ids := range strings.Split(arg, ",") {
			typeID, err := strconv.Atoi(ids)
			if err != nil {
				fmt.Println("the query-type is unsupported: ", ids)
			} else {
				mixReadWrite.RunWriteOne(typeID)
				if i < 1 {
					mixReadWrite.WriteResultToCsv(csvFileName, true)
				} else {
					mixReadWrite.WriteResultToCsv(csvFileName, false)
				}
				if mixReadWrite.agentEndpoint != "" {
					mixReadWrite.AfterRun()
				}
			}
		}
	}
}

func (d *MixReadWrite) RunWriteOne(typeID int) map[string]string {

	d.format = "fctsdb"
	if d.doDBCreate {
		d.CreateDb()
	}
	d.queryTypeID = typeID
	var workersGroup sync.WaitGroup
	d.PrepareWorkers()
	for i := 0; i < d.workers; i++ {
		d.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			d.RunProcess(w, &workersGroup)
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
	log.Printf("Total write %d points, %0.2fMB in %.2fsec (mean point rate %.2f/sec, mean value rate %.2f/s, %.2fMB/sec)\n",
		itemsRead, float64(bytesRead)/(1<<20), loadTime, itemsRate, valuesRate, convertedBytesRate)
	// d.respCollector.GetDetail().Show()
	d.respCollector.GetGroupDetail().Show()

	result := d.respCollector.GetDetail().ToMap()
	result["PointRate(p/s)"] = fmt.Sprintf("%.2f", itemsRate)
	result["ValueRate(v/s)"] = fmt.Sprintf("%.2f", valuesRate)
	result["BytesRate(MB/s)"] = fmt.Sprintf("%.2f", convertedBytesRate)
	result["Points"] = fmt.Sprintf("%d", itemsRead)
	return result
}

func (d *MixReadWrite) Init(cmd *cobra.Command) {
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
	writeFlag.BoolVar(&d.debug, "debug", false, "是否打印详细日志(default false).")
	writeFlag.StringVar(&d.cpuProfile, "cpu-profile", "", "将cpu-profile信息写入文件的地址，用于自测此工具")
	writeFlag.StringVar(&d.csvDaemonUrls, "urls", "http://localhost:8086", "被测数据库的地址")
	writeFlag.IntVar(&d.useGzip, "gzip", 1, "是否使用gzip,level[0-9],小于0表示不使用")
	writeFlag.StringVar(&d.dbName, "db", "benchmark_db", "数据库的database名称")
	writeFlag.IntVar(&d.batchSize, "batch-size", 100, "1个http请求中携带Point个数")
	writeFlag.IntVar(&d.workers, "workers", 1, "并发的http个数")
	writeFlag.StringVar(&d.agentEndpoint, "agent", "", "数据库代理服务地址，为空表示不使用 (默认不使用)")
	writeFlag.StringVar(&d.nmonEndpoint, "easy-nmon", "", "easy-nmon地址，为空表示不使用监控 (默认不使用)")
	writeFlag.StringVar(&d.mixMode, "mix-mode", "parallel", "混合模式，支持parallel(按线程比例混合)、request(按请求比例混合)")
	writeFlag.IntVar(&d.queryPercent, "query-percent", 0, "查询请求所占百分比 (default 0)")
}

func (d *MixReadWrite) Validate() {
	d.daemonUrls = strings.Split(d.csvDaemonUrls, ",")
	if len(d.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", d.daemonUrls)

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
	if d.useGzip < 0 || d.useGzip > 9 {
		log.Fatal("Invalid gzip level, must bu in 0-9")
	}
	if d.useGzip == 0 {
		log.Println("Close the gzip")
	} else {
		log.Println("Using gzip: level", d.useGzip)
	}

	switch d.useCase {
	case fctsdb.AirQuality.CaseName:
		d.queryCase = fctsdb.AirQuality
	case fctsdb.Vehicle.CaseName:
		d.queryCase = fctsdb.Vehicle
	default:
		log.Fatal("the use-case is unsupported")
	}

	d.serializer = common.NewSerializerInflux()
}

func (d *MixReadWrite) CreateDb() {
	listDatabasesFn := d.listDatabases
	createDbFn := d.createDb

	// this also test db connection
	existingDatabases, err := listDatabasesFn(d.daemonUrls[0])
	if err != nil {
		log.Println(err)
		// log.Fatal("如果被测数据库是mock的，请使用--do-db-create=false跳过此步骤")
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

func (d *MixReadWrite) PrepareWorkers() {

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
	d.writers = make([]*HTTPWriter, d.workers)
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
			SqlTemplates:     []string{d.queryCase.Types[d.queryTypeID].RawSql},
		}
		d.simulator = cfg.ToSimulator()
	case common.UseCaseAirQuality:
		cfg := &airq.AirqSimulatorConfig{
			Start:            d.timestampStart,
			End:              d.timestampEnd,
			SamplingInterval: d.samplingInterval,
			DeviceCount:      d.scaleVar,
			DeviceOffset:     d.scaleVarOffset,
			SqlTemplates:     []string{d.queryCase.Types[d.queryTypeID].RawSql},
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

func (d *MixReadWrite) SyncShowStatistics() {

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		defer ticker.Stop()
		lastTime := time.Now()
		lastItems, lastValues, lastBytes := d.itemsRead, d.valuesRead, d.bytesRead
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				took := now.Sub(lastTime)
				lastTime = now
				itemsRate := float64(d.itemsRead-lastItems) / took.Seconds()
				lastItems = d.itemsRead
				bytesRate := float64(d.bytesRead-lastBytes) / took.Seconds()
				lastBytes = d.bytesRead
				valuesRate := float64(d.valuesRead-lastValues) / took.Seconds()
				lastValues = d.valuesRead
				log.Printf("Has writen %d point, %.2fMB (mean point rate %.2f/sec, value rate %.2f/s, %.2fMB/sec in this %0.2f sec)",
					d.itemsRead, float64(d.bytesRead)/(1<<20), itemsRate, valuesRate, bytesRate/(1<<20), took.Seconds())
			case <-d.inputDone:
				return
			}
		}
	}()
}

func (d *MixReadWrite) SyncEnd() {
	d.inputDone <- struct{}{}
	close(d.inputDone)
}

func (d *MixReadWrite) PrepareProcess(i int) {

	c := &HTTPWriterConfig{
		Host:      d.daemonUrls[i%len(d.daemonUrls)],
		Database:  d.dbName,
		DebugInfo: fmt.Sprintf("worker #%d", i),
	}

	d.writers[i] = NewHTTPWriter(*c)
}

func (d *MixReadWrite) RunProcess(i int, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	point := common.MakeUsablePoint()
	switch d.mixMode {
	case "parallel":
		if i >= d.queryPercent*d.workers/100 {
			for !d.simulator.Finished() {
				err := d.processWrite(d.writers[i], point)
				if err != nil {
					log.Println(err.Error())
				}
			}
		} else {
			for !d.simulator.Finished() {
				err := d.processQuery(d.writers[i])
				if err != nil {
					log.Println(err.Error())
				}
			}
		}
	case "request":
		for !d.simulator.Finished() {
			num := fastrand.Uint32n(100)
			if num >= uint32(d.queryPercent) {
				err := d.processWrite(d.writers[i], point)
				if err != nil {
					log.Println(err.Error())
				}
			} else {
				err := d.processQuery(d.writers[i])
				if err != nil {
					log.Println(err.Error())
				}
			}
		}
	}
}

func (d *MixReadWrite) AfterRunProcess(i int) {
}

func (d *MixReadWrite) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = d.itemsRead
	bytesRead = d.bytesRead
	valuesRead = d.valuesRead
	return
}

func (d *MixReadWrite) WriteResultToCsv(fileName string, writeHead bool) {
	csvFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("open result csv failed, error:", err.Error())
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)

	heads := []string{"UseCase", "TypeID", "P50(ms)", "P90(ms)", "P95(ms)", "P99(ms)", "Min(ms)",
		"Max(ms)", "Avg(ms)", "Fail", "Total", "RunSec(s)", "Qps", "TypeName", "Start", "End"}

	r := d.respCollector.GetDetail().ToMap()
	r["UseCase"] = d.useCase
	r["TypeID"] = fmt.Sprintf("%d", d.queryTypeID)
	r["TypeName"] = d.queryCase.Types[d.queryTypeID].Name

	if writeHead {
		err := csvWriter.Write(heads)
		if err != nil {
			log.Println("write result csv failed, error:", err.Error())
		}
	}

	oneLine := make([]string, len(heads))
	for i := 0; i < len(heads); i++ {
		oneLine[i] = r[heads[i]]
	}
	err = csvWriter.Write(oneLine)
	if err != nil {
		log.Println("write result csv failed, error:", err.Error())
	}
	csvWriter.Flush()
}

func (d *MixReadWrite) AfterRun() {
	if d.agentEndpoint != "" {
		d.httpGet("/stop")
		time.Sleep(time.Second * 5)
		d.httpGet("/start")
	}
}

// processWrite reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (d *MixReadWrite) processWrite(w *HTTPWriter, point *common.Point) error {
	// var batchesSeen int64
	// 发送http write

	buf := d.bufPool.Get().(*bytes.Buffer)
	var err error
	var lat int64
	var batchItemCount int = 0
	var pointMadeIndex int64
	for batchItemCount < d.batchSize {
		pointMadeIndex = d.simulator.Next(point)
		if pointMadeIndex > d.simulator.Total() { // 以point结束为结束
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

		if d.useGzip > 0 {
			compressedBatch := d.bufPool.Get().(*bytes.Buffer)
			fasthttp.WriteGzipLevel(compressedBatch, buf.Bytes(), d.useGzip)
			//bodySize = len(compressedBatch.Bytes())
			lat, err = w.WriteLineProtocol(compressedBatch.Bytes(), true, d.debug)
			// Return the compressed batch buffer to the pool.
			compressedBatch.Reset()
			d.bufPool.Put(compressedBatch)
		} else {
			//bodySize = len(batch.Bytes())
			// fmt.Println(string(buf.Bytes()))
			lat, err = w.WriteLineProtocol(buf.Bytes(), false, d.debug)
		}
		if err != nil {
			d.respCollector.AddOne("write", lat, false)
		} else {
			d.respCollector.AddOne("write", lat, true)
			d.simulator.SetWrittenPoints(pointMadeIndex)
		}
	}
	buf.Reset()
	d.bufPool.Put(buf)
	return err
}

func (d *MixReadWrite) processQuery(w *HTTPWriter) error {
	var err error
	var lat int64
	buf := d.bufPool.Get().(*bytes.Buffer)
	d.simulator.NextSql(buf)
	if d.useGzip > 0 {
		lat, err = w.QueryLineProtocol(buf.Bytes(), true, d.debug)
	} else {
		lat, err = w.QueryLineProtocol(buf.Bytes(), false, d.debug)
	}
	buf.Reset()
	d.bufPool.Put(buf)
	if err != nil {
		d.respCollector.AddOne("query", lat, false)
		return fmt.Errorf("error query: %s", err.Error())
	}
	d.respCollector.AddOne("query", lat, true)
	return nil
}

func (d *MixReadWrite) createDb(daemonUrl, dbName string) (string, error) {
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
func (d *MixReadWrite) listDatabases(daemonUrl string) (map[string]string, error) {
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

func (d *MixReadWrite) httpGet(path string) ([]byte, error) {

	u, err := neturl.Parse(d.agentEndpoint)
	if err != nil {
		log.Fatal("Invalid agent address:", d.agentEndpoint, ", error:", err.Error())
	}
	if u.Scheme == "" {
		log.Fatal("Invalid agent address:", d.agentEndpoint)
	}
	u.Path = path

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	rData, err := ioutil.ReadAll(resp.Body)
	return rData, err
}
