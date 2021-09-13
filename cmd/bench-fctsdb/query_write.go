package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/fctsdb_query_gen"
	"github.com/spf13/cobra"
)

type QueryWrite struct {
	useCase           string
	scaleVar          int64
	scaleVarOffset    int64
	timestampStartStr string
	timestampEndStr   string
	seed              int64
	queryCount        int64

	samplingInterval time.Duration
	csvDaemonUrls    string
	daemonUrls       []string
	workers          int
	batchSize        int
	dbName           string
	timeLimit        time.Duration
	debug            bool
	toCsv            bool

	//runtime vars
	bufPool          sync.Pool
	batchChan        chan batch
	inputDone        chan struct{}
	scanFinished     bool
	totalBackOffSecs float64
	configs          []*loadWorkerConfig
	itemsRead        int64
	respCollector    ResponseCollector
	timestampStart   time.Time
	timestampEnd     time.Time
	queryCase        *fctsdb.QueryCase
	simulatorInfo    common.Simulator
	queryTypeID      int
}

var (
	queryWrite    = &QueryWrite{}
	queryWriteCmd = &cobra.Command{
		Use:   "query <query-types>",
		Short: "生成查询语句并直接发送至数据库",
		// Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("缺少参数 query-type")
				fmt.Printf("使用命令\"%s query --help\"查看帮助信息\n", TOOL_NAME)
				// cmd.Help()
			} else if len(args) == 1 {
				RunQueryWrite(args[0])
			}
		},
		Example: fmt.Sprintf("%s list   获取场景（case）和查询类型（query-type）\n"+
			"%s query 1 --use-case vehicle    测试单个查询类型的\n"+
			"%s query 1,2,7,9 --use-case vehicle   按顺序执行1,2,7,9查询类型的测试\n"+
			"%s query all --use-case vehicle   按顺序执行某个场景（case）的所有查询测试\n",
			TOOL_NAME, TOOL_NAME, TOOL_NAME, TOOL_NAME),
	}
)

func init() {
	queryWrite.Init(queryWriteCmd)
	rootCmd.AddCommand(queryWriteCmd)
}

func RunQueryWrite(arg string) {
	csvFileName := time.Now().Format("Q-Jan2_15-04-05") + ".csv"
	queryWrite.Validate()
	if arg == "all" {
		for typeID := 1; typeID <= queryWrite.queryCase.Count; typeID++ {
			queryWrite.RunOneQueryType(typeID)
			if typeID == 1 {
				queryWrite.WriteResultToCsv(csvFileName, true)
			} else {
				queryWrite.WriteResultToCsv(csvFileName, false)
			}
		}
	} else {
		for i, ids := range strings.Split(arg, ",") {
			typeID, err := strconv.Atoi(ids)
			if err != nil {
				fmt.Println("the query-type is unsupported: ", ids)
			} else {
				queryWrite.RunOneQueryType(typeID)
				if i < 1 {
					queryWrite.WriteResultToCsv(csvFileName, true)
				} else {
					queryWrite.WriteResultToCsv(csvFileName, false)
				}
			}
		}
	}
}

func (q *QueryWrite) Init(cmd *cobra.Command) {
	writeFlag := cmd.Flags()
	writeFlag.StringVar(&q.useCase, "use-case", CaseChoices[0], fmt.Sprintf("使用的测试场景(可选场景: %s)", strings.Join(CaseChoices, ", ")))
	writeFlag.Int64Var(&q.scaleVar, "scale-var", 1, "场景的变量，一般情况下是场景中模拟机的数量")
	writeFlag.Int64Var(&q.scaleVarOffset, "scale-var-offset", 0, "场景偏移量，一般情况下是模拟机的起始MN编号 (default 0)")
	writeFlag.BoolVar(&q.toCsv, "to-csv", false, "是否记录结果到csv文件")
	writeFlag.DurationVar(&q.samplingInterval, "sampling-interval", time.Second, "模拟机的采样时间")
	writeFlag.Int64Var(&q.queryCount, "query-count", 1000, "生成的查询语句数量")
	writeFlag.StringVar(&q.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "模拟机开始采样的时间 (RFC3339)")
	writeFlag.StringVar(&q.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "模拟机采样结束数据 (RFC3339)")
	writeFlag.Int64Var(&q.seed, "seed", 12345678, "全局随机数种子(设置为0是使用当前时间作为随机数种子)")
	writeFlag.StringVar(&q.csvDaemonUrls, "urls", "http://localhost:8086", "被测数据库的地址")
	writeFlag.StringVar(&q.dbName, "db", "benchmark_db", "数据库的database名称")
	writeFlag.IntVar(&q.batchSize, "batch-size", 1, "1个http请求中携带查询语句个数")
	writeFlag.IntVar(&q.workers, "workers", 1, "并发的http个数")
	writeFlag.DurationVar(&q.timeLimit, "time-limit", -1, "最大测试时间(-1表示无限制)，设置后会使query-count参数失效")
	writeFlag.BoolVar(&q.debug, "debug", false, "是否需要打印debug日志")
}

func (q *QueryWrite) Validate() {

	if q.seed == 0 {
		q.seed = int64(time.Now().Nanosecond())
	}
	log.Printf("using random seed %d\n", q.seed)
	rand.Seed(q.seed)

	// Parse timestamps:
	var err error
	q.timestampStart, err = time.Parse(time.RFC3339, q.timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	q.timestampStart = q.timestampStart.UTC()
	q.timestampEnd, err = time.Parse(time.RFC3339, q.timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	q.timestampEnd = q.timestampEnd.UTC()

	if q.samplingInterval <= 0 {
		log.Fatal("Invalid sampling interval")
	}

	log.Printf("Using sampling interval %v\n", q.samplingInterval)

	q.daemonUrls = strings.Split(q.csvDaemonUrls, ",")
	if len(q.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", q.daemonUrls)

	switch q.useCase {
	case fctsdb.AirQuality.CaseName:
		q.queryCase = fctsdb.AirQuality
		cfg := &airq.AirqSimulatorConfig{
			Start:            q.timestampStart,
			End:              q.timestampEnd,
			SamplingInterval: q.samplingInterval,
			AirqDeviceCount:  q.scaleVar,
			AirqDeviceOffset: q.scaleVarOffset,
		}
		q.simulatorInfo = cfg.ToSimulator()
	case fctsdb.Vehicle.CaseName:
		q.queryCase = fctsdb.Vehicle
		cfg := &vehicle.VehicleSimulatorConfig{
			Start:            q.timestampStart,
			End:              q.timestampEnd,
			SamplingInterval: q.samplingInterval,
			VehicleCount:     q.scaleVar,
			VehicleOffset:    q.scaleVarOffset,
		}
		q.simulatorInfo = cfg.ToSimulator()
	default:
		log.Fatal("the use-case is unsupported")
	}
}

func (q *QueryWrite) RunOneQueryType(typeID int) {
	log.Printf("*****************************************************************************************")
	log.Printf("Run the case: %s, query type id: %d, name: %s\n", q.useCase, typeID, q.queryCase.Types[typeID].Name)
	var workersGroup sync.WaitGroup
	q.queryTypeID = typeID
	q.PrepareWorkers()
	q.respCollector.SetStart(time.Now())
	for i := 0; i < q.workers; i++ {
		q.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := q.RunProcess(w, &workersGroup)
			if err != nil {
				log.Fatal(err.Error())
			}
		}(i)
		go func(w int) {
			q.AfterRunProcess(w)
		}(i)
	}

	go func() {
		queryWrite.RunQueryGenerate()
	}()

	log.Printf("Started load with %d workers\n", queryWrite.workers)
	workersGroup.Wait()
	queryWrite.respCollector.SetEnd(time.Now())
	queryWrite.GetRespResult()
}

func (q *QueryWrite) PrepareWorkers() {

	q.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, q.batchSize*1024))
		},
	}

	q.batchChan = make(chan batch, 5*q.workers)
	q.inputDone = make(chan struct{})
	q.respCollector = ResponseCollector{}
	q.configs = make([]*loadWorkerConfig, q.workers)
}

func (q *QueryWrite) PrepareProcess(i int) {
	q.configs[i] = &loadWorkerConfig{
		url: q.daemonUrls[i%len(q.daemonUrls)],
		// backingOffChan: make(chan bool, 100),
		// backingOffDone: make(chan struct{}),
	}
	var url string
	c := &HTTPWriterConfig{
		DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", i, q.configs[i].url),
		Host:      q.configs[i].url,
		Database:  q.dbName,
		// BackingOffChan: q.configs[i].backingOffChan,
		// BackingOffDone: q.configs[i].backingOffDone,
	}
	url = c.Host + "/query?db=" + neturl.QueryEscape(c.Database)

	q.configs[i].writer = NewHTTPWriter(*c, url)
}

func (q *QueryWrite) EmptyBatchChanel() {
	for range q.batchChan {
		//read out remaining batches
	}
}

func (q *QueryWrite) SyncEnd() {
	<-q.inputDone
	close(q.batchChan)
}

func (q *QueryWrite) CleanUp() {
	for _, c := range q.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	q.totalBackOffSecs = float64(0)
	for i := 0; i < q.workers; i++ {
		q.totalBackOffSecs += q.configs[i].backingOffSecs
	}
}

func (q *QueryWrite) RunQueryGenerate() {

	var queryType *fctsdb.QueryType
	var ok bool

	queryType, ok = q.queryCase.Types[q.queryTypeID]
	if !ok {
		log.Fatal("the query-type out of range")
	}

	err := queryType.Generator.Init(q.simulatorInfo)
	if err != nil {
		log.Println(err.Error())
		close(q.batchChan)
		return
	}

	buf := q.bufPool.Get().(*bytes.Buffer)

	// 采用时间控制
	if q.timeLimit > 0 {
		endTime := time.Now().Add(q.timeLimit)
		n := 0
		for time.Now().Before(endTime) {
			sql := queryType.Generator.Next()
			if sql[len(sql)-1] == ';' {
				buf.Write([]byte(neturl.QueryEscape(sql)))
			} else {
				buf.Write([]byte(neturl.QueryEscape(sql)))
				buf.Write([]byte(neturl.QueryEscape(";")))
			}
			n++
			if n >= q.batchSize {
				q.batchChan <- batch{buf, n, 0}
				buf = q.bufPool.Get().(*bytes.Buffer)
				n = 0
			}
		}
		close(q.batchChan)
		for range q.batchChan { // 将batchChan剩下的查询语句全部读取完
		}

		// 采用数量控制
	} else {
		n := 0
		for i := 0; i < int(q.queryCount); i++ {
			sql := queryType.Generator.Next()
			if sql[len(sql)-1] == ';' {
				buf.Write([]byte(neturl.QueryEscape(sql)))
			} else {
				buf.Write([]byte(neturl.QueryEscape(sql)))
				buf.Write([]byte(neturl.QueryEscape(";")))
			}
			n++
			if n >= q.batchSize {
				q.batchChan <- batch{buf, n, 0}
				buf = q.bufPool.Get().(*bytes.Buffer)
				n = 0
			}
		}

		if n > 0 {
			q.batchChan <- batch{buf, n, 0}
		}
		close(q.batchChan)
	}
}

func (q *QueryWrite) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	return q.processBatches(q.configs[i].writer, q.configs[i].backingOffChan, fmt.Sprintf("%d", i), waitGroup)
}

func (q *QueryWrite) AfterRunProcess(i int) {

}

func (q *QueryWrite) IsScanFinished() bool {
	return q.scanFinished
}

func (q *QueryWrite) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = q.itemsRead
	return
}

func (q *QueryWrite) GetRespResult() {
	fmt.Println()
	q.respCollector.GetDetail().Show()
	fmt.Println()
}

func (q *QueryWrite) WriteResultToCsv(fileName string, writeHead bool) {
	csvFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("open result csv failed, error:", err.Error())
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)

	heads := []string{"UseCase", "TypeID", "P50(ms)", "P90(ms)", "P95(ms)", "P99(ms)", "Min(ms)",
		"Max(ms)", "Avg(ms)", "Fail", "Total", "RunSec(s)", "Qps", "TypeName"}

	r := q.respCollector.GetDetail().ToMap()
	r["UseCase"] = q.useCase
	r["TypeID"] = fmt.Sprintf("%d", q.queryTypeID)
	r["TypeName"] = q.queryCase.Types[q.queryTypeID].Name

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

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (q *QueryWrite) processBatches(w *HTTPWriter, backoffSrc chan bool, telemetryWorkerLabel string, workersGroup *sync.WaitGroup) error {
	// var batchesSeen int64

	defer workersGroup.Done()

	for batch := range q.batchChan {

		buf := q.bufPool.Get().(*bytes.Buffer)
		buf.Write(w.url)
		buf.Write([]byte("&q="))
		buf.Write(batch.Buffer.Bytes())

		lat, err := w.QueryLineProtocol(buf.Bytes(), q.debug)
		if err != nil {
			q.respCollector.AddOne(w.c.Database, lat, false)
			return fmt.Errorf("error writing: %s", err.Error())
		}
		q.respCollector.AddOne(w.c.Database, lat, true)
		batch.Buffer.Reset()
		q.bufPool.Put(batch.Buffer)
		buf.Reset()
		q.bufPool.Put(buf)
	}

	return nil
}
