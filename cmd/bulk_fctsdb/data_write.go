// bulk_load_fctsdb loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/report"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
)

// TODO AP: Maybe useless
const RateControlGranularity = 1000 // 1000 ms = 1s
const RateControlMinBatchSize = 100

type InfluxBulkLoad struct {
	// Program option vars:
	csvDaemonUrls   string
	daemonUrls      []string
	ingestRateLimit int
	backoff         time.Duration
	backoffTimeOut  time.Duration
	useGzip         bool
	workers         int
	batchSize       int
	dbName          string
	dataFile        string
	timeLimit       time.Duration

	//runtime vars
	bufPool               sync.Pool
	batchChan             chan batch
	inputDone             chan struct{}
	progressIntervalItems uint64
	ingestionRateGran     float64
	maxBatchSize          int
	speedUpRequest        int32
	scanFinished          bool
	totalBackOffSecs      float64
	configs               []*workerConfig
	valuesRead            int64
	itemsRead             int64
	bytesRead             int64
	sourceReader          *os.File
}

type batch struct {
	Buffer *bytes.Buffer
	Items  int
	Values int
}

var (
	influxLoad   = &InfluxBulkLoad{}
	dataWriteCmd = &cobra.Command{
		Use:   "write",
		Short: "write the data to db",
		Run: func(cmd *cobra.Command, args []string) {
			RunWrite()
		},
	}
)

func (l *InfluxBulkLoad) Init(cmd *cobra.Command) {
	writeFlag := cmd.Flags()
	writeFlag.StringVar(&l.csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	writeFlag.DurationVar(&l.backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	writeFlag.DurationVar(&l.backoffTimeOut, "backoff-timeout", time.Minute*30, "Maximum time to spent when dealing with backoff messages in one shot")
	writeFlag.BoolVar(&l.useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	writeFlag.IntVar(&l.ingestRateLimit, "ingest-rate-limit", -1, "Ingest rate limit in values/s (-1 = no limit).")
	writeFlag.StringVar(&l.dbName, "db", "benchmark_db", "Database name.")
	writeFlag.IntVar(&l.batchSize, "batch-size", 100, "Batch size (1 line of input = 1 item).")
	writeFlag.IntVar(&l.workers, "workers", 1, "Number of parallel requests to make.")
	writeFlag.StringVar(&l.dataFile, "file", "", "Input file")
	writeFlag.DurationVar(&l.timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
}

func init() {
	influxLoad.Init(dataWriteCmd)
	rootCmd.AddCommand(dataWriteCmd)
}

func RunWrite() int {

	influxLoad.Validate()
	exitCode := 0

	influxLoad.CreateDb()

	var once sync.Once
	var workersGroup sync.WaitGroup
	syncChanDone := make(chan int)
	influxLoad.PrepareWorkers()

	scanner := influxLoad.GetScanner()
	for i := 0; i < influxLoad.workers; i++ {
		influxLoad.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := influxLoad.RunProcess(w, &workersGroup)
			if err != nil {
				log.Println(err.Error())
				once.Do(func() {
					if !scanner.IsScanFinished() {
						go func() {
							influxLoad.EmptyBatchChanel()
						}()
						syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
		go func(w int) {
			influxLoad.AfterRunProcess(w)
		}(i)
	}
	log.Printf("Started load with %d workers\n", influxLoad.workers)

	start := time.Now()
	scanner.RunScanner(influxLoad.sourceReader, syncChanDone)

	influxLoad.SyncEnd()
	close(syncChanDone)
	workersGroup.Wait()

	influxLoad.CleanUp()

	end := time.Now()
	took := end.Sub(start)

	if influxLoad.dataFile != "" {
		influxLoad.sourceReader.Close()
	}
	itemsRead, bytesRead, valuesRead := scanner.GetReadStatistics()

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	loadTime := took.Seconds()
	convertedBytesRate := bytesRate / (1 << 20)
	log.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, loadTime, influxLoad.workers, itemsRate, valuesRate, convertedBytesRate)

	if exitCode != 0 {
		os.Exit(exitCode)
	}

	return exitCode
}

type workerConfig struct {
	url            string
	backingOffChan chan bool
	backingOffDone chan struct{}
	writer         *HTTPWriter
	backingOffSecs float64
}

type Scanner interface {
	RunScanner(r io.Reader, syncChanDone chan int)
	IsScanFinished() bool
	GetReadStatistics() (itemsRead, bytesRead, valuesRead int64)
}

func (l *InfluxBulkLoad) Validate() {

	if l.dataFile != "" {
		if f, err := os.Open(l.dataFile); err == nil {
			l.sourceReader = f
		} else {
			log.Fatalf("Error opening %s: %v\n", l.dataFile, err)
		}
	}
	if l.sourceReader == nil {
		l.sourceReader = os.Stdin
	}

	l.daemonUrls = strings.Split(l.csvDaemonUrls, ",")
	if len(l.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", l.daemonUrls)

	if l.ingestRateLimit > 0 {
		l.ingestionRateGran = (float64(l.ingestRateLimit) / float64(l.batchSize)) / (float64(1000) / float64(RateControlGranularity))
		log.Printf("Using worker ingestion rate %v values/%v ms", l.ingestionRateGran, RateControlGranularity)
	} else {
		log.Print("Ingestion rate control is off")
	}

	if l.timeLimit > 0 && l.backoffTimeOut > l.timeLimit {
		l.backoffTimeOut = l.timeLimit
	}

}

func (l *InfluxBulkLoad) CreateDb() {
	listDatabasesFn := l.listDatabases
	createDbFn := l.createDb

	// this also test db connection
	existingDatabases, err := listDatabasesFn(l.daemonUrls[0])
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
	id, ok := existingDatabases[l.dbName]
	if ok {
		log.Printf("Database %s [%s] already exists", l.dbName, id)
	} else {
		id, err = createDbFn(l.daemonUrls[0], l.dbName)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1000 * time.Millisecond)
		log.Printf("Database %s [%s] created", l.dbName, id)
	}

}

func (l *InfluxBulkLoad) PrepareWorkers() {

	l.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	l.batchChan = make(chan batch, l.workers)
	l.inputDone = make(chan struct{})

	l.configs = make([]*workerConfig, l.workers)
}

func (l *InfluxBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *InfluxBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *InfluxBulkLoad) CleanUp() {
	for _, c := range l.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	l.totalBackOffSecs = float64(0)
	for i := 0; i < l.workers; i++ {
		l.totalBackOffSecs += l.configs[i].backingOffSecs
	}
}

func (l *InfluxBulkLoad) GetScanner() Scanner {
	return l
}

func (l *InfluxBulkLoad) PrepareProcess(i int) {
	l.configs[i] = &workerConfig{
		url:            l.daemonUrls[i%len(l.daemonUrls)],
		backingOffChan: make(chan bool, 100),
		backingOffDone: make(chan struct{}),
	}
	var url string
	c := &HTTPWriterConfig{
		DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, l.configs[i].url),
		Host:           l.configs[i].url,
		Database:       l.dbName,
		BackingOffChan: l.configs[i].backingOffChan,
		BackingOffDone: l.configs[i].backingOffDone,
	}
	url = c.Host + "/write?db=" + neturl.QueryEscape(c.Database)

	l.configs[i].writer = NewHTTPWriter(*c, url)
}

func (l *InfluxBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	return l.processBatches(l.configs[i].writer, l.configs[i].backingOffChan, fmt.Sprintf("%d", i), waitGroup)
}
func (l *InfluxBulkLoad) AfterRunProcess(i int) {
	l.configs[i].backingOffSecs = processBackoffMessages(i, l.configs[i].backingOffChan, l.configs[i].backingOffDone)
}

func (l *InfluxBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {

	reportTags = [][2]string{{"back_off", strconv.Itoa(int(l.backoff.Seconds()))}}

	extraVals = make([]report.ExtraVal, 0)

	if l.ingestRateLimit > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "ingest_rate_limit_values", Value: l.ingestRateLimit})
	}
	if l.totalBackOffSecs > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "total_backoff_secs", Value: l.totalBackOffSecs})
	}

	params.DBType = "InfluxDB"
	params.DestinationUrl = l.csvDaemonUrls
	params.IsGzip = l.useGzip

	return
}

func (l *InfluxBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

func (l *InfluxBulkLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = l.itemsRead
	bytesRead = l.bytesRead
	valuesRead = l.valuesRead
	return
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func (l *InfluxBulkLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0
	buf := l.bufPool.Get().(*bytes.Buffer)

	var n, values int
	var totalPoints, totalValues, totalValuesCounted int64

	newline := []byte("\n")
	var deadline time.Time
	if l.timeLimit > 0 {
		deadline = time.Now().Add(l.timeLimit)
	}

	var batchItemCount uint64
	var err error
	scanner := bufio.NewScanner(bufio.NewReaderSize(r, 4*1024*1024))
outer:
	for scanner.Scan() {
		line := scanner.Text()
		totalPoints, totalValues, err = common.CheckTotalValues(line)
		if totalPoints > 0 || totalValues > 0 {
			continue
		} else {
			fieldCnt := countFields(line)
			values += fieldCnt
			totalValuesCounted += int64(fieldCnt)
		}
		if err != nil {
			log.Fatal(err)
		}
		l.itemsRead++
		batchItemCount++

		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		if n >= l.batchSize {
			atomic.AddUint64(&l.progressIntervalItems, batchItemCount)
			batchItemCount = 0

			l.bytesRead += int64(buf.Len())
			l.batchChan <- batch{buf, n, values}
			buf = l.bufPool.Get().(*bytes.Buffer)
			n = 0
			values = 0

			if l.timeLimit > 0 && time.Now().After(deadline) {
				// bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}

			if l.ingestRateLimit > 0 {
				if l.batchSize < l.maxBatchSize {
					hint := atomic.LoadInt32(&l.speedUpRequest)
					if hint > int32(l.workers*2) { // we should wait for more requests (and this is just a magic number)
						atomic.StoreInt32(&l.speedUpRequest, 0)
						l.batchSize += int(float32(l.maxBatchSize) * 0.10)
						if l.batchSize > l.maxBatchSize {
							l.batchSize = l.maxBatchSize
						}
						log.Printf("Increased batch size to %d\n", l.batchSize)
					}
				}
			}
		}
		select {
		case <-syncChanDone:
			break outer
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		l.batchChan <- batch{buf, n, values}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)

	l.valuesRead = totalValues
	if totalValues == 0 {
		l.valuesRead = totalValuesCounted
	}
	if l.itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if l.timeLimit > 0 {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", l.itemsRead, totalPoints)
		}
	}
	l.scanFinished = true
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *InfluxBulkLoad) processBatches(w *HTTPWriter, backoffSrc chan bool, telemetryWorkerLabel string, workersGroup *sync.WaitGroup) error {
	// var batchesSeen int64

	// Ingestion rate control vars
	var gvCount float64
	var gvStart time.Time

	defer workersGroup.Done()

	for batch := range l.batchChan {
		// batchesSeen++

		//var bodySize int
		// ts := time.Now().UnixNano()

		if l.ingestRateLimit > 0 && gvStart.Nanosecond() == 0 {
			gvStart = time.Now()
		}

		// Write the batch: try until backoff is not needed.

		var err error
		sleepTime := l.backoff
		timeStart := time.Now()
		for {
			if l.useGzip {
				compressedBatch := l.bufPool.Get().(*bytes.Buffer)
				fasthttp.WriteGzip(compressedBatch, batch.Buffer.Bytes())
				//bodySize = len(compressedBatch.Bytes())
				_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
				// Return the compressed batch buffer to the pool.
				compressedBatch.Reset()
				l.bufPool.Put(compressedBatch)
			} else {
				//bodySize = len(batch.Bytes())
				_, err = w.WriteLineProtocol(batch.Buffer.Bytes(), false)
			}

			if err == ErrorBackoff {
				backoffSrc <- true
				// Report telemetry, if applicable:
				time.Sleep(sleepTime)
				sleepTime += l.backoff        // sleep longer if backpressure comes again
				if sleepTime > 10*l.backoff { // but not longer than 10x default backoff time
					log.Printf("[worker %s] sleeping on backoff response way too long (10x %v)", telemetryWorkerLabel, l.backoff)
					sleepTime = 10 * l.backoff
				}
				checkTime := time.Now()
				if timeStart.Add(l.backoffTimeOut).Before(checkTime) {
					log.Printf("[worker %s] Spent too much time in backoff: %ds\n", telemetryWorkerLabel, int64(checkTime.Sub(timeStart).Seconds()))
					break
				}
			} else {
				backoffSrc <- false
				break
			}
		}
		if err != nil {
			return fmt.Errorf("error writing: %s", err.Error())
		}

		// lagMillis intentionally includes backoff time,
		// and incidentally includes compression time:
		// lagMillis := float64(time.Now().UnixNano()-ts) / 1e6
		var lagMillis float64

		// Return the batch buffer to the pool.
		batch.Buffer.Reset()
		l.bufPool.Put(batch.Buffer)

		// Normally report after each batch
		// reportStat := true
		valuesWritten := float64(batch.Values)

		// Apply ingest rate control if set
		if l.ingestRateLimit > 0 {
			gvCount += valuesWritten
			if gvCount >= l.ingestionRateGran {
				now := time.Now()
				elapsed := now.Sub(gvStart)
				overDelay := (gvCount - l.ingestionRateGran) / (l.ingestionRateGran / float64(RateControlGranularity))
				remainingMs := RateControlGranularity - (elapsed.Nanoseconds() / 1e6) + int64(overDelay)
				valuesWritten = gvCount
				lagMillis = float64(elapsed.Nanoseconds() / 1e6)
				if remainingMs > 0 {
					time.Sleep(time.Duration(remainingMs) * time.Millisecond)
					gvStart = time.Now()
					realDelay := gvStart.Sub(now).Nanoseconds() / 1e6 // 'now' was before sleep
					lagMillis += float64(realDelay)
				} else {
					gvStart = now
					atomic.AddInt32(&l.speedUpRequest, 1)
				}
				gvCount = 0
			}
		}
	}

	return nil
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) float64 {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Since(start)
			log.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	log.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
	return totalBackoffSecs
}

func (l *InfluxBulkLoad) createDb(daemonUrl, dbName string) (string, error) {
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
func (l *InfluxBulkLoad) listDatabases(daemonUrl string) (map[string]string, error) {
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

// countFields return number of fields in protocol line
func countFields(line string) int {
	lineParts := strings.Split(line, " ") // "measurement,tags fields timestamp"
	if len(lineParts) != 3 {
		log.Fatalf("invalid protocol line: '%s'", line)
	}
	fieldCnt := strings.Count(lineParts[1], "=")
	if fieldCnt == 0 {
		log.Fatalf("invalid fields parts: '%s'", lineParts[1])
	}
	return fieldCnt
}
