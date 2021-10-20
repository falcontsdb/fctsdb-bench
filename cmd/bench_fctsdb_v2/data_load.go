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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
)

// TODO AP: Maybe useless
const RateControlGranularity = 1000 // 1000 ms = 1s
const RateControlMinBatchSize = 100

type DataLoad struct {
	// Program option vars:
	csvDaemonUrls string
	daemonUrls    []string
	useGzip       bool
	workers       int
	batchSize     int
	dbName        string
	dataFile      string
	timeLimit     time.Duration
	doDBCreate    bool
	debug         bool

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
	writers               []*HTTPWriter
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
	dataLoad    = &DataLoad{}
	dataLoadCmd = &cobra.Command{
		Use:   "data-load",
		Short: "从文件或者stdin载入数据，并发送数据到数据库，需要先使用data-gen命令",
		Run: func(cmd *cobra.Command, args []string) {
			RunLoad()
		},
		// Hidden: true,
	}
)

func init() {
	dataLoad.Init(dataLoadCmd)
	rootCmd.AddCommand(dataLoadCmd)
}

func RunLoad() int {

	dataLoad.Validate()
	exitCode := 0
	if dataLoad.doDBCreate {
		dataLoad.CreateDb()
	}

	var once sync.Once
	var workersGroup sync.WaitGroup
	syncChanDone := make(chan int)
	dataLoad.PrepareWorkers()

	scanner := dataLoad.GetScanner()
	for i := 0; i < dataLoad.workers; i++ {
		dataLoad.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := dataLoad.RunProcess(w, &workersGroup)
			if err != nil {
				log.Println(err.Error())
				once.Do(func() {
					if !scanner.IsScanFinished() {
						go func() {
							dataLoad.EmptyBatchChanel()
						}()
						syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
		go func(w int) {
			dataLoad.AfterRunProcess(w)
		}(i)
	}
	log.Printf("Started load with %d workers\n", dataLoad.workers)

	start := time.Now()
	scanner.RunScanner(dataLoad.sourceReader, syncChanDone)

	dataLoad.SyncEnd()
	close(syncChanDone)
	workersGroup.Wait()

	dataLoad.CleanUp()

	end := time.Now()
	took := end.Sub(start)

	if dataLoad.dataFile != "" {
		dataLoad.sourceReader.Close()
	}
	itemsRead, bytesRead, valuesRead := scanner.GetReadStatistics()

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	loadTime := took.Seconds()
	convertedBytesRate := bytesRate / (1 << 20)
	log.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, loadTime, dataLoad.workers, itemsRate, valuesRate, convertedBytesRate)

	if exitCode != 0 {
		os.Exit(exitCode)
	}

	return exitCode
}

type Scanner interface {
	RunScanner(r io.Reader, syncChanDone chan int)
	IsScanFinished() bool
	GetReadStatistics() (itemsRead, bytesRead, valuesRead int64)
}

func (l *DataLoad) Init(cmd *cobra.Command) {
	writeFlag := cmd.Flags()
	writeFlag.BoolVar(&l.doDBCreate, "do-db-create", true, "是否创建数据库")
	writeFlag.StringVar(&l.csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	writeFlag.BoolVar(&l.useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	writeFlag.StringVar(&l.dbName, "db", "benchmark_db", "Database name.")
	writeFlag.IntVar(&l.batchSize, "batch-size", 100, "Batch size (1 line of input = 1 item).")
	writeFlag.IntVar(&l.workers, "workers", 1, "Number of parallel requests to make.")
	writeFlag.StringVar(&l.dataFile, "file", "", "Input file")
	writeFlag.BoolVar(&l.debug, "debug", false, "Debug printing (default false).")
	// writeFlag.DurationVar(&l.timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
}

func (l *DataLoad) Validate() {

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

}

func (l *DataLoad) CreateDb() {
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

func (l *DataLoad) PrepareWorkers() {

	l.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	l.batchChan = make(chan batch, l.workers)
	l.inputDone = make(chan struct{})

	l.writers = make([]*HTTPWriter, l.workers)
}

func (l *DataLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *DataLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *DataLoad) CleanUp() {
}

func (l *DataLoad) GetScanner() Scanner {
	return l
}

func (l *DataLoad) PrepareProcess(i int) {

	c := &HTTPWriterConfig{
		Host:      l.daemonUrls[i%len(l.daemonUrls)],
		Database:  l.dbName,
		Debug:     l.debug,
		Gzip:      l.useGzip,
		DebugInfo: fmt.Sprintf("worker #%d", i),
	}

	l.writers[i] = NewHTTPWriter(*c)
}

func (l *DataLoad) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	return l.processBatches(l.writers[i], fmt.Sprintf("%d", i), waitGroup)
}
func (l *DataLoad) AfterRunProcess(i int) {
}

func (l *DataLoad) IsScanFinished() bool {
	return l.scanFinished
}

func (l *DataLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = l.itemsRead
	bytesRead = l.bytesRead
	valuesRead = l.valuesRead
	return
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func (l *DataLoad) RunScanner(r io.Reader, syncChanDone chan int) {
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
func (l *DataLoad) processBatches(w *HTTPWriter, telemetryWorkerLabel string, workersGroup *sync.WaitGroup) error {
	defer workersGroup.Done()
	for batch := range l.batchChan {
		var err error
		if l.useGzip {
			compressedBatch := l.bufPool.Get().(*bytes.Buffer)
			fasthttp.WriteGzip(compressedBatch, batch.Buffer.Bytes())
			//bodySize = len(compressedBatch.Bytes())
			_, err = w.WriteLineProtocol(compressedBatch.Bytes())
			// Return the compressed batch buffer to the pool.
			compressedBatch.Reset()
			l.bufPool.Put(compressedBatch)
		} else {
			//bodySize = len(batch.Bytes())
			_, err = w.WriteLineProtocol(batch.Buffer.Bytes())
		}
		if err != nil {
			return fmt.Errorf("error writing: %s", err.Error())
		}

		// Return the batch buffer to the pool.
		batch.Buffer.Reset()
		l.bufPool.Put(batch.Buffer)
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
	if totalBackoffSecs > 0 {
		log.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	}
	dst <- struct{}{}
	return totalBackoffSecs
}

func (l *DataLoad) createDb(daemonUrl, dbName string) (string, error) {
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
func (l *DataLoad) listDatabases(daemonUrl string) (map[string]string, error) {
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
