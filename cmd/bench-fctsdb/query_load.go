package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	neturl "net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

type QueryLoad struct {
	csvDaemonUrls   string
	daemonUrls      []string
	ingestRateLimit int
	backoff         time.Duration
	backoffTimeOut  time.Duration
	workers         int
	batchSize       int
	dbName          string
	dataFile        string
	timeLimit       time.Duration
	debug           bool

	//runtime vars
	bufPool               sync.Pool
	batchChan             chan batch
	inputDone             chan struct{}
	progressIntervalItems uint64
	scanFinished          bool
	totalBackOffSecs      float64
	configs               []*loadWorkerConfig
	itemsRead             int64
	sourceReader          *os.File
	respCollector         ResponseCollector
}

var (
	queryLoad    = &QueryLoad{}
	queryLoadCmd = &cobra.Command{
		Use:   "query-load",
		Short: "从文件或者stdin载入查询语句，并发送查询到数据库，需要先使用query-gen命令",
		Run: func(cmd *cobra.Command, args []string) {
			RunQueryLoad()
		},
		// Hidden: true,
	}
)

func init() {
	queryLoad.Init(queryLoadCmd)
	rootCmd.AddCommand(queryLoadCmd)
}

func RunQueryLoad() {
	queryLoad.Validate()
	exitCode := 0

	var once sync.Once
	var workersGroup sync.WaitGroup
	syncChanDone := make(chan int)
	queryLoad.PrepareWorkers()

	queryLoad.respCollector.SetStart(time.Now())
	for i := 0; i < queryLoad.workers; i++ {
		queryLoad.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := queryLoad.RunProcess(w, &workersGroup)
			if err != nil {
				log.Println(err.Error())
				once.Do(func() {
					if !queryLoad.IsScanFinished() {
						go func() {
							queryLoad.EmptyBatchChanel()
						}()
						syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
		go func(w int) {
			queryLoad.AfterRunProcess(w)
		}(i)
	}
	log.Printf("Started load with %d workers\n", queryLoad.workers)

	// start := time.Now()
	queryLoad.RunScanner(queryLoad.sourceReader, syncChanDone)

	queryLoad.SyncEnd()
	close(syncChanDone)
	workersGroup.Wait()
	queryLoad.respCollector.SetEnd(time.Now())

	queryLoad.CleanUp()

	if queryLoad.dataFile != "" {
		queryLoad.sourceReader.Close()
	}

	queryLoad.GetRespResult()

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func (q *QueryLoad) Init(cmd *cobra.Command) {
	writeFlag := cmd.Flags()

	writeFlag.StringVar(&q.csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	writeFlag.DurationVar(&q.backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	writeFlag.DurationVar(&q.backoffTimeOut, "backoff-timeout", time.Minute*30, "Maximum time to spent when dealing with backoff messages in one shot")
	writeFlag.IntVar(&q.ingestRateLimit, "ingest-rate-limit", -1, "Ingest rate limit in queries/s (-1 = no limit).")
	writeFlag.StringVar(&q.dbName, "db", "benchmark_db", "Database name.")
	writeFlag.IntVar(&q.batchSize, "batch-size", 1, "Batch size (1 line of input = 1 item).")
	writeFlag.IntVar(&q.workers, "workers", 1, "Number of parallel requests to make.")
	writeFlag.StringVar(&q.dataFile, "file", "", "Input file")
	writeFlag.DurationVar(&q.timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	writeFlag.BoolVar(&q.debug, "debug", false, "Debug printing (default false).")
}

func (q *QueryLoad) Validate() {

	if q.dataFile != "" {
		if f, err := os.Open(q.dataFile); err == nil {
			q.sourceReader = f
		} else {
			log.Fatalf("Error opening %s: %v\n", q.dataFile, err)
		}
	}
	if q.sourceReader == nil {
		q.sourceReader = os.Stdin
	}

	q.daemonUrls = strings.Split(q.csvDaemonUrls, ",")
	if len(q.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", q.daemonUrls)

	if q.ingestRateLimit > 0 {
		log.Printf("Using worker ingestion rate %v queries/s", q.ingestRateLimit)
	} else {
		log.Print("Ingestion rate control is off")
	}

	if q.timeLimit > 0 && q.backoffTimeOut > q.timeLimit {
		q.backoffTimeOut = q.timeLimit
	}

}

func (q *QueryLoad) PrepareProcess(i int) {
	q.configs[i] = &loadWorkerConfig{
		url:            q.daemonUrls[i%len(q.daemonUrls)],
		backingOffChan: make(chan bool, 100),
		backingOffDone: make(chan struct{}),
	}
	var url string
	c := &HTTPWriterConfig{
		DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, q.configs[i].url),
		Host:           q.configs[i].url,
		Database:       q.dbName,
		BackingOffChan: q.configs[i].backingOffChan,
		BackingOffDone: q.configs[i].backingOffDone,
	}
	url = c.Host + "/query?db=" + neturl.QueryEscape(c.Database)

	q.configs[i].writer = NewHTTPWriter(*c, url)
}

func (q *QueryLoad) PrepareWorkers() {

	q.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	q.batchChan = make(chan batch, q.workers)
	q.inputDone = make(chan struct{})

	q.configs = make([]*loadWorkerConfig, q.workers)
}

func (q *QueryLoad) EmptyBatchChanel() {
	for range q.batchChan {
		//read out remaining batches
	}
}

func (q *QueryLoad) SyncEnd() {
	<-q.inputDone
	close(q.batchChan)
}

func (q *QueryLoad) CleanUp() {
	for _, c := range q.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	q.totalBackOffSecs = float64(0)
	for i := 0; i < q.workers; i++ {
		q.totalBackOffSecs += q.configs[i].backingOffSecs
	}
}

func (q *QueryLoad) GetScanner() Scanner {
	return q
}

func (q *QueryLoad) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	return q.processBatches(q.configs[i].writer, q.configs[i].backingOffChan, fmt.Sprintf("%d", i), waitGroup)
}
func (q *QueryLoad) AfterRunProcess(i int) {
	q.configs[i].backingOffSecs = processBackoffMessages(i, q.configs[i].backingOffChan, q.configs[i].backingOffDone)
}

func (q *QueryLoad) IsScanFinished() bool {
	return q.scanFinished
}

func (q *QueryLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = q.itemsRead
	// bytesRead = q.bytesRead
	// valuesRead = q.valuesRead
	return
}

func (q *QueryLoad) GetRespResult() {
	// fmt.Println(q.respCollector.GetDetail())
	q.respCollector.GetDetail().Show()
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func (q *QueryLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	q.scanFinished = false
	q.itemsRead = 0
	// q.bytesRead = 0
	// q.valuesRead = 0
	buf := q.bufPool.Get().(*bytes.Buffer)

	var n int

	// newline := []byte("\n")
	var deadline time.Time
	if q.timeLimit > 0 {
		deadline = time.Now().Add(q.timeLimit)
	}

	var batchItemCount uint64
	scanner := bufio.NewScanner(bufio.NewReaderSize(r, 4*1024*1024))
outer:
	for scanner.Scan() {

		q.itemsRead++
		batchItemCount++
		sql := scanner.Text()

		if sql[len(sql)-1] == ';' {
			buf.Write([]byte(sql))
		} else {
			buf.Write([]byte(sql))
			buf.Write([]byte(";"))
		}

		n++
		if n >= q.batchSize {
			atomic.AddUint64(&q.progressIntervalItems, batchItemCount)
			batchItemCount = 0

			// q.bytesRead += int64(buf.Len())
			q.batchChan <- batch{buf, n, 0}
			buf = q.bufPool.Get().(*bytes.Buffer)
			n = 0

			if q.timeLimit > 0 && time.Now().After(deadline) {
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
		q.batchChan <- batch{buf, n, 0}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(q.inputDone)
	q.scanFinished = true
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (q *QueryLoad) processBatches(w *HTTPWriter, backoffSrc chan bool, telemetryWorkerLabel string, workersGroup *sync.WaitGroup) error {
	// var batchesSeen int64

	defer workersGroup.Done()
	for batch := range q.batchChan {
		buf := q.bufPool.Get().(*bytes.Buffer)
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
