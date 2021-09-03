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

type InfluxQueryLoad struct {
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
	influxQueryLoad = &InfluxQueryLoad{}
	queryWriteCmd   = &cobra.Command{
		Use:   "run",
		Short: "send the queries to db",
		Run: func(cmd *cobra.Command, args []string) {
			RunQuery()
		},
	}
)

func init() {
	influxQueryLoad.Init(queryWriteCmd)
	queryCmd.AddCommand(queryWriteCmd)
}

func RunQuery() {
	influxQueryLoad.Validate()
	exitCode := 0

	var once sync.Once
	var workersGroup sync.WaitGroup
	syncChanDone := make(chan int)
	influxQueryLoad.PrepareWorkers()

	influxQueryLoad.respCollector.SetStart(time.Now())
	for i := 0; i < influxQueryLoad.workers; i++ {
		influxQueryLoad.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := influxQueryLoad.RunProcess(w, &workersGroup)
			if err != nil {
				log.Println(err.Error())
				once.Do(func() {
					if !influxQueryLoad.IsScanFinished() {
						go func() {
							influxQueryLoad.EmptyBatchChanel()
						}()
						syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
		go func(w int) {
			influxQueryLoad.AfterRunProcess(w)
		}(i)
	}
	log.Printf("Started load with %d workers\n", influxQueryLoad.workers)

	// start := time.Now()
	influxQueryLoad.RunScanner(influxQueryLoad.sourceReader, syncChanDone)

	influxQueryLoad.SyncEnd()
	close(syncChanDone)
	workersGroup.Wait()
	influxQueryLoad.respCollector.SetEnd(time.Now())

	influxQueryLoad.CleanUp()

	if influxQueryLoad.dataFile != "" {
		influxQueryLoad.sourceReader.Close()
	}

	influxQueryLoad.GetRespResult()

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func (q *InfluxQueryLoad) Init(cmd *cobra.Command) {
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

func (l *InfluxQueryLoad) Validate() {

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
		log.Printf("Using worker ingestion rate %v queries/s", l.ingestRateLimit)
	} else {
		log.Print("Ingestion rate control is off")
	}

	if l.timeLimit > 0 && l.backoffTimeOut > l.timeLimit {
		l.backoffTimeOut = l.timeLimit
	}

}

func (q *InfluxQueryLoad) PrepareProcess(i int) {
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

func (q *InfluxQueryLoad) PrepareWorkers() {

	q.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	q.batchChan = make(chan batch, q.workers)
	q.inputDone = make(chan struct{})

	q.configs = make([]*loadWorkerConfig, q.workers)
}

func (q *InfluxQueryLoad) EmptyBatchChanel() {
	for range q.batchChan {
		//read out remaining batches
	}
}

func (q *InfluxQueryLoad) SyncEnd() {
	<-q.inputDone
	close(q.batchChan)
}

func (q *InfluxQueryLoad) CleanUp() {
	for _, c := range q.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	q.totalBackOffSecs = float64(0)
	for i := 0; i < q.workers; i++ {
		q.totalBackOffSecs += q.configs[i].backingOffSecs
	}
}

func (q *InfluxQueryLoad) GetScanner() Scanner {
	return q
}

func (q *InfluxQueryLoad) RunProcess(i int, waitGroup *sync.WaitGroup) error {
	return q.processBatches(q.configs[i].writer, q.configs[i].backingOffChan, fmt.Sprintf("%d", i), waitGroup)
}
func (q *InfluxQueryLoad) AfterRunProcess(i int) {
	q.configs[i].backingOffSecs = processBackoffMessages(i, q.configs[i].backingOffChan, q.configs[i].backingOffDone)
}

func (q *InfluxQueryLoad) IsScanFinished() bool {
	return q.scanFinished
}

func (q *InfluxQueryLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = q.itemsRead
	// bytesRead = q.bytesRead
	// valuesRead = q.valuesRead
	return
}

func (q *InfluxQueryLoad) GetRespResult() {
	// fmt.Println(q.respCollector.GetDetail())
	q.respCollector.ShowDetail()
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func (q *InfluxQueryLoad) RunScanner(r io.Reader, syncChanDone chan int) {
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
			buf.Write([]byte(neturl.QueryEscape(sql)))
		} else {
			buf.Write([]byte(neturl.QueryEscape(sql)))
			buf.Write([]byte(neturl.QueryEscape(";")))
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
func (q *InfluxQueryLoad) processBatches(w *HTTPWriter, backoffSrc chan bool, telemetryWorkerLabel string, workersGroup *sync.WaitGroup) error {
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
