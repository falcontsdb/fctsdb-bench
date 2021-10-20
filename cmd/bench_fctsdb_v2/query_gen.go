package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/fctsdb_query_gen"
	"github.com/spf13/cobra"
)

type QueryGenerator struct {
	useCase           string
	scaleVar          int64
	scaleVarOffset    int64
	queryTypeId       int
	timestampStartStr string
	timestampEndStr   string
	seed              int64
	queryCount        int64
	timestampStart    time.Time
	timestampEnd      time.Time
	samplingInterval  time.Duration
}

var (
	queryGenCmd = &cobra.Command{
		Use:   "query-gen",
		Short: "生成数据库查询语句，输出到stdout，搭配query-load使用",
		Run: func(cmd *cobra.Command, args []string) {
			RunGenerateQueries()
		},
		// Hidden: true,
	}

	listQueryCmd = &cobra.Command{
		Use:   "list",
		Short: "展示所有场景（case）和对应的查询语句类型（query-type）",
		Run: func(cmd *cobra.Command, args []string) {
			ListQueryTypes()
		},
	}
	listQueryWithDetail bool

	queryGenerator = QueryGenerator{}
)

func init() {
	rootCmd.AddCommand(queryGenCmd)
	rootCmd.AddCommand(listQueryCmd)
	// queryCmd.AddCommand(showQueryCmd)
	listQueryCmd.Flags().BoolVar(&listQueryWithDetail, "detail", false, "show the detail of query-types")
	// rootCmd.AddCommand(queryCmd)
	queryGenerator.Init(queryGenCmd)
}

func RunGenerateQueries() {
	queryGenerator.Validate()
	queryGenerator.RunProcess()
}

func (q *QueryGenerator) Init(cmd *cobra.Command) {
	queryGenFlag := cmd.Flags()
	queryGenFlag.StringVar(&q.useCase, "use-case", CaseChoices[0], fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(CaseChoices, ", ")))
	queryGenFlag.Int64Var(&q.scaleVar, "scale-var", 1, "Scaling variable specific to the use case.")
	queryGenFlag.Int64Var(&q.scaleVarOffset, "scale-var-offset", 0, "Scaling variable offset specific to the use case.")
	queryGenFlag.IntVar(&q.queryTypeId, "query-type", 1, "Scaling variable offset specific to the use case.")
	queryGenFlag.DurationVar(&q.samplingInterval, "sampling-interval", time.Second, "Simulated sampling interval.")
	queryGenFlag.Int64Var(&q.queryCount, "query-count", 1000, "Number of queries to generate.")
	queryGenFlag.StringVar(&q.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	queryGenFlag.StringVar(&q.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	queryGenFlag.Int64Var(&q.seed, "seed", 12345678, "PRNG seed (default 12345678, or 0, uses the current timestamp).")
}

func (q *QueryGenerator) Validate() {
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
}

func (q *QueryGenerator) RunProcess() {
	out := bufio.NewWriterSize(os.Stdout, 4<<24) // most potimized size based on inspection via test regression
	defer out.Flush()

	var queryType *fctsdb.QueryType
	var ok bool
	var sim common.Simulator
	switch q.useCase {
	case fctsdb.AirQuality.CaseName:
		queryType, ok = fctsdb.AirQuality.Types[q.queryTypeId]
		if !ok {
			log.Fatal("the query-type out of range")
		}
		cfg := &airq.AirqSimulatorConfig{
			Start:            q.timestampStart,
			End:              q.timestampEnd,
			SamplingInterval: q.samplingInterval,
			DeviceCount:      q.scaleVar,
			DeviceOffset:     q.scaleVarOffset,
			SqlTemplates:     []string{queryType.RawSql},
		}
		sim = cfg.ToSimulator()

	case fctsdb.Vehicle.CaseName:
		queryType, ok = fctsdb.Vehicle.Types[q.queryTypeId]
		if !ok {
			log.Fatal("the query-type out of range")
		}
		cfg := &vehicle.VehicleSimulatorConfig{
			Start:            q.timestampStart,
			End:              q.timestampEnd,
			SamplingInterval: q.samplingInterval,
			DeviceCount:      q.scaleVar,
			DeviceOffset:     q.scaleVarOffset,
			SqlTemplates:     []string{queryType.RawSql},
		}
		sim = cfg.ToSimulator()
	}

	for i := 0; i < int(q.queryCount); i++ {
		sim.NextSql(out)
		out.WriteString("\n")
	}
}

func ListQueryTypes() {
	for i := 1; i <= fctsdb.AirQuality.Count; i++ {
		qtype := fctsdb.AirQuality.Types[i]
		caseName := fctsdb.AirQuality.CaseName
		ShowQueryTypes(qtype, i, caseName)
	}
	for i := 1; i <= fctsdb.Vehicle.Count; i++ {
		qtype := fctsdb.Vehicle.Types[i]
		caseName := fctsdb.Vehicle.CaseName
		ShowQueryTypes(qtype, i, caseName)
	}
}

func ShowQueryTypes(qtype *fctsdb.QueryType, ID int, caseName string) {
	if listQueryWithDetail {
		fmt.Println("场景: ", caseName)
		fmt.Println("名称: ", qtype.Name)
		fmt.Println("ID: ", ID)
		fmt.Println("sql示例: ", qtype.RawSql)
		fmt.Println(qtype.Comment)
		fmt.Println("")
		// file, _ := os.OpenFile("case.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		// file.WriteString(fmt.Sprintf("%s,%d,%s,%s\n", caseName, ID, qtype.Name, qtype.RawSql))
	} else {
		fmt.Printf("%s %d %s\n", caseName, ID, qtype.Name)
	}
}
