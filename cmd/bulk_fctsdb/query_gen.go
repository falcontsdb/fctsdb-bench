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
	fctsdb "git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_fctsdb"
	"github.com/spf13/cobra"
)

type FctsdbQueryGenerator struct {
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
	queryCmd = &cobra.Command{
		Use:   "query",
		Short: "the command for query test",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(cmd.Help())
		},
	}

	queryGenCmd = &cobra.Command{
		Use:   "gen",
		Short: "gen the query",
		Run: func(cmd *cobra.Command, args []string) {
			RunGenerateQueries()
		},
	}

	listQueryCmd = &cobra.Command{
		Use:   "list",
		Short: "list query types",
		Run: func(cmd *cobra.Command, args []string) {
			ListQueryTypes()
		},
	}
	listQueryWithDetail bool

	fctsdbQueryGenerator = FctsdbQueryGenerator{}
)

func init() {
	queryCmd.AddCommand(queryGenCmd)
	queryCmd.AddCommand(listQueryCmd)
	// queryCmd.AddCommand(showQueryCmd)
	listQueryCmd.Flags().BoolVar(&listQueryWithDetail, "detail", false, "show the detail of query-types")
	rootCmd.AddCommand(queryCmd)
	fctsdbQueryGenerator.Init(queryGenCmd)
}

func RunGenerateQueries() {
	fctsdbQueryGenerator.Validate()
	fctsdbQueryGenerator.RunProcess()
}

func (f *FctsdbQueryGenerator) Init(cmd *cobra.Command) {
	queryGenFlag := cmd.Flags()
	queryGenFlag.StringVar(&f.useCase, "use-case", CaseChoices[0], fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(CaseChoices, ", ")))
	queryGenFlag.Int64Var(&f.scaleVar, "scale-var", 1, "Scaling variable specific to the use case.")
	queryGenFlag.Int64Var(&f.scaleVarOffset, "scale-var-offset", 0, "Scaling variable offset specific to the use case.")
	queryGenFlag.IntVar(&f.queryTypeId, "query-type", 1, "Scaling variable offset specific to the use case.")
	queryGenFlag.DurationVar(&f.samplingInterval, "sampling-interval", time.Second, "Simulated sampling interval.")
	queryGenFlag.Int64Var(&f.queryCount, "queries", 1000, "Number of queries to generate.")
	queryGenFlag.StringVar(&f.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	queryGenFlag.StringVar(&f.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	queryGenFlag.Int64Var(&f.seed, "seed", 12345678, "PRNG seed (default 12345678, or 0, uses the current timestamp).")
}

func (f *FctsdbQueryGenerator) Validate() {
	if f.seed == 0 {
		f.seed = int64(time.Now().Nanosecond())
	}
	log.Printf("using random seed %d\n", f.seed)
	rand.Seed(f.seed)

	// Parse timestamps:
	var err error
	f.timestampStart, err = time.Parse(time.RFC3339, f.timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	f.timestampStart = f.timestampStart.UTC()
	f.timestampEnd, err = time.Parse(time.RFC3339, f.timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	f.timestampEnd = f.timestampEnd.UTC()

	if f.samplingInterval <= 0 {
		log.Fatal("Invalid sampling interval")
	}

	log.Printf("Using sampling interval %v\n", f.samplingInterval)
}

func (f *FctsdbQueryGenerator) RunProcess() {
	out := bufio.NewWriterSize(os.Stdout, 4<<24) // most potimized size based on inspection via test regression
	defer out.Flush()

	var queryType *fctsdb.QueryType
	var ok bool
	var sim common.Simulator
	switch f.useCase {
	case fctsdb.AirQuality.CaseName:
		cfg := &airq.AirqSimulatorConfig{
			Start:            f.timestampStart,
			End:              f.timestampEnd,
			SamplingInterval: f.samplingInterval,
			AirqDeviceCount:  f.scaleVar,
			AirqDeviceOffset: f.scaleVarOffset,
		}
		sim = cfg.ToSimulator()
		queryType, ok = fctsdb.AirQuality.Types[f.queryTypeId]
		if !ok {
			log.Fatal("the query-type out of range")
		}

	case fctsdb.Vehicle.CaseName:
		cfg := &vehicle.VehicleSimulatorConfig{
			Start:            f.timestampStart,
			End:              f.timestampEnd,
			SamplingInterval: f.samplingInterval,
			VehicleCount:     f.scaleVar,
			VehicleOffset:    f.scaleVarOffset,
		}
		sim = cfg.ToSimulator()

		queryType, ok = fctsdb.Vehicle.Types[f.queryTypeId]
		if !ok {
			log.Fatal("the query-type out of range")
		}
	}

	queryType.Generator.Init(sim)
	for i := 0; i < int(f.queryCount); i++ {
		_, err := out.WriteString(queryType.Generator.Next() + "\n")
		if err != nil {
			log.Println("Write queries error: ", err.Error())
		}
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
		fmt.Println("名称: ", qtype.Name)
		fmt.Println("ID: ", ID)
		fmt.Println("sql示例: ", qtype.RawSql)
		fmt.Println(qtype.Comment)
		fmt.Println("")
	} else {
		fmt.Printf("%s %d %s\n", caseName, ID, qtype.Name)
	}
}
