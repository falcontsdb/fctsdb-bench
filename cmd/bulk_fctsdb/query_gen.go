package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_query_fctsdb"
	"github.com/spf13/cobra"
)

type FctsdbQueryGenerator struct {
	useCase           string
	scaleVar          int64
	scaleVarOffset    int64
	queryType         string
	timestampStartStr string
	timestampEndStr   string
	seed              int64
	queryCount        int64
}

var (
	queryCmd = &cobra.Command{
		Use:   "query",
		Short: "the command for query test",
		Run: func(cmd *cobra.Command, args []string) {
			RunGenerateQueries()
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
	listQueryCmd.Flags().BoolVar(&listQueryWithDetail, "use-case", false, "show the detail of query-types")
	rootCmd.AddCommand(queryCmd)
	fctsdbQueryGenerator.Init(queryGenCmd)
}

func (f *FctsdbQueryGenerator) Init(cmd *cobra.Command) {
	queryGenFlag := cmd.Flags()
	queryGenFlag.StringVar(&f.useCase, "use-case", CaseChoices[0], fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(CaseChoices, ", ")))
	queryGenFlag.Int64Var(&f.scaleVar, "scale-var", 1, "Scaling variable specific to the use case.")
	queryGenFlag.Int64Var(&f.scaleVarOffset, "scale-var-offset", 0, "Scaling variable offset specific to the use case.")
	queryGenFlag.StringVar(&f.queryType, "query-type", "", "Scaling variable offset specific to the use case.")
	queryGenFlag.Int64Var(&f.queryCount, "queries", 1000, "Number of queries to generate.")
	queryGenFlag.StringVar(&f.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	queryGenFlag.StringVar(&f.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	queryGenFlag.Int64Var(&f.seed, "seed", 12345678, "PRNG seed (default 12345678, or 0, uses the current timestamp).")
}

func (f *FctsdbQueryGenerator) Validate() {
	rand.Seed(f.seed)
}

func (f *FctsdbQueryGenerator) Run() {
	out := bufio.NewWriterSize(os.Stdout, 4<<24) // most potimized size based on inspection via test regression
	defer out.Flush()

	switch f.useCase {
	case bulk_query_fctsdb.AirqTypes.CaseName:
		cfg := &airq.AirqSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			AirqDeviceCount:  f.scaleVar,
			AirqDeviceOffset: f.scaleVarOffset,
		}
		sim := cfg.ToSimulator()
		for _, qtypes := range bulk_query_fctsdb.AirqTypes.Types {
			if qtypes.Name == f.queryType {
				qtypes.Generator.Init(sim)
				for i := 0; i < int(f.queryCount); i++ {
					_, err := out.WriteString(qtypes.Generator.Next() + "\n")
					if err != nil {
						log.Println("Write queries error: ", err.Error())
					}
				}
			}
		}
	case bulk_query_fctsdb.VehicleTypes.CaseName:
		cfg := &vehicle.VehicleSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			VehicleCount:  f.scaleVar,
			VehicleOffset: f.scaleVarOffset,
		}
		sim := cfg.ToSimulator()
		for _, qtypes := range bulk_query_fctsdb.VehicleTypes.Types {
			if qtypes.Name == f.queryType {
				qtypes.Generator.Init(sim)
				for i := 0; i < int(f.queryCount); i++ {
					_, err := out.WriteString(qtypes.Generator.Next() + "\n")
					if err != nil {
						log.Println("Write queries error: ", err.Error())
					}
				}
			}
		}
	}
}

func RunGenerateQueries() {
	fctsdbQueryGenerator.Validate()
	fctsdbQueryGenerator.Run()
}

func ListQueryTypes() {
	for i := 1; i <= bulk_query_fctsdb.AirqTypes.Count; i++ {
		qtype := bulk_query_fctsdb.AirqTypes.Types[i]
		caseName := bulk_query_fctsdb.AirqTypes.CaseName
		if listQueryWithDetail {

		} else {
			fmt.Printf("%s %d %s\n", caseName, i, qtype.Name)
		}
	}
	for i := 1; i <= bulk_query_fctsdb.VehicleTypes.Count; i++ {
		qtype := bulk_query_fctsdb.VehicleTypes.Types[i]
		caseName := bulk_query_fctsdb.VehicleTypes.CaseName
		if listQueryWithDetail {

		} else {
			fmt.Printf("%s %d %s\n", caseName, i, qtype.Name)
		}
	}
}

func ShowQueryTypes(name string) {
	switch fctsdbQueryGenerator.useCase {
	case bulk_query_fctsdb.AirqTypes.CaseName:
		for _, qtypes := range bulk_query_fctsdb.AirqTypes.Types {
			if qtypes.Name == name {
				fmt.Println("query name: ", qtypes.Name)
				fmt.Println("sql示例: ", qtypes.RawSql)
				fmt.Println("说明: ", qtypes.Comment)
				break
			}
		}
	case bulk_query_fctsdb.VehicleTypes.CaseName:
		for _, qtypes := range bulk_query_fctsdb.VehicleTypes.Types {
			if qtypes.Name == name {
				fmt.Println("query name: ", qtypes.Name)
				fmt.Println("sql示例: ", qtypes.RawSql)
				fmt.Println("说明: ", qtypes.Comment)
				break
			}
		}
	}
}
