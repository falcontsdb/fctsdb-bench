// data_generator generates time series data from pre-specified use cases.
//
// Supported formats:
// InfluxDB bulk load format
// ElasticSearch bulk load format
// Cassandra query format
// Mongo custom format
// OpenTSDB bulk HTTP format
// TimescaleDB SQL INSERT and binary COPY FROM
// Graphite plaintext format
// Splunk JSON format
//
// Supported use cases:
// Devops: scale_var is the number of hosts to simulate, with log messages
//         every 10 seconds.
package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/devops"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/vehicle"
	"git.querycap.com/falcontsdb/fctsdb-bench/serializers"
	"github.com/spf13/cobra"
)

// Output data format choices:
var formatChoices = []string{"influx-bulk", "es-bulk", "es-bulk6x", "es-bulk7x", "cassandra", "mongo", "opentsdb", "timescaledb-sql", "timescaledb-copyFrom", "graphite-line", "splunk-json"}

type DataGenerator struct {
	format           string
	useCase          string
	scaleVar         int64
	scaleVarOffset   int64
	samplingInterval time.Duration

	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time

	seed  int64
	debug int

	cpuProfile string
}

// Program option vars:
var (
	dataGenCmd = &cobra.Command{
		Use:   "data-gen",
		Short: "生成不同场景（case）的数据，输出到stdout，搭配data-load使用",
		Run: func(cmd *cobra.Command, args []string) {
			DataGen()
		},
		Hidden: !FullFunction,
	}
	dataGenerator = DataGenerator{}
)

func init() {

	dataGenerator.Init(dataGenCmd)
	// dataCmd.AddCommand(dataGenCmd)
	rootCmd.AddCommand(dataGenCmd)
}

func DataGen() {
	dataGenerator.Validate()
	dataGenerator.RunProcess()
}

// Parse args:
func (g *DataGenerator) Init(cmd *cobra.Command) {
	dataGenFlag := cmd.Flags()
	dataGenFlag.StringVar(&g.format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))
	dataGenFlag.StringVar(&g.useCase, "use-case", common.CaseChoices[0], fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(common.CaseChoices, ", ")))
	dataGenFlag.Int64Var(&g.scaleVar, "scale-var", 1, "Scaling variable specific to the use case.")
	dataGenFlag.Int64Var(&g.scaleVarOffset, "scale-var-offset", 0, "Scaling variable offset specific to the use case.")
	dataGenFlag.DurationVar(&g.samplingInterval, "sampling-interval", time.Second, "Simulated sampling interval.")
	// dataGenFlag.StringVar(&g.configFile, "config-file", "", "Simulator config file in TOML format (experimental)")
	dataGenFlag.StringVar(&g.timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	dataGenFlag.StringVar(&g.timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	dataGenFlag.Int64Var(&g.seed, "seed", 12345678, "PRNG seed (default 12345678, or 0, uses the current timestamp).")
	dataGenFlag.IntVar(&g.debug, "debug", 0, "Debug printing (choices: 0, 1, 2) (default 0).")
	dataGenFlag.StringVar(&g.cpuProfile, "cpu-profile", "", "Write CPU profile to `file`")
}

func (g *DataGenerator) Validate() {

	validFormat := false
	for _, s := range formatChoices {
		if s == g.format {
			validFormat = true
			break
		}
	}
	if !validFormat {
		log.Fatalf("invalid format specifier: %v", g.format)
	}

	// the default seed is the current timestamp:
	if g.seed == 0 {
		g.seed = int64(time.Now().Nanosecond())
	}
	log.Printf("using random seed %d\n", g.seed)

	rand.Seed(g.seed)

	// Parse timestamps:
	var err error
	g.timestampStart, err = time.Parse(time.RFC3339, g.timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	g.timestampStart = g.timestampStart.UTC()
	g.timestampEnd, err = time.Parse(time.RFC3339, g.timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	g.timestampEnd = g.timestampEnd.UTC()

	if g.samplingInterval <= 0 {
		log.Fatal("Invalid sampling interval")
	}

	log.Printf("Using sampling interval %v\n", g.samplingInterval)

}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func (g *DataGenerator) RunProcess() {
	defer timeTrack(time.Now(), "data_generator - main()")

	if g.cpuProfile != "" {
		f, err := os.Create(g.cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// if g.configFile != "" {
	// 	c, err := common.NewConfig(g.configFile)
	// 	if err != nil {
	// 		log.Fatalf("external config error: %v", err)
	// 	}
	// 	common.Config = c
	// 	log.Printf("Using config file %s\n", g.configFile)
	// }

	//out := bufio.NewWriterSize(os.Stdout, 4<<20) //original buffer size
	out := bufio.NewWriterSize(os.Stdout, 4<<24) // most potimized size based on inspection via test regression
	defer out.Flush()

	var sim common.Simulator

	switch g.useCase {
	case common.CaseChoices[0]:
		cfg := &vehicle.VehicleSimulatorConfig{
			Start:            g.timestampStart,
			End:              g.timestampEnd,
			SamplingInterval: g.samplingInterval,
			DeviceCount:      g.scaleVar,
			DeviceOffset:     g.scaleVarOffset,
		}
		sim = cfg.ToSimulator()
	case common.CaseChoices[1]:
		cfg := &airq.AirqSimulatorConfig{
			Start:            g.timestampStart,
			End:              g.timestampEnd,
			SamplingInterval: g.samplingInterval,
			DeviceCount:      g.scaleVar,
			DeviceOffset:     g.scaleVarOffset,
		}
		sim = cfg.ToSimulator()
	case common.CaseChoices[2]:
		cfg := &devops.DevopsSimulatorConfig{
			Start: g.timestampStart,
			End:   g.timestampEnd,

			HostCount:  g.scaleVar,
			HostOffset: g.scaleVarOffset,
		}
		devops.EpochDuration = g.samplingInterval
		sim = cfg.ToSimulator()
	default:
		log.Fatalln("the case is not supported")
	}

	var serializer common.Serializer
	switch g.format {
	case "influx-bulk":
		serializer = serializers.NewSerializerInflux()
	case "es-bulk":
		serializer = serializers.NewSerializerElastic("5x")
	case "es-bulk6x":
		serializer = serializers.NewSerializerElastic("6x")
	case "es-bulk7x":
		serializer = serializers.NewSerializerElastic("7x")
	case "cassandra":
		serializer = serializers.NewSerializerCassandra()
	// case "mongo":
	// 	serializer = common.NewSerializerMongo()
	case "opentsdb":
		serializer = serializers.NewSerializerOpenTSDB()
	// case "timescaledb-sql":
	// 	serializer = common.NewSerializerTimescaleSql()
	// case "timescaledb-copyFrom":
	// 	serializer = common.NewSerializerTimescaleBin()
	case "graphite-line":
		serializer = serializers.NewSerializerGraphiteLine()
	case "splunk-json":
		serializer = serializers.NewSerializerSplunkJson()
	default:
		panic("unreachable")
	}

	t := time.Now()
	point := common.MakeUsablePoint()
	n := int64(0)
	for !sim.Finished() {
		sim.Next(point)
		n++
		err := serializer.SerializePoint(out, point)
		if err != nil {
			log.Fatal(err)
		}

		point.Reset()
	}
	if n != sim.SeenPoints() {
		panic(fmt.Sprintf("Logic error, written %d points, generated %d points", n, sim.SeenPoints()))
	}
	serializer.SerializeSize(out, sim.SeenPoints(), sim.SeenValues())
	err := out.Flush()
	dur := time.Since(t)
	log.Printf("Written %d points, %d values, took %0f seconds\n", n, sim.SeenValues(), dur.Seconds())
	if err != nil {
		log.Fatal(err.Error())
	}
}
