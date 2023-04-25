package vehicle

import (
	"io"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

// Type IotSimulatorConfig is used to create a IotSimulator.
type VehicleSimulatorConfig struct {
	Start            time.Time
	End              time.Time
	SamplingInterval time.Duration
	DeviceCount      int64
	DeviceOffset     int64
	SqlTemplates     []string
}

func (d *VehicleSimulatorConfig) ToSimulator() *VehicleSimulator {
	if d.DeviceCount <= 0 {
		log.Fatal("the vehicle count is unavailable")
	}
	vehicleInfos := make([]Vehicle, d.DeviceCount)
	var measNum int64

	for i := 0; i < len(vehicleInfos); i++ {
		vehicleInfos[i] = NewVehicle(i, int(d.DeviceOffset), d.Start)
		measNum += int64(vehicleInfos[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / d.SamplingInterval.Nanoseconds()
	maxPoints := epochs * measNum

	dg := &VehicleSimulator{
		madePoints:       0,
		madeValues:       0,
		maxPoints:        maxPoints,
		madeSql:          0,
		writtenPoints:    0,
		Hosts:            vehicleInfos,
		SamplingInterval: d.SamplingInterval,
		TimestampStart:   d.Start,
		TimestampEnd:     d.End,
	}

	err := dg.SetSqlTemplate(d.SqlTemplates)
	if err != nil {
		log.Fatalln(err.Error())
	}
	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type VehicleSimulator struct {
	madePoints       int64
	maxPoints        int64
	madeValues       int64
	madeSql          int64
	writtenPoints    int64
	Hosts            []Vehicle
	SamplingInterval time.Duration
	TimestampStart   time.Time
	TimestampEnd     time.Time
	sqlTemplates     []*common.SqlTemplate
}

func (g *VehicleSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *VehicleSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *VehicleSimulator) Total() int64 {
	return g.maxPoints
}

func (g *VehicleSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

func (g *VehicleSimulator) SetWrittenPoints(num int64) {
	if num > g.writtenPoints {
		atomic.StoreInt64(&g.writtenPoints, num)
	}
}

func (g *VehicleSimulator) SetSqlTemplate(sqlTemplates []string) error {
	templates := make([]*common.SqlTemplate, len(sqlTemplates))
	for i := range sqlTemplates {
		temp, err := common.NewSqlTemplate(sqlTemplates[i])
		if err != nil {
			return err
		}
		templates[i] = temp
	}
	g.sqlTemplates = templates
	return nil
}

func (q *VehicleSimulator) ClearMadePointNum() {
	atomic.StoreInt64(&q.madePoints, 0)
}

// Next advances a Point to the next state in the generator.
func (g *VehicleSimulator) Next(p *common.Point) int64 {
	// switch to the next metric if needed
	madePoint := atomic.AddInt64(&g.madePoints, 1)
	pointIndex := madePoint - 1 //保证在next方法中被使用时的初始值是0
	hostIndex := pointIndex % int64(len(g.Hosts))

	vehicle := &g.Hosts[hostIndex]
	// vehicle.SimulatedMeasurements[0].Tick(v.SamplingInterval)
	// 为了多协程不混乱，这里不使用Tick方法
	timestamp := g.TimestampStart.Add(g.SamplingInterval * time.Duration(pointIndex/int64(len(g.Hosts))))
	p.SetTimestamp(&timestamp)

	// Populate host-specific tags: for example, LSVNV2182E2100001
	p.AppendTag([]byte("VIN"), vehicle.Name)

	// Populate measurement-specific tags and fields:
	vehicle.SimulatedMeasurements[0].ToPoint(p)

	atomic.AddInt64(&g.madeValues, int64(len(p.FieldValues)+len(p.Int64FiledValues)))
	return madePoint //方便另一种线程安全的结束方式，for sim.next(point) <= sim.total() {...} 保证产生的总点数正确，注意最后一次{...}里面的代码不执行
}

func (g *VehicleSimulator) NextSql(wr io.Writer) int64 {
	madeSql := atomic.AddInt64(&g.madeSql, 1)
	tmp := g.sqlTemplates[madeSql%int64(len(g.sqlTemplates))]

	// 生成数据点时，用fastrand更快速，生成的数据和seed无关联
	// 生成sql时，为了保证每次生成sql一致性，采用rand库，使用全局seed
	randomHostsIndex := rand.Intn(len(g.Hosts))
	for i := range tmp.Base {
		wr.Write(tmp.Base[i])
		if i < len(tmp.KeyWords) {
			repeat := tmp.KeyRepeat[i]
			for k := 0; k < repeat; k++ {
				vehicle := g.Hosts[(randomHostsIndex+k)%len(g.Hosts)]
				key := tmp.KeyWords[i]
				switch key {
				case "vin":
					wr.Write(vehicle.Name)
				case "start":
					wr.Write([]byte(g.TimestampStart.Format(time.RFC3339)))
				case "end":
					wr.Write([]byte(g.TimestampEnd.Format(time.RFC3339)))
				case "now":
					currentTimeInDB := g.TimestampStart.Add(g.SamplingInterval * time.Duration(g.writtenPoints/int64(len(g.Hosts))))
					wr.Write([]byte(currentTimeInDB.Format(time.RFC3339)))
				}
				if k < repeat-1 {
					wr.Write([]byte("','"))
				}
			}
		}
	}
	return madeSql
}
