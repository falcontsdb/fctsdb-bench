package universal

import (
	"io"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

// Type AirqSimulatorConfig is used to create a AirqSimulator.

type UniversalSimulatorConfig struct {
	Start            time.Time
	End              time.Time
	SamplingInterval time.Duration
	DeviceCount      int64
	DeviceOffset     int64
	MeasurementCount int64
	TagsDefine       []int64
	FieldsDefine     [3]int64
}

type UniversalCase struct {
	TagsDefine   []int64
	FieldsDefine [3]int64
}

func (d *UniversalSimulatorConfig) ToSimulator() *UniversalSimulator {
	devices := make([]Device, d.DeviceCount)
	var measNum int64

	for i := 0; i < len(devices); i++ {
		devices[i] = NewDevice(d.DeviceOffset+int64(i), d.TagsDefine, d.FieldsDefine)
		measNum += int64(devices[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / d.SamplingInterval.Nanoseconds()
	maxPoints := epochs * measNum

	dg := &UniversalSimulator{
		madePoints:       0, //保证madePoint在next方法中被使用时的初始值是0
		madeValues:       0,
		madeSql:          0,
		maxPoints:        maxPoints,
		Hosts:            devices,
		writtenPoints:    0,
		SamplingInterval: d.SamplingInterval,
		TimestampStart:   d.Start,
		TimestampEnd:     d.End,
	}
	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type UniversalSimulator struct {
	madePoints    int64
	maxPoints     int64
	madeValues    int64
	madeSql       int64
	writtenPoints int64

	Hosts            []Device
	SamplingInterval time.Duration
	TimestampStart   time.Time
	TimestampEnd     time.Time
	sqlTemplates     []*common.SqlTemplate
}

func (s *UniversalSimulator) SeenPoints() int64 {
	return s.madePoints
}

func (s *UniversalSimulator) SeenValues() int64 {
	return s.madeValues
}

func (s *UniversalSimulator) Total() int64 {
	return s.maxPoints
}

func (s *UniversalSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *UniversalSimulator) SetWrittenPoints(num int64) {
	if num > s.writtenPoints {
		atomic.StoreInt64(&s.writtenPoints, num)
	}
}

func (s *UniversalSimulator) SetSqlTemplate(sqlTemplates []string) error {
	templates := make([]*common.SqlTemplate, len(sqlTemplates))
	for i := range sqlTemplates {
		temp, err := common.NewSqlTemplate(sqlTemplates[i])
		if err != nil {
			return err
		}
		templates[i] = temp
	}
	s.sqlTemplates = templates
	return nil
}

// Next advances a Point to the next state in the generator.
func (s *UniversalSimulator) Next(p *common.Point) int64 {

	madePoint := atomic.AddInt64(&s.madePoints, 1)
	pointIndex := madePoint - 1
	hostIndex := pointIndex % int64(len(s.Hosts))

	host := &s.Hosts[hostIndex]
	// vehicle.SimulatedMeasurements[0].Tick(v.SamplingInterval)
	// 为了多协程下不混乱, 且由于这里只有一张表，这里不使用Tick方法
	timestamp := s.TimestampStart.Add(s.SamplingInterval * time.Duration(pointIndex/int64(len(s.Hosts))))
	p.SetTimestamp(&timestamp)

	for i := range host.TagKeys {
		p.AppendTag(host.TagKeys[i], host.TagValues[i])
	}

	// Populate measurement-specific tags and fields:
	host.SimulatedMeasurements[0].ToPoint(p)
	atomic.AddInt64(&s.madeValues, int64(len(p.FieldValues)))
	return madePoint //方便另一种线程安全的结束方式，for sim.next(point) <= sim.total() {...} 保证产生的总点数正确，注意最后一次{...}里面的代码不执行
}

func (s *UniversalSimulator) NextSql(wr io.Writer) int64 {
	return 0
}
