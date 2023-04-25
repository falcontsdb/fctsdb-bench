package testscene

import (
	"io"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

// Type AirqSimulatorConfig is used to create a AirqSimulator.

type SceneConfig struct {
	Start            time.Time
	End              time.Time
	SamplingInterval time.Duration
	SeriesCount      int64
	SeriesOffset     int64
	SqlTemplates     []string
}

func (c *SceneConfig) ToSimulator() *SceneSimulator {
	series := make([]Series, c.SeriesCount)
	var measNum int64

	for i := 0; i < len(series); i++ {
		series[i] = NewSeries(i, int(c.SeriesOffset), c.Start)
		measNum += int64(series[i].NumMeasurements())
	}

	epochs := c.End.Sub(c.Start).Nanoseconds() / c.SamplingInterval.Nanoseconds()
	maxPoints := epochs * measNum

	dg := &SceneSimulator{
		madePoints:       0, //保证madePoint在next方法中被使用时的初始值是0
		madeValues:       0,
		madeSql:          0,
		maxPoints:        maxPoints,
		Hosts:            series,
		writtenPoints:    0,
		SamplingInterval: c.SamplingInterval,
		TimestampStart:   c.Start,
		TimestampEnd:     c.End,
	}

	err := dg.SetSqlTemplate(c.SqlTemplates)
	if err != nil {
		log.Fatalln(err.Error())
	}
	return dg
}

// It fulfills the Simulator interface.
type SceneSimulator struct {
	madePoints    int64
	maxPoints     int64
	madeValues    int64
	madeSql       int64
	writtenPoints int64

	Hosts            []Series
	SamplingInterval time.Duration
	TimestampStart   time.Time
	TimestampEnd     time.Time
	sqlTemplates     []*common.SqlTemplate
}

func (s *SceneSimulator) SeenPoints() int64 {
	return s.madePoints
}

func (s *SceneSimulator) SeenValues() int64 {
	return s.madeValues
}

func (s *SceneSimulator) Total() int64 {
	return s.maxPoints
}

func (s *SceneSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *SceneSimulator) SetWrittenPoints(num int64) {
	if num > s.writtenPoints {
		atomic.StoreInt64(&s.writtenPoints, num)
	}
}

func (s *SceneSimulator) SetSqlTemplate(sqlTemplates []string) error {
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

func (s *SceneSimulator) ClearMadePointNum() {
	atomic.StoreInt64(&s.madePoints, 0)
}

// Next advances a Point to the next state in the generator.
func (s *SceneSimulator) Next(p *common.Point) int64 {

	madePoint := atomic.AddInt64(&s.madePoints, 1)
	pointIndex := madePoint - 1
	hostIndex := pointIndex % int64(len(s.Hosts))

	ss := &s.Hosts[hostIndex]
	// vehicle.SimulatedMeasurements[0].Tick(v.SamplingInterval)
	// 为了多协程下不混乱, 且由于这里只有一张表，这里不使用Tick方法
	timestamp := s.TimestampStart.Add(s.SamplingInterval * time.Duration(pointIndex/int64(len(s.Hosts))))
	p.SetTimestamp(&timestamp)

	// Populate host-specific tags: for example, LSVNV2182E2100001
	for i := range TagKeys {
		p.AppendTag(TagKeys[i], ss.TagValues[i])
	}

	// Populate measurement-specific tags and fields:
	ss.SimulatedMeasurements[0].ToPoint(p)
	atomic.AddInt64(&s.madeValues, int64(len(p.FieldValues)))
	return madePoint //方便另一种线程安全的结束方式，for sim.next(point) <= sim.total() {...} 保证产生的总点数正确，注意最后一次{...}里面的代码不执行
}

func (s *SceneSimulator) NextSql(wr io.Writer) int64 {
	madeSql := atomic.AddInt64(&s.madeSql, 1)
	tmp := s.sqlTemplates[madeSql%int64(len(s.sqlTemplates))]

	// 生成数据点时，用fastrand更快速，生成的数据和seed无关联
	// 生成sql时，为了保证每次生成sql一致性，采用rand库，使用全局seed
	randomHostsIndex := rand.Intn(len(s.Hosts))
	for i := range tmp.Base {
		wr.Write(tmp.Base[i])
		if i < len(tmp.KeyWords) {
			repeat := tmp.KeyRepeat[i]
			for k := 0; k < repeat; k++ {
				scene := s.Hosts[(randomHostsIndex+k)%len(s.Hosts)]
				key := tmp.KeyWords[i]
				switch key {
				case string(TagKeys[0]):
					wr.Write(scene.TagValues[0])
				case string(TagKeys[1]):
					wr.Write(scene.TagValues[1])
				case string(TagKeys[2]):
					wr.Write(scene.TagValues[2])
				case string(TagKeys[3]):
					wr.Write(scene.TagValues[3])
				case "start":
					wr.Write([]byte(s.TimestampStart.Format(time.RFC3339)))
				case "end":
					wr.Write([]byte(s.TimestampEnd.Format(time.RFC3339)))
				case "now":
					currentTimeInDB := s.TimestampStart.Add(s.SamplingInterval * time.Duration(s.writtenPoints/int64(len(s.Hosts))))
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
