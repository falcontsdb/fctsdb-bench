package metaqueries

import (
	"fmt"
	"io"
	"time"

	"math/rand"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
)

var (
	TagKeys = [][]byte{
		[]byte("X"),
		[]byte("Y"),
	}
	valKey  = []byte("val")
	measKey = []byte("example_measurement")
)

const TagSig = "Tag-%d"

type MetaquerySimulatorConfig struct {
	Start time.Time
	End   time.Time

	ScaleFactor int
}

func (d *MetaquerySimulatorConfig) ToSimulator() *MetaquerySimulator {
	dg := &MetaquerySimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  int64(d.ScaleFactor) * int64(d.ScaleFactor),

		axis:    d.ScaleFactor,
		TagList: make(map[int][]byte),

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	// Tag values are generated and stored in memory for later use. The number of
	// tag values is equal to the scaleFactor. The same list of tag values is used
	// for both the "X" and "Y" tag keys.
	for i := 0; i < d.ScaleFactor; i++ {
		dg.TagList[i] = []byte(fmt.Sprintf(TagSig, i))
	}

	// The stepTime is the interval between generated points. This will result in
	// the generated data being spread evenly across the provided time range.
	dg.stepTime = time.Duration(int64(dg.timestampEnd.Sub(dg.timestampStart)) / dg.maxPoints)

	return dg
}

// MetaquerySimulator fullfills the Simulator interface.
type MetaquerySimulator struct {
	madePoints int64
	maxPoints  int64
	madeValues int64

	axis    int
	TagList map[int][]byte

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	stepTime       time.Duration
}

func (g *MetaquerySimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *MetaquerySimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *MetaquerySimulator) Total() int64 {
	return g.maxPoints
}

func (g *MetaquerySimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (g *MetaquerySimulator) Next(p *common.Point) int64 {
	p.SetMeasurementName(measKey)
	p.SetTimestamp(&g.timestampNow)

	// Tag values are populated based on a pseudo-random selection from the list
	// of created tags. On a sufficiently large dataset, this will result in each
	// tag appearing roughly the same amount of times throughout the generated
	// data, evenly distributed over the time range.
	xIdx := rand.Intn(g.axis)
	yIdx := rand.Intn(g.axis)
	p.AppendTag(TagKeys[0], g.TagList[xIdx])
	p.AppendTag(TagKeys[1], g.TagList[yIdx])

	p.AppendField(valKey, rand.Float64())

	g.madePoints++
	g.madeValues++
	g.timestampNow = g.timestampNow.Add(g.stepTime)
	return g.madePoints
}

func (g *MetaquerySimulator) NextSql(wr io.Writer) int64 {
	return 0
}

func (g *MetaquerySimulator) SetWrittenPoints(num int64) {
}

func (g *MetaquerySimulator) SetSqlTemplate(sqlTemplates []string) error {
	return nil
}
