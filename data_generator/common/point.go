package common

import (
	"time"
)

// Point wraps a single data point. It stores database-agnostic data
// representing one point in time of one measurement.
//
// Internally, Point uses byte slices instead of strings to try to minimize
// overhead.
type Point struct {
	MeasurementName  []byte
	TagKeys          [][]byte
	TagValues        [][]byte
	FieldKeys        [][]byte
	FieldValues      []interface{}
	Int64FiledKeys   [][]byte
	Int64FiledValues []int64
	Timestamp        *time.Time
}

// Using these literals prevents the slices from escaping to the heap, saving
// a few micros per call:
// var ()

func (p *Point) Reset() {
	p.MeasurementName = nil
	p.TagKeys = p.TagKeys[:0]
	p.TagValues = p.TagValues[:0]
	p.FieldKeys = p.FieldKeys[:0]
	p.FieldValues = p.FieldValues[:0]
	p.Int64FiledKeys = p.Int64FiledKeys[:0]
	p.Int64FiledValues = p.Int64FiledValues[:0]
	p.Timestamp = nil
}

func (p *Point) SetTimestamp(t *time.Time) {
	p.Timestamp = t
}

func (p *Point) SetMeasurementName(s []byte) {
	p.MeasurementName = s
}

func (p *Point) AppendTag(key, value []byte) {
	p.TagKeys = append(p.TagKeys, key)
	p.TagValues = append(p.TagValues, value)
}

func (p *Point) AppendField(key []byte, value interface{}) {
	p.FieldKeys = append(p.FieldKeys, key)
	p.FieldValues = append(p.FieldValues, value)
}

// int64 特例化，加快速度
func (p *Point) AppendInt64Field(key []byte, value int64) {
	p.Int64FiledKeys = append(p.Int64FiledKeys, key)
	p.Int64FiledValues = append(p.Int64FiledValues, value)
}

func MakeUsablePoint() *Point {
	return &Point{
		MeasurementName:  nil,
		TagKeys:          make([][]byte, 0),
		TagValues:        make([][]byte, 0),
		FieldKeys:        make([][]byte, 0),
		FieldValues:      make([]interface{}, 0),
		Int64FiledKeys:   make([][]byte, 0),
		Int64FiledValues: make([]int64, 0),
		Timestamp:        &time.Time{},
	}
}
