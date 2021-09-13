package common

import (
	"encoding/gob"
	"sync"
	"time"
)

// Point wraps a single data point. It stores database-agnostic data
// representing one point in time of one measurement.
//
// Internally, Point uses byte slices instead of strings to try to minimize
// overhead.
type Point struct {
	MeasurementName []byte
	TagKeys         [][]byte
	TagValues       [][]byte
	FieldKeys       [][]byte
	FieldValues     []interface{}
	Timestamp       *time.Time

	encoder *gob.Encoder
}

// Using these literals prevents the slices from escaping to the heap, saving
// a few micros per call:
var ()

// scratchBufPool helps reuse serialization scratch buffers.
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

func (p *Point) Reset() {
	p.MeasurementName = nil
	p.TagKeys = p.TagKeys[:0]
	p.TagValues = p.TagValues[:0]
	p.FieldKeys = p.FieldKeys[:0]
	p.FieldValues = p.FieldValues[:0]
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

func (p *Point) AddFeild(key [][]byte, value []interface{}) {
	p.FieldKeys = key
	p.FieldValues = value
}
