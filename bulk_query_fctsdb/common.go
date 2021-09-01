package bulk_query_fctsdb

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	},
}

type QueryType struct {
	Name      string
	RawSql    string
	Comment   string
	Generator QueryGenerator
}

type QueryTypes struct {
	CaseName string
	Types    map[int]*QueryType
	Count    int
}

func NewQueryTypes(caseName string) *QueryTypes {
	return &QueryTypes{
		Count:    0,
		CaseName: caseName,
		Types:    make(map[int]*QueryType),
	}
}

func (qs *QueryTypes) Regist(q *QueryType) {
	qs.Count += 1
	qs.Types[qs.Count] = q
}

type QueryGenerator interface {
	Next() string
	Init(interface{})
}
