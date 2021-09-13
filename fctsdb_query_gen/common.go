package bulk_query_fctsdb

import (
	"sync"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/common"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 32*1024)
	},
}

type QueryType struct {
	Name      string
	RawSql    string
	Comment   string
	Generator QueryGenerator
}

type QueryCase struct {
	CaseName string
	Types    map[int]*QueryType
	Count    int
}

func NewQueryCase(caseName string) *QueryCase {
	return &QueryCase{
		Count:    0,
		CaseName: caseName,
		Types:    make(map[int]*QueryType),
	}
}

func (qs *QueryCase) Regist(q *QueryType) {
	qs.Count += 1
	qs.Types[qs.Count] = q
}

type QueryGenerator interface {
	Next() string
	Init(common.Simulator) error
}
