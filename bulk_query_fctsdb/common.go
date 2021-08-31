package bulk_query_fctsdb

type QueryType struct {
	Name      string
	RawSql    string
	Comment   string
	Generator QueryGenerator
}

type QueryTypes struct {
	CaseName string
	Types    []*QueryType
}

func (qs *QueryTypes) Regist(q *QueryType) {
	qs.Types = append(qs.Types, q)
}

type QueryGenerator interface {
	Next() string
	Init(interface{})
}
