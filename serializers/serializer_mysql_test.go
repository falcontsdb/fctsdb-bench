package serializers

import (
	"fmt"
	"testing"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

var (
	tagKeys = [][]byte{
		[]byte("t1"),
		[]byte("t2"),
		[]byte("t3"),
	}

	tagValues = [][]byte{
		[]byte("tv1"),
		[]byte("tv2"),
		[]byte("tv3"),
	}
	fieldKeys = [][]byte{
		[]byte("f_int"),
		[]byte("f_float"),
		[]byte("f_string"),
		[]byte("f_bool"),
	}
	fieldValues = []interface{}{
		1,
		1.1,
		"xxx",
		true,
	}
	intFieldlKeys = [][]byte{
		[]byte("i1"),
		[]byte("i2"),
	}
	intFieldValues = []int64{
		1,
		2,
	}

	tt    = time.Now()
	point = common.Point{
		MeasurementName:  []byte("m"),
		TagKeys:          tagKeys,
		TagValues:        tagValues,
		FieldKeys:        fieldKeys,
		FieldValues:      fieldValues,
		Int64FiledKeys:   intFieldlKeys,
		Int64FiledValues: intFieldValues,
		Timestamp:        &tt,
	}
)

type selfWrite struct {
	data []byte
}

func (w *selfWrite) Write(data []byte) (n int, err error) {
	w.data = data
	return len(data), nil
}
func TestSerializerMysql_SerializePoint(t *testing.T) {
	sm := NewSerializerMysql()
	w := selfWrite{
		data: []byte{},
	}
	fmt.Println(sm.SerializePoint(&w, &point))
	fmt.Println(string(w.data))
}
func TestSerializerMysql_CreateDatabaseFromPoint(t *testing.T) {
	sm := NewSerializerMysql()
	w := selfWrite{
		data: []byte{},
	}
	fmt.Println(sm.CreateTableFromPoint(&w, &point))
	fmt.Println(string(w.data))
}
