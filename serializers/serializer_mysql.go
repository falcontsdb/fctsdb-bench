package serializers

import (
	"fmt"
	"io"
	"strconv"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

type SerializerMysql struct {
}

func NewSerializerMysql() *SerializerMysql {
	return &SerializerMysql{}
}

// insert into table values ( "xxx","xxx")
func (s *SerializerMysql) SerializePoint(w io.Writer, p *common.Point) (err error) {
	buf := scratchBufPool.Get().([]byte)
	// buf := make([]byte, 0, 4*1024)
	//buf = append(buf, "insert into "...)
	//buf = append(buf, p.MeasurementName...)
	buf = append(buf, "("...)

	// add the timestamp
	buf = append(buf, '"')
	buf = append(buf, p.Timestamp.Format("2006-01-02 15:04:05.000")...)
	buf = append(buf, '"')

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, '"')
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, '"')
	}
	buf = append(buf, ',')

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)
		if i+1 < len(p.FieldKeys) || len(p.Int64FiledKeys) != 0 {
			buf = append(buf, ',')
		}
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		v := p.Int64FiledValues[i]
		buf = strconv.AppendInt(buf, v, 10)
		if i+1 < len(p.Int64FiledKeys) {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ")"...)
	_, err = w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)
	return err
}

func (s *SerializerMysql) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}

func (s *SerializerMysql) CreateTableFromPoint(w io.Writer, p *common.Point) error {
	buf := scratchBufPool.Get().([]byte)
	// buf := make([]byte, 0, 4*1024)
	buf = append(buf, "create table IF NOT EXISTS "...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, " ("...)

	// add the timestamp
	buf = append(buf, "time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP"...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, " char(64) NOT NULL DEFAULT ''"...)
	}
	buf = append(buf, ',')

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		k := p.FieldKeys[i]
		v := p.FieldValues[i]
		buf = append(buf, k...)
		switch v.(type) {
		case int, int64:
			buf = append(buf, " bigint"...)
		case float64, float32:
			buf = append(buf, " double"...)
		case []byte:
			buf = append(buf, " char(64)"...)
		case string:
			buf = append(buf, " char(64)"...)
		case bool:
			//mysql不支持bool，一般使用tinyint(1)来存储，这里使用char是因为这样就不需要修改fastFormatAppend函数
			buf = append(buf, " char(64)"...)
		default:
			panic(fmt.Sprintf("unknown field type for %#v", v))
		}
		buf = append(buf, ',')
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		buf = append(buf, p.Int64FiledKeys[i]...)
		buf = append(buf, " bigint"...)
		buf = append(buf, ',')
	}
	buf = append(buf, "PRIMARY KEY pk_name_gender_ctime(time,"...)
	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, p.TagKeys[i]...)
		if i+1 < len(p.TagKeys) {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, ")"...)
	buf = append(buf, ");"...)

	_, err := w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)
	return err

}
