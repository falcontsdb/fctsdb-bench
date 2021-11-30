package serializers

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// scratchBufPool helps reuse serialization scratch buffers.
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

const DatasetSizeMarker = "dataset-size:"

var DatasetSizeMarkerRE = regexp.MustCompile(DatasetSizeMarker + `(\d+),(\d+)`)

func serializeSizeInText(w io.Writer, points int64, values int64) error {
	buf := scratchBufPool.Get().([]byte)
	buf = append(buf, fmt.Sprintf("%s%d,%d\n", DatasetSizeMarker, points, values)...)
	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func fastFormatAppend(v interface{}, buf []byte, singleQuotesForString bool) []byte {
	var quotationChar = "\""
	if singleQuotesForString {
		quotationChar = "'"
	}
	switch v := v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v), 10)
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case float64:
		return strconv.AppendFloat(buf, v, 'f', 16, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v), 'f', 16, 32)
	case bool:
		return strconv.AppendBool(buf, v)
	case []byte:
		buf = append(buf, quotationChar...)
		buf = append(buf, v...)
		buf = append(buf, quotationChar...)
		return buf
	case string:
		buf = append(buf, quotationChar...)
		buf = append(buf, v...)
		buf = append(buf, quotationChar...)
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %v", v))
	}
}

func CheckTotalValues(line string) (totalPoints, totalValues int64, err error) {
	if strings.HasPrefix(line, DatasetSizeMarker) {
		parts := DatasetSizeMarkerRE.FindAllStringSubmatch(line, -1)
		if parts == nil || len(parts[0]) != 3 {
			err = fmt.Errorf("incorrent number of matched groups: %#v", parts)
			return
		}
		if i, e := strconv.Atoi(parts[0][1]); e == nil {
			totalPoints = int64(i)
		} else {
			err = e
			return
		}
		if i, e := strconv.Atoi(parts[0][2]); e == nil {
			totalValues = int64(i)
		} else {
			err = e
		}
	}
	return
}
