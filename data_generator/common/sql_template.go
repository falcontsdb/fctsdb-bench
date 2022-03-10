package common

import (
	"fmt"
	"strconv"
	"strings"
)

type SqlTemplate struct {
	Base      [][]byte
	KeyWords  []string
	KeyRepeat []int
}

func NewSqlTemplate(tql string) (*SqlTemplate, error) {
	tqlBytes := []byte(tql)
	tmp := &SqlTemplate{}
	key := make([]byte, 0)
	base := make([]byte, 0)
	record := false
	for _, b := range tqlBytes {
		switch b {
		case '{':
			if !record {
				record = true
				tmp.Base = append(tmp.Base, base)
				base = make([]byte, 0)
			} else {
				return nil, fmt.Errorf("can not parse the sql template, repeat {")
			}
		case '}':
			if record {
				record = false
				keyMsg := strings.ToLower(string(key))
				keyIE := strings.Split(keyMsg, "*")
				if len(keyIE) >= 2 {
					repeat, err := strconv.Atoi(strings.TrimSpace(keyIE[1]))
					if err != nil {
						return nil, fmt.Errorf("can not parse the sql template, %s is incorrect", keyMsg)
					}
					tmp.KeyRepeat = append(tmp.KeyRepeat, repeat)
				} else {
					tmp.KeyRepeat = append(tmp.KeyRepeat, 1)
				}
				tmp.KeyWords = append(tmp.KeyWords, strings.TrimSpace(keyIE[0]))
				key = make([]byte, 0)
			} else {
				return nil, fmt.Errorf("can not parse the sql template, repeat }")
			}
		default:
			if record {
				key = append(key, b)
			} else {
				base = append(base, b)
			}
		}
	}
	if len(base) > 0 {
		tmp.Base = append(tmp.Base, base)
	}

	// tmp.TimeStart = append(tmp.TimeStart, '\'')
	// tmp.TimeStart = append(tmp.TimeStart, []byte(start.UTC().String())...)
	// tmp.TimeStart = append(tmp.TimeStart, '\'')

	// tmp.TimeEnd = append(tmp.TimeEnd, '\'')
	// tmp.TimeEnd = append(tmp.TimeEnd, []byte(end.UTC().String())...)
	// tmp.TimeEnd = append(tmp.TimeEnd, '\'')

	return tmp, nil
}
