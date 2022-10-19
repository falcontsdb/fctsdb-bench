package common

import (
	"fmt"
	"strconv"
	"strings"
)

// SqlTemplate对象用于记录sql文本被分割后的内容信息
type SqlTemplate struct {
	Base      [][]byte //记录sql文本中不用替换的分段文本
	KeyWords  []string //记录sql文本中需要替换的关键字
	KeyRepeat []int    //记录对应关键字需要重复替换的次数
}

// 根据文本进行分割，生成SqlTemplate对象
// 举个例子：
// "select mean(aqi) as aqi from city_air_quality where city in '{city*6}' and time >= '{now}'-30d group by time(1d)"
// 将被分割成base段: "select mean(aqi) as aqi from city_air_quality where city in '"、"' and time >= '"、"'-30d group by time(1d)"三个
// 关键字: city、now
// 重复次数: 6、1
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

	return tmp, nil
}
