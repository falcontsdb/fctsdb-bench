package picture

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

type Line struct {
	name        string
	xAxis       []string
	seriesKey   []string
	seriesValue [][]string
	small       bool
}

func NewLine(name string) *Line {
	return &Line{
		name:        name,
		xAxis:       make([]string, 0),
		seriesKey:   make([]string, 0), //为了有序输出，key和value单独存储
		seriesValue: make([][]string, 0),
		small:       false,
	}
}

func (l *Line) AddSeries(name string, series []string) {
	l.seriesKey = append(l.seriesKey, name)
	l.seriesValue = append(l.seriesValue, series)
}

func (l *Line) SetXAxis(xAxis []string) {
	l.xAxis = xAxis
}

func (l *Line) SetSmallSize(useSmallSize bool) {
	l.small = useSmallSize
}

func (l *Line) ToHtml() string {
	tmp := `<div class="item" id="{{ ChartID }}" style="{{ Style }}"></div>
	<script type="text/javascript">
		"use strict";
		let echarts_{{ ChartID }} = echarts.init(document.getElementById('{{ ChartID }}'), "white");
		let option_{{ ChartID }} = {{ JSON }};
		echarts_{{ ChartID }}.setOption(option_{{ ChartID }});
	</script>
	`

	htm := strings.ReplaceAll(tmp, "{{ ChartID }}", generateUniqueID())
	if l.small {
		htm = strings.ReplaceAll(htm, "{{ Style }}", "width:495px;height:300px;display:inline-block;")
	} else {
		htm = strings.ReplaceAll(htm, "{{ Style }}", "width:800px;height:320px;display:inline-block;")
	}

	options := make(map[string]interface{})

	options["xAxis"] = map[string]interface{}{
		// "boundaryGap": false,
		"data": l.xAxis,
	}

	series := make([]map[string]interface{}, 0)
	for i, key := range l.seriesKey {
		ser := make(map[string]interface{})
		ser["name"] = key
		ser["type"] = "line"
		ser["waveAnimation"] = false
		// ser["renderLabelForZeroData"] = false
		// ser["selectedMode"] = false
		ser["animation"] = false
		ser["label"] = map[string]interface{}{"show": true}
		ser["data"] = l.seriesValue[i]
		series = append(series, ser)
	}
	options["series"] = series

	options["yAxis"] = map[string]interface{}{
		"type": "value",
		"axisLabel": map[string]string{
			"formatter": "{{ yAxisLabelFormatter }}", // function无法通过json.Marshal得到正确的结果，用字符串{{ yAxisLabelFormatter }}占位，后期替换
		},
	}

	options["title"] = map[string]interface{}{
		"text":      l.name,
		"textStyle": map[string]interface{}{"fontSize": 14},
		"top":       "0px",
	}

	options["tooltip"] = map[string]interface{}{
		"trigger": "axis",
	}
	options["legend"] = map[string]interface{}{
		"top": "20px",
	}
	options["toolbox"] = map[string]interface{}{
		"right": "20px",
		"feature": map[string]interface{}{
			"saveAsImage": map[string]interface{}{},
		},
	}

	options["grid"] = map[string]interface{}{
		"left":         "2%",
		"right":        "5%",
		"bottom":       "2%",
		"containLabel": true,
	}

	// options["color"] = []string{
	// 	"#ee6666",
	// 	"#73c0de",
	// 	"#fac858",
	// 	"#ea7ccc",
	// 	"#91cc75",
	// 	"#3ba272",
	// 	"#fc8452",
	// 	"#9a60b4",
	// 	"#5470c6",
	// }

	js, err := json.Marshal(options)
	if err != nil {
		fmt.Println(err.Error())
	}

	htm = strings.ReplaceAll(htm, "{{ JSON }}", string(js))

	// 替换function的占位字符串{{ yAxisLabelFormatter }}
	yAxisLabelFormatter := "function(value,index){if (value < 1000) {return value;}else{return value/1000+'k';}}"
	htm = strings.ReplaceAll(htm, `"{{ yAxisLabelFormatter }}"`, yAxisLabelFormatter)
	return htm
}

func generateUniqueID() string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits := 6                            // 6 bits to represent a letter index
	letterIdxMask := uint64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := uint64(10)                    // # of letter indices fitting in 63 bits

	buf := make([]byte, letterIdxMax, 1024)

	for cache, remain := fastrand.Uint64(), letterIdxMax; remain != 0; {
		buf[remain-1] = letterBytes[int(cache&letterIdxMask)%len(letterBytes)]
		cache >>= letterIdxBits
		remain--
	}
	buf = append(buf, '_')
	buf = strconv.AppendUint(buf, uint64(time.Now().UnixNano()), 10)
	return string(buf[:len(buf)-3])
}
