package picture

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"unsafe"
)

type Line struct {
	name   string
	xAxis  []string
	series map[string][]string
	small  bool
}

func NewLine(name string) *Line {
	return &Line{
		name:   name,
		xAxis:  make([]string, 0),
		series: make(map[string][]string),
		small:  false,
	}
}

func (l *Line) AddSeries(name string, series []string) {
	l.series[name] = series
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

	id := generateUniqueID(10)

	htm := strings.ReplaceAll(tmp, "{{ ChartID }}", id)
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
	for key, values := range l.series {
		ser := make(map[string]interface{})
		ser["name"] = key
		ser["type"] = "line"
		ser["waveAnimation"] = false
		// ser["renderLabelForZeroData"] = false
		// ser["selectedMode"] = false
		ser["animation"] = false
		ser["label"] = map[string]interface{}{"show": true}

		// datas := make([]map[string]interface{}, 0)
		// for _, value := range values {
		// 	data := make(map[string]interface{})
		// 	data["value"] = value
		// 	datas = append(datas, data)
		// }
		ser["data"] = values
		series = append(series, ser)
	}
	options["series"] = series

	options["yAxis"] = map[string]interface{}{
		"type": "value",
		"axisLabel": map[string]string{
			"formatter": "{{ yAxisLabelFormatter }}",
		},
	}

	options["title"] = map[string]interface{}{
		"text":      l.name,
		"textStyle": map[string]interface{}{"fontSize": 14},
		"top":       "0px",
	}

	options["tooltip"] = map[string]interface{}{
		// "show":    true,
		"trigger": "axis",
	}
	options["legend"] = map[string]interface{}{
		// "show": true,
		"top": "20px",
	}
	options["toolbox"] = map[string]interface{}{
		// "show":  true,
		"right": "20px",
		"feature": map[string]interface{}{
			"saveAsImage": map[string]interface{}{
				// "show": true, dd
			},
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

	yAxisLabelFormatter := "function(value,index){if (value < 1000) {return value;}else{return value/1000+'k';}}"
	htm = strings.ReplaceAll(htm, `"{{ yAxisLabelFormatter }}"`, yAxisLabelFormatter)
	return htm
}

func generateUniqueID(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits := 6                           // 6 bits to represent a letter index
	letterIdxMask := int64(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 63 / letterIdxBits           // # of letter indices fitting in 63 bits

	buf := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			buf[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&buf))
}
