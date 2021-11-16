package reporter

import (
	"bytes"
	"fmt"
	"html/template"

	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/templates"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type Bar struct {
	name   string
	xAxis  []string
	series map[string][]string
}

func NewBar(name string) *Bar {
	return &Bar{
		name:   name,
		xAxis:  make([]string, 0),
		series: make(map[string][]string),
	}
}

func (l *Bar) AddSeries(name string, series []string) {
	l.series[name] = series
}

func (l *Bar) SetXAxis(xAxis []string) {
	l.xAxis = xAxis
}

func (l *Bar) ToHtml() string {

	cline := charts.NewBar()
	cline = cline.SetXAxis(l.xAxis)
	for key, datas := range l.series {
		barDatas := make([]opts.BarData, len(datas))
		for i, data := range datas {
			barDatas[i] = opts.BarData{Value: data}
		}
		cline = cline.AddSeries(key, barDatas, charts.WithLabelOpts(opts.Label{Show: true})) //, charts.WithLineChartOpts(opts.LineChart{YAxisIndex: 1}))
	}
	return renderCharter(l.name, cline)
}

func renderCharter(name string, charter Charter) string {
	htm := ""
	var err error
	charter.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "图-" + name, Top: "0px", TitleStyle: &opts.TextStyle{FontSize: 14}}),                                              // 图注
		charts.WithInitializationOpts(opts.Initialization{Width: "800px", Height: "320px"}),                                                                      // 全局大小
		charts.WithLegendOpts(opts.Legend{Show: true, Top: "20px"}),                                                                                              // 图例
		charts.WithTooltipOpts(opts.Tooltip{Show: true, Trigger: "axis"}),                                                                                        // tip标签
		charts.WithToolboxOpts(opts.Toolbox{Show: true, Right: "20px", Feature: &opts.ToolBoxFeature{SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: true}}}), // 下载图
		charts.WithYAxisOpts(opts.YAxis{AxisLabel: &opts.AxisLabel{Formatter: "function(value,index){if (value >=1000) { value = value/1000+'k';}else if(value <1000){value = value;}return value}"}}),
	)

	buf := bytes.NewBuffer(make([]byte, 0, 4*1024))
	tpl, _ := template.New("chart").Parse("")
	tpl.Funcs(template.FuncMap{
		"safeJS": func(s interface{}) template.JS {
			return template.JS(fmt.Sprint(s))
		},
	})
	tpl, err = tpl.Parse(templates.BaseTpl)
	if err != nil {
		fmt.Println("parse error: ", err.Error())
		return htm
	}
	charter.Validate()
	err = tpl.Execute(buf, charter)
	if err != nil {
		fmt.Println("execute error", err.Error())
		return htm
	}
	htm += buf.String()
	return htm
}
