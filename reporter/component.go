package reporter

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"

	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/templates"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type Table struct {
	Columns []string
	RowHead []string
	Rows    [][]string
}

func CreateTable(columns ...string) *Table {
	return &Table{Columns: columns}
}

func (p *Table) AddRows(cells ...interface{}) {
	p.RowHead = append(p.RowHead, fmt.Sprintf("%v", cells[0]))
	var rowCells []string
	for _, cell := range cells {
		rowCells = append(rowCells, fmt.Sprintf("%v", cell))
	}
	p.Rows = append(p.Rows, rowCells)
}

func (p *Table) GetColumn(name string) []string {
	var columnIndex int = -1
	var col string
	for columnIndex, col = range p.Columns {
		if col == name {
			break
		}
	}
	var column []string
	if columnIndex < 0 {
		return column
	} else {
		for _, row := range p.Rows {
			column = append(column, row[columnIndex])
		}
	}
	return column
}

func (p *Table) ToMarkDown() string {
	columnMaxLength := make([]int, len(p.Columns))
	for i, h := range p.Columns {
		columnMaxLength[i] = len(h)
	}

	for _, row := range p.Rows {
		for i, cell := range row {
			maxLength := max(len(cell), columnMaxLength[i])
			columnMaxLength[i] = maxLength
		}
	}

	lines := make([]string, len(p.Rows)+2)
	fillchar := " "
	for columnIndex, column := range p.Columns {
		lines[0] += left(column, columnMaxLength[columnIndex], fillchar)
		lines[1] += left("", columnMaxLength[columnIndex], "-")
		for rowIndex, row := range p.Rows {
			if columnIndex < len(row) {
				lines[rowIndex+2] += left(row[columnIndex], columnMaxLength[columnIndex], fillchar)
			} else {
				lines[rowIndex+2] += left("null", columnMaxLength[columnIndex], fillchar)
			}

		}
		if columnIndex != len(p.Columns)-1 {
			for lineIndex := range lines {
				lines[lineIndex] += "|"
			}
		}
	}
	return strings.Join(lines, "\n")
}

func left(word string, length int, fillchar string) string {
	result := word
	for i := 0; i < length-len(word); i++ {
		result += fillchar
	}
	return result
}

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

type Charter interface {
	Type() string
	GetAssets() opts.Assets
	Validate()
	SetGlobalOptions(options ...charts.GlobalOpts) *charts.RectChart
}

type Picture interface {
	SetXAxis([]string)
	AddSeries(string, []string)
	ToHtml() string
}

type Line struct {
	name   string
	xAxis  []string
	series map[string][]string
}

func NewLine(name string) *Line {
	return &Line{
		name:   name,
		xAxis:  make([]string, 0),
		series: make(map[string][]string),
	}
}

func (l *Line) AddSeries(name string, series []string) {
	l.series[name] = series
}

func (l *Line) SetXAxis(xAxis []string) {
	l.xAxis = xAxis
}

func (l *Line) ToHtml() string {
	cline := charts.NewLine()
	cline = cline.SetXAxis(l.xAxis)
	for key, datas := range l.series {
		lineDatas := make([]opts.LineData, len(datas))
		for i, data := range datas {
			lineDatas[i] = opts.LineData{Value: data}
		}
		cline = cline.AddSeries(key, lineDatas, charts.WithLabelOpts(opts.Label{Show: true}))
	}

	return renderCharter(l.name, cline)
}

func renderCharter(name string, charter Charter) string {
	htm := ""
	var err error
	charter.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "图-" + name, Top: "0px", TitleStyle: &opts.TextStyle{FontSize: 14}}),
		charts.WithInitializationOpts(opts.Initialization{Width: "800px", Height: "320px"}),
		charts.WithLegendOpts(opts.Legend{Show: true, Top: "20px"}),
		charts.WithTooltipOpts(opts.Tooltip{Show: true, Trigger: "axis"}),
		charts.WithToolboxOpts(opts.Toolbox{Show: true, Right: "20px", Feature: &opts.ToolBoxFeature{SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: true}}}),
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
