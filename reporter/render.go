package reporter

import (
	"bytes"
	"fmt"
	"html/template"
	"io"

	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/src"
	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/templates"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
)

type Charter interface {
	Type() string
	GetAssets() opts.Assets
	Validate()
	SetGlobalOptions(options ...charts.GlobalOpts) *charts.RectChart
}

type TestCase interface {
	ToHtml() string
}

type PerformanceTestCase struct {
	Name     string
	Document string
	Table    *Table
	Picture  Charter
}

func (t *PerformanceTestCase) ToHtml() string {
	var htm string
	var md string
	md += ("## " + t.Name + "\n\n")
	md += (t.Document + "\n\n")
	if t.Table != nil {
		md += (t.Table.ToMarkDown() + "\n\n")
	}
	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	mdOpts := html.RendererOptions{Flags: htmlFlags}
	renderer := html.NewRenderer(mdOpts)
	htm += string(markdown.ToHTML([]byte(md), nil, renderer))

	var err error

	if t.Picture != nil {
		t.Picture.SetGlobalOptions(
			charts.WithTitleOpts(opts.Title{Title: "图-" + t.Name, Left: "30px", Top: "8px"}),
			charts.WithInitializationOpts(opts.Initialization{Width: "800px", Height: "400px"}),
			charts.WithLegendOpts(opts.Legend{Show: true, Top: "12px"}),
			charts.WithTooltipOpts(opts.Tooltip{Show: true, Trigger: "axis"}),
			charts.WithToolboxOpts(opts.Toolbox{Show: true, Right: "20px", Feature: &opts.ToolBoxFeature{SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{Show: true}}}))
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
		t.Picture.Validate()
		err = tpl.Execute(buf, t.Picture)
		if err != nil {
			fmt.Println("execute error", err.Error())
			return htm
		}
		htm += buf.String()
	}
	return htm
}

type Page struct {
	Css       string
	Js        string
	Title     string
	Document  string
	TestCases []TestCase
}

func NewPage(title string) *Page {
	return &Page{
		Css: src.Css,
		Js:  src.EchartsJs,
	}
}

func (p *Page) ToHtmlOneFile(w io.Writer) error {
	pageHeadTmp := `<!DOCTYPE html>
<html>
<head>
	<meta charset="utf-8">
	<title>%s</title>
	<script>%s</script>
	<style>%s</style> 
</head>
<body>
<div class="container">
`

	w.Write([]byte(fmt.Sprintf(pageHeadTmp, p.Title, p.Js, p.Css)))
	pageBody := fmt.Sprintf("<h1>%s</h1>\n<p>%s</p>\n", p.Title, p.Document)
	for _, tcase := range p.TestCases {
		pageBody += tcase.ToHtml()
		pageBody += `<div contenteditable="true"><p>执行无异常。</p></div>`
	}
	w.Write([]byte(pageBody))
	pageEnd := "</div>\n</body>\n</html>"
	w.Write([]byte(pageEnd))
	return nil
}
