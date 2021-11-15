package reporter

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/src"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
)

func generateBarItems() []interface{} {
	items := make([]interface{}, 0)
	for i := 0; i < 7; i++ {
		items = append(items, rand.Intn(3000))
	}
	return items
}

func TestMarkdown(t *testing.T) {
	// pf := &Table{Name: "变化用例"}
	// pf.Document = "我们注意到influxdb-comparisons是先生成数据到文件或者stdout，再由另一个工具写入到数据库。\n因此参照influxdb-comparisons工具，提供以下类似的命令进行方式对比。\n"
	pf := CreateTable("ID", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
	pf.AddRows(generateBarItems()...)
	pf.AddRows(generateBarItems()...)
	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	opts := html.RendererOptions{Flags: htmlFlags}
	renderer := html.NewRenderer(opts)
	md := pf.ToMarkDown()
	fmt.Println(md)
	ht := markdown.ToHTML([]byte(md), nil, renderer)
	f, _ := os.Create("../hah.html")
	defer f.Close()
	f.Write(ht)
}

func TestJs(t *testing.T) {
	f, _ := os.Create("hah.html")
	defer f.Close()
	f.Write([]byte(src.EchartsJs))
}

func generateLineItems() []opts.LineData {
	items := make([]opts.LineData, 0)
	for i := 0; i < 7; i++ {
		items = append(items, opts.LineData{Value: rand.Intn(300)})
	}
	return items
}
func TestCharts(t *testing.T) {
	line := charts.NewLine()
	line.SetGlobalOptions(charts.WithTitleOpts(opts.Title{
		Title: "Two",
		// Subtitle: "It's extremely easy to use, right?",
	}))

	line.SetXAxis([]string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}).
		AddSeries("Category A", generateLineItems(), charts.WithLabelOpts(opts.Label{Show: true})).
		AddSeries("Category B", generateLineItems(), charts.WithLabelOpts(opts.Label{Show: true}))
	// tpl := template.Must(template.New("line").Parse("")).Funcs(template.FuncMap{
	// 	"safeJS": func(s interface{}) template.JS {
	// 		return template.JS(fmt.Sprint(s))
	// 	},
	// })
	// tpl = template.Must(tpl.Parse(templates.BaseTpl))
	// // if err != nil {
	// // 	fmt.Println("parse error: ", err.Error())
	// // }
	// err := tpl.Execute(buf, line)
	// if err != nil {
	// 	fmt.Println("exec", err.Error())
	// }
	// fmt.Println(buf.String())

	testcase := PerformanceTestCase{
		name:     "aaa",
		Document: "ddddd",
		// Picture:  line,
	}

	fmt.Println(testcase.ToHtml())
	f, _ := os.Create("hah.html")
	defer f.Close()
	f.Write([]byte(testcase.ToHtml()))

}

func TestPage(t *testing.T) {
	line := charts.NewLine()
	line.SetXAxis([]string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}).
		AddSeries("Category A", generateLineItems(), charts.WithLabelOpts(opts.Label{Show: true})).
		AddSeries("Category B", generateLineItems(), charts.WithLabelOpts(opts.Label{Show: true}))

	table := CreateTable("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
	table.AddRows("1", "2", "3", "5", "6", "7", "8")
	table.AddRows("1", "2", "3", "5", "6", "7", "8")
	table.AddRows("1", "2", "3", "5", "6", "7", "8")
	table.AddRows("1", "2", "3", "5", "6", "7", "8")

	testcase := &PerformanceTestCase{
		name:     "aaa",
		Document: "ddddd",
		// Picture:  line,
		Table: table,
	}

	page := &Page{
		Title:     "性能测试",
		Document:  "数据库性能测试",
		Js:        src.EchartsJs,
		Css:       src.Css,
		TestCases: []TestCase{testcase},
	}

	// fmt.Println(testcase.ToHtml())
	f, _ := os.Create("hah.html")
	defer f.Close()
	// f.Write([]byte(testcase.ToHtml()))
	page.ToHtmlOneFile(f)

}
