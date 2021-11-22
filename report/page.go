package report

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"git.querycap.com/falcontsdb/fctsdb-bench/report/src"
	"git.querycap.com/falcontsdb/fctsdb-bench/report/table"
)

type Picture interface {
	SetXAxis([]string)
	AddSeries(string, []string)
	SetSmallSize(bool)
	ToHtml() string
}

type TestCase interface {
	GetName() string
	ToHtml(string) string
	ToMarkDown(string) string
}

type PerformanceTestCase struct {
	name     string
	Title    string
	Document string
	Table    *table.Table
	Pictures []Picture
}

func NewPerformanceTestCase(name string) *PerformanceTestCase {
	return &PerformanceTestCase{
		name: name,
	}
}

func (t *PerformanceTestCase) GetName() string {
	return t.name
}

func (t *PerformanceTestCase) ToHtml(index string) string {

	// title 是二级目录
	htm := ("<h2>" + index + t.Title + "</h2>\n")

	// 对document进行分行渲染
	for _, line := range strings.Split(t.Document, "\n") {
		if line != "" {
			htm += ("<p>" + line + "</p>\n")
		}
	}

	// 添加table
	if t.Table != nil {
		htm += t.Table.ToHtml()
	}

	// 添加picture
	for _, pic := range t.Pictures {
		if len(t.Pictures) == 1 {
			htm += pic.ToHtml()
		} else {
			pic.SetSmallSize(true)
			htm += pic.ToHtml()
		}
	}

	return htm
}

func (t *PerformanceTestCase) ToMarkDown(index string) string {
	var md string
	md += ("## " + index + t.Title + "\n\n")
	md += (strings.ReplaceAll(t.Document, "\n", "\n\n") + "\n\n")
	if t.Table != nil {
		md += (t.Table.ToMarkDown() + "\n\n")
	}
	return md
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
		Title:     title,
		Css:       src.Css,
		Js:        src.EchartsJs,
		TestCases: make([]TestCase, 0),
	}
}

func (p *Page) HasTestCase(name string) bool {
	return getTestCaseIndex(name, p.TestCases) >= 0
}

func (p *Page) AddTestCase(testCase TestCase) {
	p.TestCases = append(p.TestCases, testCase)
}

func (p *Page) GetTestCase(name string) TestCase {
	return p.TestCases[getTestCaseIndex(name, p.TestCases)]
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
	pageBody := fmt.Sprintf("<h1>%s</h1>\n", p.Title)
	for _, line := range strings.Split(p.Document, "\n") {
		pageBody += fmt.Sprintf("<p>%s</p>\n", line)
	}
	for index, tcase := range p.TestCases {
		pageBody += tcase.ToHtml(strconv.Itoa(index+1) + ". ")
		pageBody += `<div contenteditable="true"><p>执行无异常。</p></div>`
	}
	w.Write([]byte(pageBody))
	pageEnd := "</div>\n</body>\n</html>"
	w.Write([]byte(pageEnd))
	return nil
}

func (p *Page) ToMarkDown(w io.Writer) error {
	var md string
	md += ("# " + p.Title + "\n\n")
	md += (strings.ReplaceAll(p.Document, "\n", "\n\n") + "\n\n")
	for index, tcase := range p.TestCases {
		md += tcase.ToMarkDown(strconv.Itoa(index+1) + ". ")
	}
	_, err := w.Write([]byte(md))
	return err
}

func getTestCaseIndex(name string, slice []TestCase) int {
	for i, v := range slice {
		if v.GetName() == name {
			return i
		}
	}
	return -1
}
