package reporter

import (
	"fmt"
	"io"
	"strings"

	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/src"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
)

type TestCase interface {
	GetName() string
	ToHtml() string
}

type PerformanceTestCase struct {
	name     string
	Title    string
	Document string
	Table    *Table
	Picture  Picture
}

func NewPerformanceTestCase(name string) *PerformanceTestCase {
	return &PerformanceTestCase{
		name: name,
	}
}

func (t *PerformanceTestCase) GetName() string {
	return t.name
}

func (t *PerformanceTestCase) ToHtml() string {
	var htm string
	var md string
	md += ("## " + t.name + "\n\n")
	md += (strings.ReplaceAll(t.Document, "\n", "\n\n") + "\n\n")
	if t.Table != nil {
		md += (t.Table.ToMarkDown() + "\n\n")
	}
	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	mdOpts := html.RendererOptions{Flags: htmlFlags}
	renderer := html.NewRenderer(mdOpts)
	htm += string(markdown.ToHTML([]byte(md), nil, renderer))
	if t.Picture != nil {
		htm += t.Picture.ToHtml()
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
	for _, tcase := range p.TestCases {
		pageBody += tcase.ToHtml()
		pageBody += `<div contenteditable="true"><p>执行无异常。</p></div>`
	}
	w.Write([]byte(pageBody))
	pageEnd := "</div>\n</body>\n</html>"
	w.Write([]byte(pageEnd))
	return nil
}

func getTestCaseIndex(name string, slice []TestCase) int {
	for i, v := range slice {
		if v.GetName() == name {
			return i
		}
	}
	return -1
}
