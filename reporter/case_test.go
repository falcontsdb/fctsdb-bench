package reporter

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"git.querycap.com/falcontsdb/fctsdb-bench/reporter/src"
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
