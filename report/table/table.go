package table

import (
	"fmt"
	"strings"
)

type Table struct {
	Columns []string
	RowHead []string //不使用map存储rows，会打乱行顺序
	Rows    [][]Cell
}

type Cell struct {
	Text            string
	Color           string
	BackgroundColor string
	Href            string
}

func CreateTable(columns ...string) *Table {
	return &Table{Columns: columns}
}

func (p *Table) AddRows(cells ...interface{}) {
	p.RowHead = append(p.RowHead, fmt.Sprintf("%v", cells[0]))
	var rowCells []Cell
	for _, cell := range cells {
		switch cell := cell.(type) {
		case Cell:
			rowCells = append(rowCells, cell)
		default:
			rowCells = append(rowCells, Cell{
				Text: fmt.Sprintf("%v", cell),
			})
		}
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
			column = append(column, row[columnIndex].Text)
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
			maxLength := max(len(cell.Text), columnMaxLength[i])
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
				lines[rowIndex+2] += left(row[columnIndex].Text, columnMaxLength[columnIndex], fillchar)
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

// ToHtml() 渲染成html表格，如下：
// <table>
// <thead>
// <tr>
// <th>查询百分比</th>
// <th>写入(point/s): v139</th>
// <th>写入(point/s): v140</th>
// <th>比较写入: v139与v140</th>
// <th>监控: v140</th>
// </tr>
// </thead>
// <tbody>
// <tr>
// <td>20</td>
// <td>64058.12</td>
// <td>64194.44</td>
// <td>0.21%</td>
// <td><a href="http://124.71.230.36:4000/sources/1" target="_blank">地址</a></td>
// </tr>
// <tr>
// <td>80</td>
// <td>1000</td>
// <td>5565.04</td>
// <td>4505.32</td>
// <td style="color:orangered">-19.04%</td>
// <td><a href="http://124.71.230.36:4000/sources/1" target="_blank">地址</a></td>
// </tr>
// </tbody>
// </table>
func (p *Table) ToHtml() string {

	htm := "<table>\n"
	htm += "<thead>\n<tr>\n"

	// 渲染thead
	for _, col := range p.Columns {
		htm += fmt.Sprintf("<th>%s</th>\n", col)
	}
	htm += "</tr>\n</thead>\n"

	// 渲染tbody
	htm += "<tbody>\n"
	for _, row := range p.Rows {
		htm += "<tr>\n"
		for _, cell := range row {

			// 最终渲染效果应该和下面类似：
			// 超链接：<td><a href="http://124.71.230.36:4000/sources/1/dashboards/4" target="_blank">地址</a></td>
			// 带颜色：<td style="color:#ccc;background-color:#fff">-96.57%</td>
			htm += "<td"

			style := make([]string, 0)
			if cell.BackgroundColor != "" {
				style = append(style, "background-color:"+cell.BackgroundColor)
			}
			if cell.Color != "" {
				style = append(style, "color:"+cell.Color)
			}
			if len(style) != 0 {
				htm += fmt.Sprintf(" style=\"%s\"", strings.Join(style, ";"))
			}

			htm += ">"
			if cell.Href != "" {
				htm += fmt.Sprintf("<a href=\"%s\" target=\"_blank\">%s</a>", cell.Href, cell.Text)
			} else {
				htm += cell.Text
			}
			htm += "</td>\n"

		}
		htm += "</tr>\n"
	}
	htm += "</tbody>\n"
	htm += "</table>\n"

	return htm
}
