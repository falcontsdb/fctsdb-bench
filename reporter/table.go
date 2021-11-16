package reporter

import (
	"fmt"
	"strings"
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
