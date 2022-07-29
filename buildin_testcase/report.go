package buildin_testcase

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"git.querycap.com/falcontsdb/fctsdb-bench/query_generator"
	"git.querycap.com/falcontsdb/fctsdb-bench/report"
	"git.querycap.com/falcontsdb/fctsdb-bench/report/picture"
	"git.querycap.com/falcontsdb/fctsdb-bench/report/table"
)

// fcbenchCaseDefine 是一个更简化版的报告输出定义
type fcbenchCaseDefine struct {
	Title        string        //case名称
	Document     string        //case说明
	TableColumns []interface{} //table列定义，string类型表示只基础信息，DataDefine表示数据列，可以比较和计算
	Pictures     []PictureDefine
}

type PictureDefine struct {
	Type         string   //图片类型，line折线图，bar柱状图
	XAxisColumn  string   //图片的x坐标取至table的哪一个列
	SeriesColumn []string //图片的series取至table的哪一个列
}

type DataDefine struct {
	Name      string
	Compare   bool
	LowIsGood bool
}

var (
	csvHeaders = []string{"Group", "Mod", "场景", "Series", "并发数", "Batch Size", "查询百分比", "采样时间",
		"P50(r)", "P90(r)", "P95(r)", "P99(r)", "Min(r)", "Max(r)", "Avg(r)", "Fail(r)", "Total(r)", "查询(q/s)",
		"P50(w)", "P90(w)", "P95(w)", "P99(w)", "Min(w)", "Max(w)", "Avg(w)", "Fail(w)", "Total(w)", "Qps(w)", "写入(p/s)", "写入(value/s)", "TotalPoints",
		"RunSec", "Gzip", "Sql", "监控"}
	csvHeaderMap = make(map[string]int)

	performances = make(map[string]*fcbenchCaseDefine)
)

func init() {
	for index, head := range csvHeaders {
		csvHeaderMap[head] = index
	}
}

func init() {

	performances["车载Series变化"] = &fcbenchCaseDefine{
		Title:        "车载场景-写入性能-Series变化",
		Document:     "测试车载场景（1个tag，60个field），series个数对写入性能的影响。",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Series", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载batchsize变化"] = &fcbenchCaseDefine{
		Title:        "车载场景-写入性能-batchsize变化",
		Document:     "测试车载场景（1个tag，60个field），每个http携带数据量（batchsize）对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Batch Size", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载采样时间变化"] = &fcbenchCaseDefine{
		Title:        "车载场景-写入性能-采样时间变化",
		Document:     "测试车载场景（1个tag，60个field），采样时间（每个series两条数据timestamp间隔）对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "采样时间", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载并发数变化"] = &fcbenchCaseDefine{
		Title:        "车载场景-写入性能-并发数变化",
		Document:     "测试车载场景（1个tag，60个field），并发数对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "并发数", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载Gzip变化"] = &fcbenchCaseDefine{
		Title:        "车载场景-写入性能-Gzip等级变化",
		Document:     "测试车载场景（1个tag，60个field），Gzip是否开启及等级对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", "Gzip", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Gzip", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量Series变化"] = &fcbenchCaseDefine{
		Title:        "空气质量场景-写入性能-Series变化",
		Document:     "测试空气质量（5个tag，8个field），series个数对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Series", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量batchsize变化"] = &fcbenchCaseDefine{
		Title:        "空气质量场景-写入性能-batchsize变化",
		Document:     "测试空气质量（5个tag，8个field），每个http携带数据量（batchsize）对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Batch Size", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量采样时间变化"] = &fcbenchCaseDefine{
		Title:        "空气质量场景-写入性能-采样时间变化",
		Document:     "测试空气质量（5个tag，8个field），采样时间（每个series两条数据timestamp间隔）对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "采样时间", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量并发数变化"] = &fcbenchCaseDefine{
		Title:        "空气质量场景-写入性能-并发数变化",
		Document:     "测试空气质量（5个tag，8个field），并发数对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "并发数", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量Gzip变化"] = &fcbenchCaseDefine{
		Title:        "空气质量场景-写入性能-Gzip等级变化",
		Document:     "测试空气质量（5个tag，8个field），Gzip是否开启对写入性能的影响",
		TableColumns: []interface{}{"Series", "并发数", "Batch Size", "采样时间", "Gzip", DataDefine{"写入(p/s)", true, false}, DataDefine{"P95(w)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Gzip", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	// 空气质量查询性能
	performances["空气质量查询-Series变化"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-Series变化",
		Document:     "测试空气质量（5个tag，8个field），tag in查询语句中不同series数量的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-limit数量变化"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-limit数量变化",
		Document:     "测试空气质量（5个tag，8个field），limit查询语句中数量变化的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-shard数量变化"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-shard数量变化",
		Document:     "测试空气质量（5个tag，8个field），查询语句命中的shard数量变化的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-field数量变化"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-field数量变化",
		Document:     "测试空气质量（5个tag，8个field），查询语句中含有的feild数量不同的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-聚合函数"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-聚合函数",
		Document:     "测试空气质量（5个tag，8个field），查询语句中不同聚合函数的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-window函数"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-window函数",
		Document:     "测试空气质量（5个tag，8个field），单并发下不同的window范围的查询响应时间",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-时间排序"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-时间排序",
		Document:     "测试空气质量（5个tag，8个field），查询语句中时间排序对count函数的性能影响",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-并发数变化"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-并发数变化",
		Document:     "测试空气质量（5个tag，8个field），查询语句并发数变化的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "并发数", SeriesColumn: []string{"查询(q/s)"}},
		},
	}
	performances["空气质量查询-group by从句"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-group by从句",
		Document:     "测试空气质量（5个tag，8个field），带group by从句的查询性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-嵌套语句"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-嵌套语句",
		Document:     "测试空气质量（5个tag，8个field），带嵌套语句的查询性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}
	performances["空气质量查询-slimit语句"] = &fcbenchCaseDefine{
		Title:        "空气质量查询-slimit语句",
		Document:     "测试空气质量（5个tag，8个field），带slimit从句的查询性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}

	performances["空气质量查询性能"] = &fcbenchCaseDefine{
		Title:        "空气质量场景-查询性能",
		Document:     "测试空气质量（5个tag，8个field），不同查询语句的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}

	performances["车载查询性能"] = &fcbenchCaseDefine{
		Title:        "车载场景-查询性能",
		Document:     "测试车载场景（1个tag，60个field），不同查询语句的性能",
		TableColumns: []interface{}{"并发数", "Sql", DataDefine{"查询(q/s)", true, false}, DataDefine{"P95(r)", false, false}},
	}

	performances["空气质量混合比例"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定并发总数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定并发总数，变化混合比例。",
		TableColumns: []interface{}{"Series", "并发数", "查询百分比", "Batch Size", "采样时间", DataDefine{"查询(q/s)", true, false}, DataDefine{"写入(p/s)", true, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"写入(p/s)"}},
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"查询(q/s)"}},
		},
	}

	performances["空气质量混合方式1"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定写入并发数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定写入并发数24个，增加查询并发数来改变混合比例。",
		TableColumns: []interface{}{"Series", "并发数", "查询百分比", "Batch Size", "采样时间", DataDefine{"查询(q/s)", true, false}, DataDefine{"写入(p/s)", true, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"写入(p/s)"}},
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"查询(q/s)"}},
		},
	}

	performances["空气质量混合方式2"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定查询并发数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定查询并发数24个，增加写入并发数来改变混合比例。",
		TableColumns: []interface{}{"Series", "并发数", "查询百分比", "Batch Size", "采样时间", DataDefine{"查询(q/s)", true, false}, DataDefine{"写入(p/s)", true, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"写入(p/s)"}},
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"查询(q/s)"}},
		},
	}

	performances["车载混合比例"] = &fcbenchCaseDefine{
		Title: "车载场景-混合性能-固定并发总数",
		Document: "测试车载（1个tag，60个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.Vehicle.Types[1].RawSql + "\n" +
			"本用例固定并发总数，变化混合比例。",
		TableColumns: []interface{}{"Series", "并发数", "查询百分比", "Batch Size", "采样时间", DataDefine{"查询(q/s)", true, false}, DataDefine{"写入(p/s)", true, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"写入(p/s)"}},
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"查询(q/s)"}},
		},
	}

	performances["车载混合方式1"] = &fcbenchCaseDefine{
		Title: "车载场景-混合性能-固定写入并发数",
		Document: "测试车载（1个tag，60个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.Vehicle.Types[1].RawSql + "\n" +
			"本用例固定写入并发数24个，增加查询并发数来改变混合比例。",
		TableColumns: []interface{}{"Series", "并发数", "查询百分比", "Batch Size", "采样时间", DataDefine{"查询(q/s)", true, false}, DataDefine{"写入(p/s)", true, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"写入(p/s)"}},
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"查询(q/s)"}},
		},
	}

	performances["车载混合方式2"] = &fcbenchCaseDefine{
		Title: "车载场景-混合性能-固定查询并发数",
		Document: "测试车载（1个tag，60个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.Vehicle.Types[1].RawSql + "\n" +
			"本用例固定查询并发数24个，增加写入并发数来改变混合比例。",
		TableColumns: []interface{}{"Series", "并发数", "查询百分比", "Batch Size", "采样时间", DataDefine{"查询(q/s)", true, false}, DataDefine{"写入(p/s)", true, false}},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"写入(p/s)"}},
			{Type: "line", XAxisColumn: "查询百分比", SeriesColumn: []string{"查询(q/s)"}},
		},
	}

}

func CreateReport(fileNames ...string) *report.Page {

	// filesOrder := make([]string, 0)

	allCsvRecords := make(map[string][][]string)
	for index := range fileNames {
		csvFile, err := os.OpenFile(fileNames[index]+".csv", os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalln("open result csv failed, error:", err.Error())
		}
		csvReader := csv.NewReader(csvFile)
		_, fileNames[index] = path.Split(fileNames[index]) //只取文件名字，去掉路径
		allCsvRecords[fileNames[index]], err = csvReader.ReadAll()
		if err != nil {
			log.Fatalln("read csv error:", err.Error())
		}
	}

	// 整体完成效果：
	// 如果是单个文件，只记录简单的数据并进行画图
	// 如果是多个文件，说明要对不同文件中相同的内容进行数据比较，计算提升百分比
	benchReport := report.NewPage("性能测试")
	benchReport.Document = "测试海东青数据库的性能\n" + "使用工具：fcbench\n"
	var currentTestCase *report.PerformanceTestCase
	var tableSep string = "<br>" //表中分隔符

	// 取第一个cse文件开始遍历
	for rowIndex, row := range allCsvRecords[fileNames[0]] {
		// 跳过第一行
		if rowIndex == 0 {
			continue
		}
		// 处理尾行携带的环境信息
		if row[0] == "test-env" {
			benchReport.Document += strings.ReplaceAll(strings.Split(row[1], "**")[0], ";", "\n")
			for _, fileName := range fileNames {
				benchReport.Document += "\n"
				infos := strings.Split(allCsvRecords[fileName][rowIndex][1], "**")
				if len(infos) > 1 {
					benchReport.Document += fileName
					benchReport.Document += infos[len(infos)-1]
				}
			}
		} else {
			if caseDefine, ok := performances[row[0]]; ok {
				// 步骤1：判断是否需要在报告中新增一个测试用例，并同时给测试用例创建表格
				if benchReport.HasTestCase(row[0]) {
					currentTestCase = benchReport.GetTestCase(row[0]).(*report.PerformanceTestCase)
				} else {

					// 步骤2：处理表头信息
					tableHeaders := make([]string, 0)
					for _, column := range caseDefine.TableColumns {
						switch column := column.(type) {
						case string:
							// 步骤2.1：记录所有信息表头，所有文件只需要记录一次
							tableHeaders = append(tableHeaders, column)
						case DataDefine:
							// 步骤2.1：记录所有数据表头，每个文件都需要记录一次
							for _, fileName := range fileNames {
								tableHeaders = append(tableHeaders, column.Name+tableSep+fileName)
							}

							// 步骤2.2：拼接“比较第一个csv和最后一个csv的差值"的表头
							if len(fileNames) > 1 && column.Compare {
								keywords := strings.Split(column.Name, "(")[0]
								tableHeaders = append(tableHeaders, "比较"+keywords+tableSep+fileNames[0]+"与"+fileNames[len(fileNames)-1])
							}
						default:
						}
					}

					// 步骤1.3：添加监控列
					for _, fileName := range fileNames {
						tableHeaders = append(tableHeaders, "监控"+tableSep+fileName)
					}

					currentTestCase = report.NewPerformanceTestCase(row[0])
					currentTestCase.Title = caseDefine.Title
					currentTestCase.Document = caseDefine.Document + "\n单位解释：p/s - points/秒 、q/s - queries/秒、P95响应时间单位（ms）"
					currentTestCase.Table = table.CreateTable(tableHeaders...)
					currentTestCase.Conclusion = "执行无异常"
					benchReport.AddTestCase(currentTestCase)
				}

				// 步骤3：将数据记录到测试用例中
				var rowData []interface{}
				for _, column := range caseDefine.TableColumns {
					switch column := column.(type) {
					case string:
						// 步骤1.1：记录所有信息数据，只需记录一次
						rowData = append(rowData, row[csvHeaderMap[column]])
					case DataDefine:

						// 步骤1.1：记录所有数据，每个文件都要记录
						for _, fileName := range fileNames {
							rowData = append(rowData, allCsvRecords[fileName][rowIndex][csvHeaderMap[column.Name]])
						}

						// 步骤1.2：拼接“比较第一个csv和最后一个csv的差值"的表头
						if len(fileNames) > 1 && column.Compare {
							oldData, err := strconv.ParseFloat(allCsvRecords[fileNames[0]][rowIndex][csvHeaderMap[column.Name]], 64)
							if err != nil {
								rowData = append(rowData, "error")
								continue
							}
							newData, err := strconv.ParseFloat(allCsvRecords[fileNames[len(fileNames)-1]][rowIndex][csvHeaderMap[column.Name]], 64)
							if err != nil {
								rowData = append(rowData, "error")
								continue
							}

							rate := (newData - oldData) / oldData * 100
							// 显示时着色
							if rate > 5 {
								if column.LowIsGood {
									rowData = append(rowData, table.Cell{Text: fmt.Sprintf("%.2f%%", rate), Color: "orangered"})
								} else {
									rowData = append(rowData, table.Cell{Text: fmt.Sprintf("%.2f%%", rate), Color: "limegreen"})
								}
							} else if rate < -5 {
								if column.LowIsGood {
									rowData = append(rowData, table.Cell{Text: fmt.Sprintf("%.2f%%", rate), Color: "limegreen"})
								} else {
									rowData = append(rowData, table.Cell{Text: fmt.Sprintf("%.2f%%", rate), Color: "orangered"})
								}
							} else {
								rowData = append(rowData, fmt.Sprintf("%.2f%%", rate))
							}
						}
					default:
					}
				}

				// 步骤3.4：监控列
				for _, fileName := range fileNames {
					rowData = append(rowData, table.Cell{Text: "地址", Href: allCsvRecords[fileName][rowIndex][csvHeaderMap["监控"]]})
				}
				// 步骤3.5：判断执行过程中是否有失败的数据
				for _, fileName := range fileNames {
					if (allCsvRecords[fileName][rowIndex][csvHeaderMap["Fail(r)"]] != "0" && allCsvRecords[fileName][rowIndex][csvHeaderMap["Fail(r)"]] != "") ||
						(allCsvRecords[fileName][rowIndex][csvHeaderMap["Fail(w)"]] != "0" && allCsvRecords[fileName][rowIndex][csvHeaderMap["Fail(w)"]] != "") {
						log.Printf("Need attention: the csv %s (line %d) has error http requests!!!!", fileName, rowIndex)
						currentTestCase.Conclusion = "<b>执行有异常数据，请查看详细数据记录表</b>"
					}
				}

				currentTestCase.Table.AddRows(rowData...)
			}
		}
	}

	// 步骤4：设置图片
	for _, testcase := range benchReport.TestCases {
		testcase := testcase.(*report.PerformanceTestCase)
		if caseDefine, ok := performances[testcase.GetName()]; ok {
			for _, picDefine := range caseDefine.Pictures {
				switch picDefine.Type {
				case "line":
					words := strings.Split(testcase.Title, "-")
					line := picture.NewLine(words[len(words)-1] + "-" + picDefine.SeriesColumn[0])
					line.SetXAxis(testcase.Table.GetColumn(picDefine.XAxisColumn))
					for _, field := range picDefine.SeriesColumn {
						for _, fileName := range fileNames {
							line.AddSeries(field+":"+fileName, testcase.Table.GetColumn(field+tableSep+fileName))
						}
					}
					testcase.Pictures = append(testcase.Pictures, line)
				}
			}
		}
	}

	return benchReport
}
