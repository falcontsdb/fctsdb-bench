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
	Title       string   //case名称
	Document    string   //case说明
	TableTags   []string //记录非数据列，类似于海东青数据库的tag
	TableFeilds []string //记录数据列，类似于海东青数据库的feild
	Pictures    []PictureDefine
}

type PictureDefine struct {
	Type         string   //图片类型，line折线图，bar柱状图
	XAxisColumn  string   //图片的x坐标取至table的哪一个列
	SeriesColumn []string //图片的series取至table的哪一个列
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
		Title:       "车载场景-写入性能-Series变化",
		Document:    "测试车载场景（1个tag，60个field），series个数对写入性能的影响。",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Series", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载batchsize变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-batchsize变化",
		Document:    "测试车载场景（1个tag，60个field），每个http携带数据量（batchsize）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Batch Size", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载采样时间变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-采样时间变化",
		Document:    "测试车载场景（1个tag，60个field），采样时间（每个series两条数据timestamp间隔）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "采样时间", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载并发数变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-并发数变化",
		Document:    "测试车载场景（1个tag，60个field），并发数对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "并发数", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["车载Gzip变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-Gzip等级变化",
		Document:    "测试车载场景（1个tag，60个field），Gzip是否开启及等级对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间", "Gzip"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Gzip", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量Series变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-Series变化",
		Document:    "测试空气质量（5个tag，8个field），series个数对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Series", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量batchsize变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-batchsize变化",
		Document:    "测试空气质量（5个tag，8个field），每个http携带数据量（batchsize）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Batch Size", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量采样时间变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-采样时间变化",
		Document:    "测试空气质量（5个tag，8个field），采样时间（每个series两条数据timestamp间隔）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "采样时间", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量并发数变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-并发数变化",
		Document:    "测试空气质量（5个tag，8个field），并发数对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "并发数", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量Gzip变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-Gzip等级变化",
		Document:    "测试空气质量（5个tag，8个field），Gzip是否开启对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间", "Gzip"},
		TableFeilds: []string{"写入(p/s)"},
		Pictures: []PictureDefine{
			{Type: "line", XAxisColumn: "Gzip", SeriesColumn: []string{"写入(p/s)"}},
		},
	}

	performances["空气质量查询性能"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-查询性能",
		Document:    "测试空气质量（5个tag，8个field），不同查询语句的性能",
		TableTags:   []string{"场景", "并发数", "Sql"},
		TableFeilds: []string{"查询(q/s)"},
	}

	performances["车载查询性能"] = &fcbenchCaseDefine{
		Title:       "车载场景-查询性能",
		Document:    "测试车载场景（1个tag，60个field），不同查询语句的性能",
		TableTags:   []string{"场景", "并发数", "Sql"},
		TableFeilds: []string{"查询(q/s)"},
	}

	performances["空气质量混合比例"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定并发总数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定并发总数，变化混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(q/s)", "写入(p/s)"},
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
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(q/s)", "写入(p/s)"},
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
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(q/s)", "写入(p/s)"},
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
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(q/s)", "写入(p/s)"},
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
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(q/s)", "写入(p/s)"},
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
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(q/s)", "写入(p/s)"},
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
		// 处理尾行
		if row[0] == "test-env" {
			benchReport.Document += strings.ReplaceAll(row[1], ";", "\n")
		} else {
			if caseDefine, ok := performances[row[0]]; ok {
				tableHeaders := caseDefine.TableTags

				// 步骤1：处理表头
				if len(fileNames) > 1 {
					// 多个文件需要进行以下步骤：
					// 步骤1.1：记录所有csv的相同field值
					for _, field := range caseDefine.TableFeilds {
						for _, fileName := range fileNames {
							tableHeaders = append(tableHeaders, field+tableSep+fileName)
						}
					}
					// 步骤1.2：比较第一个csv和最后一个csv的差值
					for _, field := range caseDefine.TableFeilds {
						keywords := strings.Split(field, "(")[0]
						tableHeaders = append(tableHeaders, "比较"+keywords+tableSep+fileNames[0]+"与"+fileNames[len(fileNames)-1])
					}
					// 步骤1.3：添加监控列
					for _, fileName := range fileNames {
						tableHeaders = append(tableHeaders, "监控"+tableSep+fileName)
					}
				} else {
					// 单个文件仅记录数据
					tableHeaders = append(tableHeaders, caseDefine.TableFeilds...)
					tableHeaders = append(tableHeaders, "监控")
				}

				// 步骤2：判断是否需要创建表格
				if benchReport.HasTestCase(row[0]) {
					currentTestCase = benchReport.GetTestCase(row[0]).(*report.PerformanceTestCase)
				} else {
					currentTestCase = report.NewPerformanceTestCase(row[0])
					currentTestCase.Title = caseDefine.Title
					currentTestCase.Document = caseDefine.Document + "\n单位解释：p/s - points/秒 、q/s - queries/秒"
					currentTestCase.Table = table.CreateTable(tableHeaders...)
					benchReport.AddTestCase(currentTestCase)
				}

				// 步骤3：记录数据
				var rowData []interface{}
				if len(fileNames) > 1 {

					// 步骤3.1：先记录tag
					for _, header := range caseDefine.TableTags {
						// 替换场景的单词，方便显示美观
						data := row[csvHeaderMap[header]]
						switch data {
						case "vehicle":
							data = "车载"
						case "air-quality":
							data = "AirQ"
						}
						rowData = append(rowData, data)
					}
					// 步骤3.2：记录所有csv的相同field值
					for _, field := range caseDefine.TableFeilds {
						for _, fileName := range fileNames {
							rowData = append(rowData, allCsvRecords[fileName][rowIndex][csvHeaderMap[field]])
						}
					}
					// 步骤3.3：比较第一个csv和最后一个csv的差值
					for _, field := range caseDefine.TableFeilds {
						oldData, err := strconv.ParseFloat(allCsvRecords[fileNames[0]][rowIndex][csvHeaderMap[field]], 64)
						if err != nil {
							rowData = append(rowData, "error")
							continue
						}
						newData, err := strconv.ParseFloat(allCsvRecords[fileNames[len(fileNames)-1]][rowIndex][csvHeaderMap[field]], 64)
						if err != nil {
							rowData = append(rowData, "error")
							continue
						}

						rate := (newData - oldData) / oldData * 100
						if rate > 5 {
							rowData = append(rowData, table.Cell{Text: fmt.Sprintf("%.2f%%", rate), Color: "limegreen"})
						} else if rate < -5 {
							rowData = append(rowData, table.Cell{Text: fmt.Sprintf("%.2f%%", rate), Color: "orangered"})
						} else {
							rowData = append(rowData, fmt.Sprintf("%.2f%%", rate))
						}

					}

					// 步骤3.4：监控列
					for _, fileName := range fileNames {

						// rowData = append(rowData, "[地址]("+allCsvRecords[fileName][rowIndex][csvHeaderMap["监控"]]+")")
						rowData = append(rowData, table.Cell{Text: "地址", Href: allCsvRecords[fileName][rowIndex][csvHeaderMap["监控"]]})
					}

				} else {
					for _, header := range tableHeaders {
						if header == "监控" { // 监控特例化
							// rowData = append(rowData, "[地址]("+row[csvHeaderMap[header]]+")")
							rowData = append(rowData, table.Cell{Text: "地址", Href: row[csvHeaderMap[header]]})
						} else {
							// 替换场景的单词，方便显示美观
							data := row[csvHeaderMap[header]]
							switch data {
							case "vehicle":
								data = "车载"
							case "air-quality":
								data = "AirQ"
							}
							rowData = append(rowData, data)
						}
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
					if len(fileNames) > 1 {
						for _, field := range picDefine.SeriesColumn {
							for _, fileName := range fileNames {
								line.AddSeries(field+":"+fileName, testcase.Table.GetColumn(field+tableSep+fileName))
							}
						}
					} else {
						for _, field := range picDefine.SeriesColumn {
							line.AddSeries(field, testcase.Table.GetColumn(field))
						}
					}
					testcase.Pictures = append(testcase.Pictures, line)
				}
			}
		}
	}

	return benchReport
}
