package buildin_testcase

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	"git.querycap.com/falcontsdb/fctsdb-bench/query_generator"
	"git.querycap.com/falcontsdb/fctsdb-bench/reporter"
)

// fcbenchCaseDefine 是一个更简化版的报告输出定义
type fcbenchCaseDefine struct {
	Title       string   //case名称
	Document    string   //case说明
	TableTags   []string //记录非数据列，类似于海东青数据库的tag
	TableFeilds []string //记录数据列，类似于海东青数据库的feild
	PicType     string   //图片类型，line折线图，bar柱状图
	PicXAxisTag string   //图片的x坐标取至table的哪一个列
}

var (
	csvHeaders = []string{"Group", "Mod", "场景", "Series", "并发数", "Batch Size", "查询百分比", "采样时间",
		"P50(r)", "P90(r)", "P95(r)", "P99(r)", "Min(r)", "Max(r)", "Avg(r)", "Fail(r)", "Total(r)", "查询(query/s)",
		"P50(w)", "P90(w)", "P95(w)", "P99(w)", "Min(w)", "Max(w)", "Avg(w)", "Fail(w)", "Total(w)", "Qps(w)", "写入(point/s)", "写入(value/s)", "TotalPoints",
		"RunSec", "Gzip", "Sql", "Monitor"}
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
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "Series",
	}

	performances["车载batchsize变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-batchsize变化",
		Document:    "测试车载场景（1个tag，60个field），每个http携带数据量（batchsize）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "Batch Size",
	}

	performances["车载采样时间变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-采样时间变化",
		Document:    "测试车载场景（1个tag，60个field），采样时间（每个series两条数据timestamp间隔）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "采样时间",
	}

	performances["车载并发数变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-并发数变化",
		Document:    "测试车载场景（1个tag，60个field），并发数对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "并发数",
	}

	performances["车载Gzip变化"] = &fcbenchCaseDefine{
		Title:       "车载场景-写入性能-Gzip等级变化",
		Document:    "测试车载场景（1个tag，60个field），Gzip是否开启及等级对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间", "Gzip"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "Gzip",
	}

	performances["空气质量Series变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-Series变化",
		Document:    "测试空气质量（5个tag，8个field），series个数对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "Series",
	}

	performances["空气质量batchsize变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-batchsize变化",
		Document:    "测试空气质量（5个tag，8个field），每个http携带数据量（batchsize）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "Batch Size",
	}

	performances["空气质量采样时间变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-采样时间变化",
		Document:    "测试空气质量（5个tag，8个field），采样时间（每个series两条数据timestamp间隔）对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "采样时间",
	}

	performances["空气质量并发数变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-并发数变化",
		Document:    "测试空气质量（5个tag，8个field），并发数对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "并发数",
	}

	performances["空气质量Gzip变化"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-写入性能-Gzip等级变化",
		Document:    "测试空气质量（5个tag，8个field），Gzip是否开启对写入性能的影响",
		TableTags:   []string{"场景", "Series", "并发数", "Batch Size", "采样时间", "Gzip"},
		TableFeilds: []string{"写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "Gzip",
	}

	performances["空气质量查询性能"] = &fcbenchCaseDefine{
		Title:       "空气质量场景-查询性能",
		Document:    "测试空气质量（5个tag，8个field），不同查询语句的性能",
		TableTags:   []string{"场景", "并发数", "Sql"},
		TableFeilds: []string{"查询(query/s)"},
	}

	performances["车载查询性能"] = &fcbenchCaseDefine{
		Title:       "车载场景-查询性能",
		Document:    "测试车载场景（5个tag，8个field），不同查询语句的性能",
		TableTags:   []string{"场景", "并发数", "Sql"},
		TableFeilds: []string{"查询(query/s)"},
	}

	performances["空气质量混合比例"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定并发总数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定并发总数，变化混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(query/s)", "写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "查询百分比",
	}

	performances["空气质量混合方式1"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定写入并发数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定写入并发数24个，增加查询并发数来改变混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(query/s)", "写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "查询百分比",
	}

	performances["空气质量混合方式2"] = &fcbenchCaseDefine{
		Title: "空气质量场景-混合性能-固定查询并发数",
		Document: "测试空气质量（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.AirQuality.Types[1].RawSql + "\n" +
			"本用例固定查询并发数24个，增加写入并发数来改变混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(query/s)", "写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "查询百分比",
	}

	performances["车载混合比例"] = &fcbenchCaseDefine{
		Title: "车载场景-混合性能-固定并发总数",
		Document: "测试车载（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.Vehicle.Types[1].RawSql + "\n" +
			"本用例固定并发总数，变化混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(query/s)", "写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "查询百分比",
	}

	performances["车载混合方式1"] = &fcbenchCaseDefine{
		Title: "车载场景-混合性能-固定写入并发数",
		Document: "测试车载（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.Vehicle.Types[1].RawSql + "\n" +
			"本用例固定写入并发数24个，增加查询并发数来改变混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(query/s)", "写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "查询百分比",
	}

	performances["车载混合方式2"] = &fcbenchCaseDefine{
		Title: "车载场景-混合性能-固定查询并发数",
		Document: "测试车载（5个tag，8个field），查询和写入比例对性能的影响。\n" +
			"测试语句: " + query_generator.Vehicle.Types[1].RawSql + "\n" +
			"本用例固定查询并发数24个，增加写入并发数来改变混合比例。",
		TableTags:   []string{"场景", "Series", "并发数", "查询百分比", "Batch Size", "采样时间"},
		TableFeilds: []string{"查询(query/s)", "写入(point/s)"},
		PicType:     "line",
		PicXAxisTag: "查询百分比",
	}

}

func CreateReport(fileNames ...string) {

	// filesOrder := make([]string, 0)

	allCsvRecords := make(map[string][][]string)
	for _, fileName := range fileNames {
		csvFile, err := os.OpenFile(fileName+".csv", os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalln("open result csv failed, error:", err.Error())
		}
		csvReader := csv.NewReader(csvFile)
		allCsvRecords[fileName], err = csvReader.ReadAll()
		if err != nil {
			log.Fatalln("read csv error:", err.Error())
		}
	}

	report := reporter.NewPage("性能测试")
	report.Document = "测试海东青数据库的性能\n" + "使用工具：fcbench\n"
	// testcaseIndex := make(map[string]int)
	// testcaseContain := make([]*reporter.PerformanceTestCase, 0)
	var currentTestCase *reporter.PerformanceTestCase
	for rowIndex, row := range allCsvRecords[fileNames[0]] { //取第一个cse文件开始遍历
		if rowIndex == 0 { //跳过第一行
			continue
		}
		if caseDefine, ok := performances[row[0]]; ok {
			tableHeaders := caseDefine.TableTags
			if len(fileNames) > 1 {
				// 记录所有csv的相同field值
				for _, field := range caseDefine.TableFeilds {
					for _, fileName := range fileNames {
						tableHeaders = append(tableHeaders, field+": "+fileName)
					}
				}
				// 比较第一个csv和最后一个csv的差值
				for _, field := range caseDefine.TableFeilds {
					tableHeaders = append(tableHeaders, "比较"+field+": "+fileNames[0]+"与"+fileNames[len(fileNames)-1])
				}
			} else {
				tableHeaders = append(tableHeaders, caseDefine.TableFeilds...)
			}

			// 判断是否需要创建表格
			if report.HasTestCase(row[0]) {
				currentTestCase = report.GetTestCase(row[0]).(*reporter.PerformanceTestCase)
			} else {
				currentTestCase = reporter.NewPerformanceTestCase(row[0])
				currentTestCase.Title = caseDefine.Title
				currentTestCase.Document = caseDefine.Document
				currentTestCase.Table = reporter.CreateTable(tableHeaders...)
				report.AddTestCase(currentTestCase)
			}

			// 记录数据
			var rowData []interface{}
			if len(fileNames) > 1 {

				// 先记录tag
				for _, header := range caseDefine.TableTags {
					rowData = append(rowData, row[csvHeaderMap[header]])
				}
				// 记录所有csv的相同field值
				for _, field := range caseDefine.TableFeilds {
					for _, fileName := range fileNames {
						rowData = append(rowData, allCsvRecords[fileName][rowIndex][csvHeaderMap[field]])
					}
				}
				// 比较第一个csv和最后一个csv的差值
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
					rowData = append(rowData, fmt.Sprintf("%.2f%%", (newData-oldData)/oldData*100))
				}
			} else {
				for _, header := range tableHeaders {
					rowData = append(rowData, row[csvHeaderMap[header]])
				}
			}
			currentTestCase.Table.AddRows(rowData...)
		}
	}

	// 设置图片
	for _, testcase := range report.TestCases {
		performanceTestCase := testcase.(*reporter.PerformanceTestCase)
		if caseDefine, ok := performances[performanceTestCase.GetName()]; ok {
			switch caseDefine.PicType {
			case "line":
				line := reporter.NewLine(performanceTestCase.Title)
				line.SetXAxis(performanceTestCase.Table.GetColumn(caseDefine.PicXAxisTag))

				if len(fileNames) > 1 {
					for _, field := range caseDefine.TableFeilds {
						for _, fileName := range fileNames {
							line.AddSeries(field+":"+fileName, performanceTestCase.Table.GetColumn(field+": "+fileName))
						}
					}
				} else {
					for _, field := range caseDefine.TableFeilds {
						line.AddSeries(field, performanceTestCase.Table.GetColumn(field))
					}
				}
				performanceTestCase.Picture = line
			}
		}
	}
	f, _ := os.Create(fileNames[len(fileNames)-1] + ".html")
	defer f.Close()
	report.ToHtmlOneFile(f)
	// report.ToMarkDown(f)
}
