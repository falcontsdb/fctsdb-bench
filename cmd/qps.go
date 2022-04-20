package main

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type RespState struct {
	Label  string
	Lat    int64
	IsPass bool
}

type RespTimeResult struct {
	Label  string
	P50    float64
	P90    float64
	P95    float64
	P99    float64
	Min    float64 // min response time (ms)
	Max    float64
	Avg    float64
	Fail   int     // 失败数
	Total  int     // 总数
	RunSec float64 // 运行时间（秒）
	Qps    float64
	Start  time.Time // 开始时间
	End    time.Time // 结束时间
}

type GroupResult []RespTimeResult

type ResultCollector struct {
	states    []*RespState
	startTime time.Time
	endTime   time.Time
	mutex     sync.Mutex
	values    int64
	points    int64
	bytes     int64
	queries   int64
}

func NewResponseCollector() *ResultCollector {
	return &ResultCollector{
		states:  make([]*RespState, 0),
		values:  0,
		points:  0,
		bytes:   0,
		queries: 0,
	}
}

func (c *ResultCollector) Add(r *RespState) {
	c.mutex.Lock()
	c.states = append(c.states, r)
	c.mutex.Unlock()
}

func (c *ResultCollector) AddOneResponTime(label string, lat int64, isPass bool) {
	c.mutex.Lock()
	c.states = append(c.states, &RespState{
		Label:  label,
		Lat:    lat,
		IsPass: isPass,
	})
	c.mutex.Unlock()
}

func (c *ResultCollector) AddValues(count int64) {
	atomic.AddInt64(&c.values, count)
}

func (c *ResultCollector) AddPoints(count int64) {
	atomic.AddInt64(&c.points, count)
}

func (c *ResultCollector) AddBytes(count int64) {
	atomic.AddInt64(&c.bytes, count)
}

func (c *ResultCollector) AddQueries(count int64) {
	atomic.AddInt64(&c.queries, count)
}

func (c *ResultCollector) GetValues() int64 {
	return atomic.LoadInt64(&c.values)
}

func (c *ResultCollector) GetPoints() int64 {
	return atomic.LoadInt64(&c.points)
}

func (c *ResultCollector) GetBytes() int64 {
	return atomic.LoadInt64(&c.bytes)
}

func (c *ResultCollector) GetQueries() int64 {
	return atomic.LoadInt64(&c.queries)
}

func (c *ResultCollector) Reset() {
	c.values = 0
	c.points = 0
	c.bytes = 0
	c.queries = 0
	c.states = c.states[:0]
	c.startTime = time.Time{}
	c.endTime = time.Time{}
}

func (c *ResultCollector) SetStartTime(t time.Time) {
	c.startTime = t
}

func (c *ResultCollector) SetEndTime(t time.Time) {
	c.endTime = t
}

func (c *ResultCollector) GetDetail() RespTimeResult {
	respTimes := make([]int64, 0)
	successCount := 0
	for _, state := range c.states {
		if state.IsPass { //判断响应是否成功
			successCount += 1
			respTimes = append(respTimes, state.Lat)
		}
	}

	if len(respTimes) > 0 {
		sort.Slice(respTimes, func(i, j int) bool { return respTimes[i] < respTimes[j] })
		qps := float64(successCount) / c.endTime.Sub(c.startTime).Seconds()
		runSec := c.endTime.Sub(c.startTime).Seconds()
		return RespTimeResult{
			Label:  "total",
			P50:    Round(float64(respTimes[int(0.5*float64(successCount))])/1e6, 1),  //50%
			P90:    Round(float64(respTimes[int(0.9*float64(successCount))])/1e6, 1),  //90%
			P95:    Round(float64(respTimes[int(0.95*float64(successCount))])/1e6, 1), //95%
			P99:    Round(float64(respTimes[int(0.99*float64(successCount))])/1e6, 1), //99%
			Min:    Round(float64(respTimes[0])/1e6, 1),                               //MIN
			Max:    Round(float64(respTimes[successCount-1])/1e6, 1),                  //MAX
			Avg:    Round(float64(AvgInt64(respTimes))/1e6, 1),
			Fail:   len(c.states) - successCount,
			Total:  len(c.states),
			Qps:    Round(qps, 3),
			RunSec: Round(runSec, 3),
			Start:  c.startTime,
			End:    c.endTime,
		}
	}
	return RespTimeResult{}
}

func (c *ResultCollector) GetGroupDetail() (gr GroupResult) {

	groupRespTime := make(map[string][]int64) // 按标签分组保存成功的响应时间{"label": [rt1, rt2, rt3]}
	groupCount := make(map[string]int)        // 按标签分组保存总的请求数
	for _, state := range c.states {
		if _, ok := groupRespTime[state.Label]; !ok {
			groupRespTime[state.Label] = make([]int64, 0)
			groupCount[state.Label] = 0
		}
		if state.IsPass {
			groupRespTime[state.Label] = append(groupRespTime[state.Label], state.Lat)
		}
		groupCount[state.Label] += 1
	}

	for label, respTimes := range groupRespTime {
		var r RespTimeResult
		runSec := c.endTime.Sub(c.startTime).Seconds()
		r.Label = label
		r.Fail = groupCount[label] - len(respTimes)
		r.Total = groupCount[label]
		r.RunSec = Round(runSec, 3)
		r.Start = c.startTime
		r.End = c.endTime
		if len(respTimes) > 0 {
			sort.Slice(respTimes, func(i, j int) bool { return respTimes[i] < respTimes[j] })
			qps := float64(len(respTimes)) / c.endTime.Sub(c.startTime).Seconds()

			r.P50 = Round(float64(respTimes[int(0.5*float64(len(respTimes)))])/1e6, 1)  //50%
			r.P90 = Round(float64(respTimes[int(0.9*float64(len(respTimes)))])/1e6, 1)  //90%
			r.P95 = Round(float64(respTimes[int(0.95*float64(len(respTimes)))])/1e6, 1) //95%
			r.P99 = Round(float64(respTimes[int(0.99*float64(len(respTimes)))])/1e6, 1) //99%
			r.Min = Round(float64(respTimes[0])/1e6, 1)                                 //MIN
			r.Max = Round(float64(respTimes[len(respTimes)-1])/1e6, 1)                  //MAX
			r.Avg = Round(float64(AvgInt64(respTimes))/1e6, 1)
			r.Qps = Round(qps, 3)
		}
		gr = append(gr, r)
	}
	return
}

func Round(f float64, bit int) float64 {
	v, _ := strconv.ParseFloat(fmt.Sprintf("%."+strconv.Itoa(bit)+"f", f), 64)
	return v
}

func (r RespTimeResult) Show() {
	// 利用反射
	t := reflect.TypeOf(r)
	v := reflect.ValueOf(r)
	keys := make([]string, t.NumField())
	values := make([]string, t.NumField())
	maxLengths := make([]int, t.NumField())
	// 下面3步核心思想是，取key和value两个的长度，key更长，就将value长度补位空字符和key一样长，value更长，就补位key
	// 最终需要得到以下格式：
	// P50(ms) P90(ms) P95(ms) P99(ms) Min(ms) Max(ms) Avg(ms) Qps Fail Total RunSec(s)
	// 676     826     837     867     60      869     608     281 0    864   3.072
	//
	// 第1步：先遍历一变，按照key的长度，格式化value并记录下来
	for k := 0; k < t.NumField(); k++ {
		// 合入单位
		switch t.Field(k).Name {
		case "Qps", "Fail", "Total", "Label":
			keys[k] = fmt.Sprintf("%v", t.Field(k).Name)
		case "RunSec":
			keys[k] = fmt.Sprintf("%v(s)", t.Field(k).Name)
		case "Start", "End":
			continue
		default:
			keys[k] = fmt.Sprintf("%v(ms)", t.Field(k).Name)
		}

		values[k] = fmt.Sprintf("%v", v.Field(k).Interface())
		maxLengths[k] = max(len(keys[k]), len(values[k]))
	}

	// 第2步：按value长度打印key，不足补位空字符串
	for k := 0; k < len(keys); k++ {
		fmt.Print(left(keys[k], maxLengths[k]+1, " "))
	}
	fmt.Print("\n")

	// 第3步：输出value
	for k := 0; k < len(keys); k++ {
		fmt.Print(left(values[k], maxLengths[k]+1, " "))
	}
	fmt.Print("\n")
}

func (r RespTimeResult) ToMap() map[string]string {
	// 利用反射
	t := reflect.TypeOf(r)
	v := reflect.ValueOf(r)

	m := make(map[string]string)
	for k := 0; k < t.NumField(); k++ {
		var key string
		// 合入单位
		switch t.Field(k).Name {
		case "Qps", "Fail", "Total", "Start", "End":
			key = fmt.Sprintf("%v", t.Field(k).Name)
		case "RunSec":
			key = fmt.Sprintf("%v(s)", t.Field(k).Name)
		default:
			key = fmt.Sprintf("%v(ms)", t.Field(k).Name)
		}
		value := fmt.Sprintf("%v", v.Field(k).Interface())
		m[key] = value
	}
	return m
}

func (g GroupResult) Show() {
	// 利用反射
	var keys []string
	var groupValues [][]string
	var maxLengths []int
	for i, r := range g {
		t := reflect.TypeOf(r)
		v := reflect.ValueOf(r)
		values := make([]string, t.NumField())
		// 下面3步核心思想是，取key和value两个的长度，key更长，就将value长度补位空字符和key一样长，value更长，就补位key
		// 最终需要得到以下格式：
		// P50(ms) P90(ms) P95(ms) P99(ms) Min(ms) Max(ms) Avg(ms) Qps Fail Total RunSec(s)
		// 676     826     837     867     60      869     608     281 0    864   3.072
		//
		// 第1步：先遍历一变，按照key的长度，格式化value并记录下来
		if i == 0 {
			keys = make([]string, t.NumField())
			maxLengths = make([]int, t.NumField())
			for k := 0; k < t.NumField(); k++ {
				// 合入单位
				switch t.Field(k).Name {
				case "Qps", "Fail", "Total", "Label":
					keys[k] = fmt.Sprintf("%v", t.Field(k).Name)
				case "RunSec":
					keys[k] = fmt.Sprintf("%v(s)", t.Field(k).Name)
				case "Start", "End":
					continue
					// keys[k] = fmt.Sprintf("%v ", t.Field(k).Name)
				default:
					keys[k] = fmt.Sprintf("%v(ms)", t.Field(k).Name)
				}

				// key长度大于value，将value补位；key长度小于value，则保持value
				values[k] = fmt.Sprintf("%v", v.Field(k).Interface())
				maxLengths[k] = max(len(keys[k]), len(values[k]))
			}
		} else {
			for k := 0; k < t.NumField(); k++ {
				switch t.Field(k).Name {
				case "Start", "End":
					continue
				}
				// key长度大于value，将value补位；key长度小于value，则保持value
				values[k] = fmt.Sprintf("%v", v.Field(k).Interface())
				maxLengths[k] = max(maxLengths[k], len(values[k]))
			}
		}
		groupValues = append(groupValues, values)
	}

	// 第2步：按max length长度打印key，不足补位空字符串
	for k := 0; k < len(keys); k++ {
		fmt.Print(left(keys[k], maxLengths[k]+1, " "))
	}
	fmt.Print("\n")

	// 第3步：输出value
	for _, values := range groupValues {
		for k := 0; k < len(keys); k++ {
			fmt.Print(left(values[k], maxLengths[k]+1, " "))
		}
		fmt.Print("\n")
	}
}

func (g GroupResult) ToMap() map[string]string {
	// 利用反射
	m := make(map[string]string)
	for _, r := range g {
		t := reflect.TypeOf(r)
		v := reflect.ValueOf(r)
		label := r.Label
		if label == "query" {
			label = "r"
		}
		if label == "write" {
			label = "w"
		}
		for k := 0; k < t.NumField(); k++ {
			switch t.Field(k).Name {
			case "RunSec":
				key := fmt.Sprintf("%v", t.Field(k).Name)
				value := fmt.Sprintf("%v", v.Field(k).Interface())
				m[key] = value
			case "Start":
				key := "Start"
				value := r.Start.UTC().Format(time.RFC3339)
				m[key] = value
			case "End":
				key := "End"
				value := r.End.UTC().Format(time.RFC3339)
				m[key] = value
			case "Label":
			default:
				key := fmt.Sprintf("%v(%v)", t.Field(k).Name, label)
				value := fmt.Sprintf("%v", v.Field(k).Interface())
				m[key] = value
			}
		}
	}
	return m
}

func AvgInt64(list []int64) int64 {
	var total int64 = 0
	var highWord int64 = 0 //超出int64范围的计数
	for _, v := range list {
		if math.MaxInt64-total <= v { //超出int64最大范围时的情况
			highWord += 1
			e := (total - math.MaxInt64) //先减后加，避免溢出
			total = e + v
		} else {
			total += v
		}
	}
	var avg int64 = 0
	var count int64 = int64(len(list))
	for i := int64(0); i < highWord; i++ {
		if math.MaxInt64-avg <= math.MaxInt64/count { //如果结果溢出，则表示无法计数
			log.Fatal("compute average int64 overload")
		}
		avg += math.MaxInt64 / count
	}
	avg += total / count
	return avg
}

func left(word string, length int, fillchar string) string {
	words := word
	for i := 0; i < length-len(word); i++ {
		words += fillchar
	}
	return words
}

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}
