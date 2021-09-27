package main

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"
)

type RespStat struct {
	Lable  string
	Lat    int64
	IsPass bool
}

type RespTimeResult struct {
	P50    float64
	P90    float64
	P95    float64
	P99    float64
	Min    float64 //min response time (ms)
	Max    float64
	Avg    float64
	Fail   int
	Total  int
	RunSec float64
	Qps    float64
	Start  int
	End    int
}

type ResponseCollector struct {
	stats     []*RespStat
	startTime time.Time
	endTime   time.Time
	mutex     sync.Mutex
}

func (c *ResponseCollector) Add(r *RespStat) {
	c.mutex.Lock()
	c.stats = append(c.stats, r)
	c.mutex.Unlock()
}

func (c *ResponseCollector) AddOne(lable string, lat int64, isPass bool) {
	c.mutex.Lock()
	c.stats = append(c.stats, &RespStat{
		Lable:  lable,
		Lat:    lat,
		IsPass: isPass,
	})
	c.mutex.Unlock()
}

func (c *ResponseCollector) SetStart(t time.Time) {
	c.startTime = t
}

func (c *ResponseCollector) SetEnd(t time.Time) {
	c.endTime = t
}

func (c *ResponseCollector) GetDetail() RespTimeResult {
	respTimes := make([]int64, 0)
	successCount := 0
	for _, rtime := range c.stats {
		if rtime.IsPass { //判断响应是否成功
			successCount += 1
			respTimes = append(respTimes, rtime.Lat)
		}
	}

	if len(respTimes) > 0 {
		sort.Slice(respTimes, func(i, j int) bool { return respTimes[i] < respTimes[j] })
		qps := float64(successCount) / c.endTime.Sub(c.startTime).Seconds()
		runSec := c.endTime.Sub(c.startTime).Seconds()
		return RespTimeResult{
			P50:    Round(float64(respTimes[int(0.5*float64(successCount))])/1e6, 1),  //50%
			P90:    Round(float64(respTimes[int(0.9*float64(successCount))])/1e6, 1),  //90%
			P95:    Round(float64(respTimes[int(0.95*float64(successCount))])/1e6, 1), //95%
			P99:    Round(float64(respTimes[int(0.99*float64(successCount))])/1e6, 1), //99%
			Min:    Round(float64(respTimes[0])/1e6, 1),                               //MIN
			Max:    Round(float64(respTimes[successCount-1])/1e6, 1),                  //MAX
			Avg:    Round(float64(AvgInt64(respTimes))/1e6, 1),
			Fail:   len(c.stats) - successCount,
			Total:  len(c.stats),
			Qps:    Round(qps, 3),
			RunSec: Round(runSec, 3),
			Start:  int(c.startTime.Unix()),
			End:    int(c.endTime.Unix()),
		}
	}
	return RespTimeResult{}
}

func Round(f float64, bit int) float64 {
	v, _ := strconv.ParseFloat(fmt.Sprintf("%."+strconv.Itoa(bit)+"f", f), 64)
	return v
}

func stringComplement(src string, bit int, sep string) string {
	sepb := []byte(sep)
	srcb := []byte(src)
	for i := 0; i < bit; i++ {
		srcb = append(srcb, sepb...)
	}
	return string(srcb)
}

func (r RespTimeResult) Show() {
	// 利用反射
	t := reflect.TypeOf(r)
	v := reflect.ValueOf(r)
	keys := make([]string, t.NumField())
	values := make([]string, t.NumField())

	// 下面3步核心思想是，取key和value两个的长度，key更长，就将value长度补位空字符和key一样长，value更长，就补位key
	// 最终需要得到以下格式：
	// P50(ms) P90(ms) P95(ms) P99(ms) Min(ms) Max(ms) Avg(ms) Qps Fail Total RunSec(s)
	// 676     826     837     867     60      869     608     281 0    864   3.072
	//
	// 第1步：先遍历一变，按照key的长度，格式化value并记录下来
	for k := 0; k < t.NumField(); k++ {
		var key string
		// 合入单位
		if t.Field(k).Name == "Qps" || t.Field(k).Name == "Fail" || t.Field(k).Name == "Total" || t.Field(k).Name == "Start" || t.Field(k).Name == "End" {
			key = fmt.Sprintf("%v ", t.Field(k).Name)
		} else if t.Field(k).Name == "RunSec" {
			key = fmt.Sprintf("%v(s) ", t.Field(k).Name)
		} else {
			key = fmt.Sprintf("%v(ms) ", t.Field(k).Name)
		}

		keys[k] = key

		// key长度大于value，将value补位；key长度小于value，则保持value
		value := fmt.Sprintf("%v", v.Field(k).Interface())
		if len(key) > len(value) {
			values[k] = stringComplement(fmt.Sprintf("%v", value), len(key)-len(value), " ")
		} else {
			values[k] = value + " "
		}
	}

	// 第2步：按value长度打印key，不足补位空字符串
	for k := 0; k < t.NumField(); k++ {
		fmt.Print(stringComplement(keys[k], len(values[k])-len(keys[k]), " "))
	}
	fmt.Print("\n")

	// 第3步：输出value
	for k := 0; k < t.NumField(); k++ {
		fmt.Print(values[k], "")
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
		if t.Field(k).Name == "Qps" || t.Field(k).Name == "Fail" || t.Field(k).Name == "Total" || t.Field(k).Name == "Start" || t.Field(k).Name == "End" {
			key = fmt.Sprintf("%v", t.Field(k).Name)
		} else if t.Field(k).Name == "RunSec" {
			key = fmt.Sprintf("%v(s)", t.Field(k).Name)
		} else {
			key = fmt.Sprintf("%v(ms)", t.Field(k).Name)
		}
		value := fmt.Sprintf("%v", v.Field(k).Interface())
		m[key] = value
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
