package main

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"time"
)

type RespStat struct {
	Lable  string
	Lat    int64
	IsPass bool
}

type RespTimeResult struct {
	Min     int //min response time (ms)
	Max     int
	Avg     int
	P50     int
	P90     int
	P95     int
	P99     int
	Qps     float64
	Success int64
	Count   int64
}

type ResponseCollector struct {
	stats     []*RespStat
	startTime time.Time
	endTime   time.Time
}

func (c *ResponseCollector) Add(r *RespStat) {
	c.stats = append(c.stats, r)
}

func (c *ResponseCollector) AddOne(lable string, lat int64, isPass bool) {
	c.stats = append(c.stats, &RespStat{
		Lable:  lable,
		Lat:    lat,
		IsPass: isPass,
	})
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
		return RespTimeResult{
			P50:     int(respTimes[int(0.5*float64(successCount))] / 1e6),  //50%
			P90:     int(respTimes[int(0.9*float64(successCount))] / 1e6),  //90%
			P95:     int(respTimes[int(0.95*float64(successCount))] / 1e6), //95%
			P99:     int(respTimes[int(0.99*float64(successCount))] / 1e6), //99%
			Min:     int(respTimes[0]) / 1e6,                               //MIN
			Max:     int(respTimes[successCount-1]) / 1e6,                  //MAX
			Avg:     int(AvgInt64(respTimes)) / 1e6,
			Success: int64(successCount),
			Count:   int64(len(c.stats)),
			Qps:     float64(successCount) / float64(c.endTime.Sub(c.startTime).Seconds()),
		}
	}
	return RespTimeResult{}
}

func (c *ResponseCollector) ShowDetail() {
	r := c.GetDetail()
	t := reflect.TypeOf(r)
	v := reflect.ValueOf(r)
	for k := 0; k < t.NumField(); k++ {
		if t.Field(k).Name == "Qps" || t.Field(k).Name == "Success" || t.Field(k).Name == "Count" {
			fmt.Printf("%s -- %v \n", t.Field(k).Name, v.Field(k).Interface())
		} else {
			fmt.Printf("%s -- %v ms\n", t.Field(k).Name, v.Field(k).Interface())
		}
	}
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
