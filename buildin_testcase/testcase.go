package buildin_testcase

import (
	"git.querycap.com/falcontsdb/fctsdb-bench/query_generator"
)

type BasicBenchTaskConfig struct {
	Group            string
	MixMode          string
	UseCase          string
	Workers          int
	BatchSize        int
	ScaleVar         int64
	SamplingInterval string
	TimeLimit        string
	UseGzip          int
	QueryPercent     int
	PrePareData      string
	NeedPrePare      bool
	Clean            bool
	SqlTemplate      []string
}

var (
	defaultTimeLimite       = "5m"
	defaultQueryPrePareData = "90d"
	defaultMixedPrePareData = "10m"

	// BuildinConfigs = []BasicBenchTaskConfig{buildinConfig_1, buildinConfig_2, buildinConfig_3, buildinConfig_4, buildinConfig_5, buildinConfig_6, buildinConfig_7, buildinConfig_8, buildinConfig_9, buildinConfig_10}
	// BuildinConfigs = []BasicBenchTaskConfig{BasicBenchTaskConfig{MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, QueryPercent: 50, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[i+1].RawSql}}}
	BuildinConfigs []BasicBenchTaskConfig
)

func init() {
	AddBuildinConfigs()
}

func AddBuildinConfigs() {
	// 纯写
	// Series 变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 1, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 1000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// batchsize 变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 10, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 100, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 5000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 采样时间变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "10s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "30s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 并发数变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载并发数变化", MixMode: "write_only", Workers: 8, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载并发数变化", MixMode: "write_only", Workers: 16, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载并发数变化", MixMode: "write_only", Workers: 32, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载并发数变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})

	// Gzip变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Gzip变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 0, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Gzip变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载Gzip变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 6, Clean: true})

	// Series 变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 1, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 1000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Series变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 100000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// batchsize 变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 10, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 100, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量batchsize变化", MixMode: "write_only", Workers: 64, BatchSize: 5000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 采样时间变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "10s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "30s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量采样时间变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// 并发数变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量并发数变化", MixMode: "write_only", Workers: 8, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量并发数变化", MixMode: "write_only", Workers: 16, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量并发数变化", MixMode: "write_only", Workers: 32, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量并发数变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	// gzip变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Gzip变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 0, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Gzip变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 1, Clean: true})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量Gzip变化", MixMode: "write_only", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, UseGzip: 6, Clean: true})

	// 先写数据， 第一个用例在开始前要清理所有数据和写入准备数据， NeedPrePare和Clean必须为ture，之后都不需要
	// needPrePareAndClean := true
	// for i := 0; i < query_generator.AirQuality.Count; i++ {
	// 	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询性能", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: needPrePareAndClean, UseGzip: 1, Clean: needPrePareAndClean, SqlTemplate: []string{query_generator.AirQuality.Types[i+1].RawSql}})
	// 	needPrePareAndClean = false // 不用再准备数据
	// }
	// 纯读 air-quality
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-Series变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: true, UseGzip: 1, Clean: true, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' order by time desc limit 1"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-Series变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id in ('{site_id*10}') order by time desc limit 1"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-Series变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id in ('{site_id*100}') order by time desc limit 1"}})
	// limit N变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-limit数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' order by time desc limit 1"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-limit数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' order by time desc limit 10"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-limit数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' order by time desc limit 100"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-limit数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' order by time desc limit 1000"}})
	// shard个数变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-shard数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-shard数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-15d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-shard数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-30d"}})
	// field个数变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-field数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-field数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi,pm10 from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-field数量变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi,pm10,pm25 from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	// 聚合函数
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-聚合函数", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-聚合函数", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select sum(aqi) from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-聚合函数", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select max(aqi) from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-聚合函数", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select first(aqi) from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-聚合函数", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select last(aqi) from city_air_quality where site_id = '{site_id}' and time > '{now}'-1d"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-聚合函数", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select max(aqi),pm10,pm25,o3,no2 from city_air_quality where time > '{now}'-1d"}})
	// window函数
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-window函数", MixMode: "read_only", Workers: 1, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select mean(aqi) from city_air_quality where time > '{now}'-1d group by window(time,2h,2h)"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-window函数", MixMode: "read_only", Workers: 1, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select mean(aqi) from city_air_quality where time > '{now}'-1d group by window(time,1h,2h)"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-window函数", MixMode: "read_only", Workers: 1, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select mean(aqi) from city_air_quality where time > '{now}'-1d group by window(time,4h,2h)"}})
	// 时间排序
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-时间排序", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality  order by time desc"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-时间排序", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality  order by time asc"}})
	// 并发数变化
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-并发数变化", MixMode: "read_only", Workers: 32, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-1h"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-并发数变化", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-1h"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-并发数变化", MixMode: "read_only", Workers: 96, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select aqi from city_air_quality where site_id = '{site_id}' and time > '{now}'-1h"}})
	// group by从句
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-group by从句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where province = '{province}' and time > '{now}'-1d group by city"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-group by从句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where city = '{city}' and time > '{now}'-1d group by county"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-group by从句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where county = '{county}' and time > '{now}'-1d group by site_id"}})
	// 嵌套语句
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-嵌套语句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select top(aqi, 100), site_id from (select last(aqi) as aqi from city_air_quality where city='{city}' group by site_id)"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-嵌套语句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where province='{province}' and time > '{now}'-1d group by city)"}})
	// slimit语句
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-slimit语句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where time > '{now}'-1d group by site_id slimit 1"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-slimit语句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where time > '{now}'-1d group by site_id slimit 10"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-slimit语句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where time > '{now}'-1d group by site_id slimit 100"}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量查询-slimit语句", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: false, UseGzip: 1, Clean: false, SqlTemplate: []string{"select count(aqi) from city_air_quality where time > '{now}'-1d group by site_id slimit 1000"}})

	// 纯读 vehicle
	// 先写数据， 第一个用例在开始前要清理所有数据和写入准备数据， NeedPrePare和Clean必须为ture，之后都不需要
	needPrePareAndClean := true
	for i := 0; i < query_generator.Vehicle.Count; i++ {
		BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载查询性能", MixMode: "read_only", Workers: 64, BatchSize: 1, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "60s", TimeLimit: defaultTimeLimite, PrePareData: defaultQueryPrePareData, NeedPrePare: needPrePareAndClean, UseGzip: 1, Clean: needPrePareAndClean, SqlTemplate: []string{query_generator.Vehicle.Types[i+1].RawSql}})
		needPrePareAndClean = false // 不用再准备数据
	}

	// 不同的混合比例
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})

	// 固定写入线程数
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式1", MixMode: "parallel", Workers: 30, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式1", MixMode: "parallel", Workers: 40, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式1", MixMode: "parallel", Workers: 60, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式1", MixMode: "parallel", Workers: 120, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})

	// 固定查询线程数
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式2", MixMode: "parallel", Workers: 30, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式2", MixMode: "parallel", Workers: 40, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式2", MixMode: "parallel", Workers: 60, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "空气质量混合方式2", MixMode: "parallel", Workers: 120, BatchSize: 1000, UseCase: "air-quality", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{query_generator.AirQuality.Types[1].RawSql}})

	// 不同的混合比例
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合比例", MixMode: "parallel", Workers: 64, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})

	// 固定写入线程数
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式1", MixMode: "parallel", Workers: 30, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式1", MixMode: "parallel", Workers: 40, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式1", MixMode: "parallel", Workers: 60, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式1", MixMode: "parallel", Workers: 120, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})

	// 固定查询线程数
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式2", MixMode: "parallel", Workers: 30, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 80, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式2", MixMode: "parallel", Workers: 40, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 60, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式2", MixMode: "parallel", Workers: 60, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 40, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
	BuildinConfigs = append(BuildinConfigs, BasicBenchTaskConfig{Group: "车载混合方式2", MixMode: "parallel", Workers: 120, BatchSize: 1000, UseCase: "vehicle", ScaleVar: 10000, SamplingInterval: "1s", TimeLimit: defaultTimeLimite, PrePareData: defaultMixedPrePareData, NeedPrePare: true, UseGzip: 1, QueryPercent: 20, Clean: true, SqlTemplate: []string{query_generator.Vehicle.Types[1].RawSql}})
}
