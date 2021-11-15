package query_generator

import (
	"fmt"
	"math/rand"
	"time"
	"unsafe"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/airq"
)

type AirqBasicGenerator struct {
	// basic struct
	sim   *airq.AirqSimulator
	epoch string
}

func (g *AirqBasicGenerator) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	return nil
}

func (g *AirqBasicGenerator) loadEpochFromEnd(d time.Duration) {
	end := g.sim.TimestampEnd
	start := end.Add(d * -1)
	g.epoch = fmt.Sprintf("time >= '%s' and time < '%s'", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
}

func (g *AirqBasicGenerator) Next() string {
	return ""
}

type airqFromOneSiteNewest struct {
	// case 1
	AirqBasicGenerator
}

func (g *airqFromOneSiteNewest) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select * from city_air_quality where site_id = '%s' order by time desc limit 1;", airq.SiteID)
}

type airqFromSitesNewest struct {
	// case 2
	AirqBasicGenerator
	count int // 需要查询的设备数
	// perm  []int
}

func (g *airqFromSitesNewest) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	// g.perm = rand.Perm(len(g.sim.Hosts))
	if g.count >= len(g.sim.Hosts) {
		return fmt.Errorf("site num the query needed is more than the count of sites in database")
	}
	shuffleAirqHost(g.sim.Hosts)
	return nil
}

func shuffleAirqHost(slice []airq.AirqDevice) {
	n := len(slice)
	for i := 0; i < len(slice); i++ {
		randIndex := rand.Intn(n)
		slice[i], slice[randIndex] = slice[randIndex], slice[i]
	}
}

func (g *airqFromSitesNewest) Next() string {

	index := rand.Intn(len(g.sim.Hosts))
	buf := bufPool.Get().([]byte)
	buf = append(buf, []byte(`select * from city_air_quality where site_id in (`)...)
	for i := 0; i < g.count; i++ {
		buf = append(buf, '\'')
		buf = append(buf, g.sim.Hosts[(index+i)%len(g.sim.Hosts)].SiteID...)
		buf = append(buf, '\'')
		if i != g.count-1 {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, []byte(") group by site_id order by time desc limit 1;")...)
	defer func() {
		buf = buf[:0]
		bufPool.Put(buf)
	}()
	return *(*string)(unsafe.Pointer(&buf))
}

type countOfDataFromOneSite struct {
	// case 3
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfDataFromOneSite) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *countOfDataFromOneSite) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select count(aqi) from city_air_quality where site_id = '%s' and %s", airq.SiteID, g.epoch)
}

type limitOffsetWithTimeOfOneSite struct {
	// case 4
	AirqBasicGenerator
	Period time.Duration
}

func (g *limitOffsetWithTimeOfOneSite) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

//todo
// make the offset-clause value changeable
func (g *limitOffsetWithTimeOfOneSite) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	g.sim.TimestampEnd.Add(g.Period * -1)
	return fmt.Sprintf("select * from city_air_quality where site_id = '%s' and %s order by time desc limit 100 offset 100", airq.SiteID, g.epoch)
}

type countOfData struct {
	// case 5
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfData) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *countOfData) Next() string {
	return fmt.Sprintf("select count(aqi) from city_air_quality where %s", g.epoch)
}

type countOfDataGroupByTag struct {
	// case 6
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfDataGroupByTag) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *countOfDataGroupByTag) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select count(aqi) from city_air_quality where city = '%s' and %s group by county", airq.City, g.epoch)
}

type countOfDataGroupByCity struct {
	// case 7
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfDataGroupByCity) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *countOfDataGroupByCity) Next() string {
	return fmt.Sprintf("select count(aqi) from city_air_quality where %s group by city", g.epoch)
}

type meanOfLastGroupBy struct {
	// case 8
	AirqBasicGenerator
}

func (g *meanOfLastGroupBy) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select mean(*) from (select last(*) from city_air_quality where city='%s' group by site_id)", airq.City)
}

type lastGroupBy struct {
	// case 9
	AirqBasicGenerator
}

func (g *lastGroupBy) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select last(*) from city_air_quality where city='%s' group by site_id", airq.City)
}

type meanOfOneSiteGroupByTime struct {
	// case 10
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *meanOfOneSiteGroupByTime) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *meanOfOneSiteGroupByTime) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select mean(*) from city_air_quality where site_id = '%s' and %s group by time(%s)", airq.SiteID, g.epoch, g.GroupByPeriod.String())
}

type meanOfOneCityGroupByTime struct {
	// case 11
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *meanOfOneCityGroupByTime) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *meanOfOneCityGroupByTime) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select mean(*) from city_air_quality where city = '%s' and %s group by time(%s)", airq.City, g.epoch, g.GroupByPeriod.String())
}

type topOfLastGroupBySite struct {
	// case 12
	AirqBasicGenerator
}

func (g *topOfLastGroupBySite) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select top(aqi, 100), site_id from (select last(aqi) as aqi from city_air_quality where city='%s' group by site_id)", airq.City)
}

type countOfMeanGroupBytime struct {
	// case 13
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *countOfMeanGroupBytime) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *countOfMeanGroupBytime) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select count(aqi) from (select mean(aqi) as aqi from city_air_quality where city = '%s' and site_id = '%s' and %s group by time(%s)) where aqi > 50", airq.City, airq.SiteID, g.epoch, g.GroupByPeriod.String())
}

type countOfMeanGroupBytime1 struct {
	// case 14
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *countOfMeanGroupBytime1) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *countOfMeanGroupBytime1) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select count(aqi) from (select mean(aqi) as aqi from city_air_quality where city = '%s' and %s group by time(%s)) where aqi > 50", airq.City, g.epoch, g.GroupByPeriod.String())
}

type topOfMeanGroupByCity struct {
	// case 15
	AirqBasicGenerator
	Period time.Duration
}

func (g *topOfMeanGroupByCity) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *topOfMeanGroupByCity) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	return fmt.Sprintf("select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where province='%s' and %s group by city)", airq.Province, g.epoch)
}

type topOfMeanGroupByCity1 struct {
	// case 16
	AirqBasicGenerator
	Period time.Duration
}

func (g *topOfMeanGroupByCity1) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.loadEpochFromEnd(g.Period)
	return nil
}

func (g *topOfMeanGroupByCity1) Next() string {
	return fmt.Sprintf("select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where %s group by city)", g.epoch)
}

type meanOfOneCityAndMonthGroupByDay struct {
	// case 17
	AirqBasicGenerator
	timeNow   time.Time
	timeIndex int
}

func (g *meanOfOneCityAndMonthGroupByDay) Init(sim common.Simulator) error {
	g.sim = sim.(*airq.AirqSimulator)
	g.timeNow = g.sim.TimestampStart
	g.timeIndex = 0
	return nil
}

func (g *meanOfOneCityAndMonthGroupByDay) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	airq := g.sim.Hosts[index]
	start := g.timeNow
	end := start.Add(time.Hour * 24 * 30)
	if end.After(g.sim.TimestampEnd) { // 如果超过结束时间，重头开始，本条因为不满30天，丢弃
		g.timeIndex = 0
		start = g.sim.TimestampStart
		end = start.Add(time.Hour * 24 * 30)
	}
	g.timeIndex++
	g.timeNow = end

	return fmt.Sprintf("select mean(aqi) as aqi from city_air_quality where city = '%s' and time > '%s' and time < '%s' group by time(1d)",
		airq.City, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
}
