package bulk_query_fctsdb

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/airq"
)

type AirqBasicGenerator struct {
	sim *airq.AirqSimulator
}

func (g *AirqBasicGenerator) Init(sim interface{}) {
	g.sim = sim.(*airq.AirqSimulator)
}

func (g *AirqBasicGenerator) Next() string {
	return ""
}

type airqFromOneSiteNewest struct {
	AirqBasicGenerator
}

func (g *airqFromOneSiteNewest) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select * from city_air_quality where site_id = '%s'  order by time desc limit 1", airq.SiteID)
}

type airqFromSitesNewest struct {
	AirqBasicGenerator
	count int
	perm  []int
}

func (g *airqFromSitesNewest) Init(sim interface{}) {
	g.sim = sim.(*airq.AirqSimulator)
	g.perm = rand.Perm(len(g.sim.Airqs))
}

func (g *airqFromSitesNewest) Next() string {
	if g.count > len(g.sim.Airqs) {
		log.Fatal("site num the query needed is more than the count of sites in database")
	}
	index := rand.Intn(len(g.sim.Airqs) - g.count)
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Write([]byte(`select * from city_air_quality where site_id in (`))
	for i := 0; i < g.count; i++ {
		buf.Write([]byte(`'`))
		buf.Write(g.sim.Airqs[g.perm[index+i]].SiteID)
		buf.Write([]byte(`'`))
		if i != g.count-1 {
			buf.Write([]byte(`,`))
		}
	}
	buf.Write([]byte(") group by site_id order by time desc limit 1;"))
	sql := buf.String()
	buf.Reset()
	bufPool.Put(buf)
	return sql
}

type countOfDataFromOneSite struct {
	AirqBasicGenerator
	Period time.Duration
}

//todo
// change the Now() in query to the endTime of dataSet
func (g *countOfDataFromOneSite) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select count(aqi) from city_air_quality where site_id = '%s' and time > now()-%s", airq.SiteID, g.Period.String())
}

type limitOffsetWithTimeOfOneSite struct {
	AirqBasicGenerator
	Period time.Duration
}

//todo
// make the offset-clause value changeable
func (g *limitOffsetWithTimeOfOneSite) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select * from city_air_quality where device_id = '%s' and time > now()-%s order by time desc limit 100 offset 100", airq.SiteID, g.Period.String())
}

type countOfData struct {
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfData) Next() string {
	return fmt.Sprintf("select count(aqi) from city_air_quality where time > now()-%s", g.Period.String())
}

type countOfDataGroupByTag struct {
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfDataGroupByTag) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select count(aqi) from city_air_quality where city = '%s' and time > now()-%s group by county", airq.City, g.Period.String())
}

type countOfDataGroupByCity struct {
	AirqBasicGenerator
	Period time.Duration
}

func (g *countOfDataGroupByCity) Next() string {
	return fmt.Sprintf("select count(aqi) from city_air_quality where time > now()-%s group by city", g.Period.String())
}

type meanOfLastGroupBy struct {
	AirqBasicGenerator
}

func (g *meanOfLastGroupBy) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select mean(*) from (select last(*) from city_air_quality where city='%s' group by site_id)", airq.City)
}

type lastGroupBy struct {
	AirqBasicGenerator
}

func (g *lastGroupBy) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select last(*) from city_air_quality where city='%s' group by site_id", airq.City)
}

type meanOfOneSiteGroupByTime struct {
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *meanOfOneSiteGroupByTime) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select mean(*) from city_air_quality where site_id = '%s' and time > now()-%s group by time(%s)", airq.SiteID, g.Period.String(), g.GroupByPeriod.String())
}

type meanOfOneCityGroupByTime struct {
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *meanOfOneCityGroupByTime) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select mean(*) from city_air_quality where city = '%s' and time > now()-%s group by time(%s)", airq.City, g.Period.String(), g.GroupByPeriod.String())
}

type topOfLastGroupBySite struct {
	AirqBasicGenerator
}

func (g *topOfLastGroupBySite) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select top(aqi, 100), site_id from (select last(aqi) as aqi from city_air_quality where city='%s' group by site_id)", airq.City)
}

type countOfMeanGroupBytime struct {
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *countOfMeanGroupBytime) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select count(aqi) from (select mean(aqi) as aqi from city_air_quality where city = '%s' and site_id = '%s' and time > now()-%s group by time(%s)) where aqi > 50", airq.City, airq.SiteID, g.Period, g.GroupByPeriod)
}

type countOfMeanGroupBytime1 struct {
	AirqBasicGenerator
	Period        time.Duration
	GroupByPeriod time.Duration
}

func (g *countOfMeanGroupBytime1) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select count(aqi) from (select mean(aqi) as aqi from city_air_quality where city = '%s' and time > now()-%s group by time(%s)) where aqi > 50", airq.City, g.Period, g.GroupByPeriod)
}

type topOfMeanGroupByCity struct {
	AirqBasicGenerator
	Period time.Duration
}

func (g *topOfMeanGroupByCity) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where province='%s' and time > now()-%s group by city)", airq.Province, g.Period)
}

type topOfMeanGroupByCity1 struct {
	AirqBasicGenerator
	Period time.Duration
}

func (g *topOfMeanGroupByCity1) Next() string {
	return fmt.Sprintf("select top(aqi, 100) as aqi, city from (select mean(aqi) as aqi from city_air_quality where time > now()-%s group by city)", g.Period)
}
