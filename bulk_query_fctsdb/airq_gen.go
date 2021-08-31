package bulk_query_fctsdb

import (
	"fmt"
	"math/rand"

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

type OneDeviceNewest struct {
	AirqBasicGenerator
}

func (g *OneDeviceNewest) Next() string {
	index := rand.Intn(len(g.sim.Airqs))
	airq := g.sim.Airqs[index]
	return fmt.Sprintf("select * from city_air_quality where site_id='%s' order by time desc limit 1", airq.SiteID)
}
