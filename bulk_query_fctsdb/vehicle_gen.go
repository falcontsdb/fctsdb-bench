package bulk_query_fctsdb

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	},
}

type VehicleBasicGenerator struct {
	sim *vehicle.VehicleSimulator
}

func (g *VehicleBasicGenerator) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
}

func (g *VehicleBasicGenerator) Next() string {
	return ""
}

type OneCarNewest struct {
	VehicleBasicGenerator
}

func (g *OneCarNewest) Next() string {
	index := rand.Intn(len(g.sim.Vehicles))
	veh := g.sim.Vehicles[index]
	return fmt.Sprintf("select * from vehicle where VIN='%s' order by time desc limit 1;", veh.Name)
}

type CarsNewest struct {
	VehicleBasicGenerator
	count int
	perm  []int
}

func (g *CarsNewest) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.perm = rand.Perm(len(g.sim.Vehicles))
}

func (g *CarsNewest) Next() string {
	if g.count > len(g.sim.Vehicles) {
		log.Fatal("query cars more than the count of cars in database")
	}
	// perm := rand.Perm(len(g.sim.Vehicles))
	index := rand.Intn(len(g.sim.Vehicles) - g.count)
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Write([]byte(`select * from vehicle where VIN in (`))
	for i := 0; i < g.count; i++ {
		buf.Write([]byte(`'`))
		buf.Write(g.sim.Vehicles[g.perm[index+i]].Name)
		buf.Write([]byte(`'`))
		if i != g.count-1 {
			buf.Write([]byte(`,`))
		}
	}
	buf.Write([]byte(") group by VIN order by time desc limit 1;"))
	sql := buf.String()
	buf.Reset()
	bufPool.Put(buf)
	return sql
}

type CarsPaging struct {
	VehicleBasicGenerator
}

func (g *CarsPaging) Next() string {
	index := rand.Intn(len(g.sim.Vehicles))
	veh := g.sim.Vehicles[index]
	return fmt.Sprintf("select * from vehicle where VIN='%s' and time > now()-1d order by time desc limit 100 offset 100;", veh.Name)
}
