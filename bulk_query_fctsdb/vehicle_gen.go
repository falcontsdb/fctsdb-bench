package bulk_query_fctsdb

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
)

// basic
type VehicleBasicGenerator struct {
	sim *vehicle.VehicleSimulator
}

func (g *VehicleBasicGenerator) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
}

func (g *VehicleBasicGenerator) Next() string {
	return ""
}

// case 1
type OneCarNewest struct {
	VehicleBasicGenerator
}

func (g *OneCarNewest) Next() string {
	index := rand.Intn(len(g.sim.Vehicles))
	veh := g.sim.Vehicles[index]
	return fmt.Sprintf("select * from vehicle where VIN='%s' order by time desc limit 1;", veh.Name)
}

// case 2
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

// case 3
type CarPaging struct {
	VehicleBasicGenerator
}

func (g *CarPaging) Next() string {
	index := rand.Intn(len(g.sim.Vehicles))
	veh := g.sim.Vehicles[index]
	return fmt.Sprintf("select * from vehicle where VIN='%s' and time > now()-1d order by time desc limit 100 offset 100;", veh.Name)
}

// case 4
type OneCarMessageCountMonth struct {
	VehicleBasicGenerator
}

func (g *OneCarMessageCountMonth) Next() string {
	index := rand.Intn(len(g.sim.Vehicles))
	veh := g.sim.Vehicles[index]
	return fmt.Sprintf("select count(value1) from vehicle where VIN='%s' and time > now()-30d;", veh.Name)
}

// case 5
type CarsMessageCountMonth struct {
	VehicleBasicGenerator
}

func (g *CarsMessageCountMonth) Next() string {
	return "select count(value1) from vehicle where time > now()-30d;"
}

// case 6
type CarsGroupMessageCountMonth struct {
	VehicleBasicGenerator
}

func (g *CarsGroupMessageCountMonth) Next() string {
	return "select count(value1) from vehicle where time > now()-30d group by VIN;"
}

// case 7
type OneCarMessageCountYear struct {
	VehicleBasicGenerator
}

func (g *OneCarMessageCountYear) Next() string {
	index := rand.Intn(len(g.sim.Vehicles))
	veh := g.sim.Vehicles[index]
	return fmt.Sprintf("select count(value1) from vehicle where VIN='%s' and time > now()-1y;", veh.Name)
}

// case 8
type CarsMessageCountYear struct {
	VehicleBasicGenerator
}

func (g *CarsMessageCountYear) Next() string {
	return "select count(value1) from vehicle where time > now()-1y;"
}

// case 9
type CarsGroupMessageCountYear struct {
	VehicleBasicGenerator
}

func (g *CarsGroupMessageCountYear) Next() string {
	return "select count(value1) from vehicle where time > now()-1y group by VIN;"
}
