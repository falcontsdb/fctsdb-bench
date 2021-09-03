package bulk_query_fctsdb

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/bulk_data_gen/vehicle"
)

// basic
type VehicleBasicGenerator struct {
	sim   *vehicle.VehicleSimulator
	epoch string
}

func (g *VehicleBasicGenerator) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
}

func (g *VehicleBasicGenerator) loadEpochFromEnd(d time.Duration) {
	end := g.sim.TimestampEnd
	start := end.Add(d * -1)
	g.epoch = fmt.Sprintf("time >= '%s' and time < '%s'", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
}

func (g *VehicleBasicGenerator) Next() string {
	return ""
}

// case 1
type OneCarNewest struct {
	VehicleBasicGenerator
}

func (g *OneCarNewest) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	veh := g.sim.Hosts[index]
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
	g.perm = rand.Perm(len(g.sim.Hosts))
}

func (g *CarsNewest) Next() string {
	if g.count >= len(g.sim.Hosts) {
		log.Fatal("query cars more than the count of cars in database")
	}
	// perm := rand.Perm(len(g.sim.Vehicles))
	index := rand.Intn(len(g.sim.Hosts) - g.count)
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Write([]byte(`select * from vehicle where VIN in (`))
	for i := 0; i < g.count; i++ {
		buf.Write([]byte(`'`))
		buf.Write(g.sim.Hosts[g.perm[index+i]].Name)
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

func (g *CarPaging) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24)
}

func (g *CarPaging) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	veh := g.sim.Hosts[index]
	return fmt.Sprintf("select * from vehicle where VIN='%s' and %s order by time desc limit 100 offset 100;", veh.Name, g.epoch)
}

// case 4
type OneCarMessageCountMonth struct {
	VehicleBasicGenerator
}

func (g *OneCarMessageCountMonth) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24 * 30)
}

func (g *OneCarMessageCountMonth) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	veh := g.sim.Hosts[index]
	return fmt.Sprintf("select count(value1) from vehicle where VIN='%s' and %s;", veh.Name, g.epoch)
}

// case 5
type CarsMessageCountMonth struct {
	VehicleBasicGenerator
}

func (g *CarsMessageCountMonth) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24 * 30)
}

func (g *CarsMessageCountMonth) Next() string {
	return fmt.Sprintf("select count(value1) from vehicle where %s;", g.epoch)
}

// case 6
type CarsGroupMessageCountMonth struct {
	VehicleBasicGenerator
}

func (g *CarsGroupMessageCountMonth) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24 * 30)
}

func (g *CarsGroupMessageCountMonth) Next() string {
	return fmt.Sprintf("select count(value1) from vehicle where %s group by VIN;", g.epoch)
}

// case 7
type OneCarMessageCountYear struct {
	VehicleBasicGenerator
}

func (g *OneCarMessageCountYear) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24 * 365)
}

func (g *OneCarMessageCountYear) Next() string {
	index := rand.Intn(len(g.sim.Hosts))
	veh := g.sim.Hosts[index]
	return fmt.Sprintf("select count(value1) from vehicle where VIN='%s' and %s;", veh.Name, g.epoch)
}

// case 8
type CarsMessageCountYear struct {
	VehicleBasicGenerator
}

func (g *CarsMessageCountYear) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24 * 365)
}

func (g *CarsMessageCountYear) Next() string {
	return fmt.Sprintf("select count(value1) from vehicle where %s;", g.epoch)
}

// case 9
type CarsGroupMessageCountYear struct {
	VehicleBasicGenerator
}

func (g *CarsGroupMessageCountYear) Init(sim interface{}) {
	g.sim = sim.(*vehicle.VehicleSimulator)
	g.loadEpochFromEnd(time.Hour * 24 * 365)
}

func (g *CarsGroupMessageCountYear) Next() string {
	return fmt.Sprintf("select count(value1) from vehicle where %s group by VIN;", g.epoch)
}
