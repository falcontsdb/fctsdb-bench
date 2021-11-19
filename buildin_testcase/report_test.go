package buildin_testcase

import (
	"fmt"
	"testing"

	"github.com/shirou/gopsutil/host"
)

func TestReport(t *testing.T) {
	// CreateReport("v138", "v139")
	CreateReport("html", "v137", "v138")
}

func TestEnv(t *testing.T) {
	// memory, err := mem.VirtualMemory()
	// if err == nil {
	fmt.Println(host.Info())
	fmt.Printf("%.fG\n", 8.6)
	// fmt.Println(string(agent.GetEnv()))
	// }
}
