package buildin_testcase

import (
	"fmt"
	"os"
	"testing"

	"github.com/shirou/gopsutil/host"
)

func TestReport(t *testing.T) {
	// CreateReport("v138", "v139")
	report := CreateReport("v137", "v138")
	f, _ := os.Create("v138.html")
	defer f.Close()
	report.ToHtmlOneFile(f)
}

func TestEnv(t *testing.T) {
	// memory, err := mem.VirtualMemory()
	// if err == nil {
	fmt.Println(host.Info())
	fmt.Printf("%.fG\n", 8.6)
	// fmt.Println(string(agent.GetEnv()))
	// }
}
