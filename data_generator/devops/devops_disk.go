package devops

import (
	"fmt"
	"sync/atomic"
	"time"

	. "git.querycap.com/falcontsdb/fctsdb-bench/common"
	"git.querycap.com/falcontsdb/fctsdb-bench/util/fastrand"
)

const OneTerabyte int64 = 1 << 40

var (
	DiskByteString        = []byte("disk") // heap optimization
	TotalByteString       = []byte("total")
	FreeByteString        = []byte("free")
	UsedByteString        = []byte("used")
	UsedPercentByteString = []byte("used_percent")
	INodesTotalByteString = []byte("inodes_total")
	INodesFreeByteString  = []byte("inodes_free")
	INodesUsedByteString  = []byte("inodes_used")

	DiskTags = [][]byte{
		[]byte("path"),
		[]byte("fstype"),
	}
	DiskFSTypeChoices = [][]byte{
		[]byte("ext3"),
		[]byte("ext4"),
		[]byte("btrfs"),
	}
)

type DiskMeasurement struct {
	timestamp time.Time

	path, fsType []byte
	// uptime       time.Duration
	// freeBytesDist Distribution
	free int64
}

func NewDiskMeasurement(start time.Time, sda int) *DiskMeasurement {
	if sda == 0 {
		sda = int(fastrand.Uint32n(10))
	}
	path := []byte(fmt.Sprintf("/dev/sda%d", sda))
	fsType := DiskFSTypeChoices[fastrand.Uint32n(uint32(len(DiskFSTypeChoices)))]
	if Config != nil { // partial override from external config
		path = Config.GetTagBytesValue(DiskByteString, DiskTags[0], true, path)
		fsType = Config.GetTagBytesValue(DiskByteString, DiskTags[1], true, fsType)
	}
	return &DiskMeasurement{
		path:   path,
		fsType: fsType,

		timestamp: start,
		// freeBytesDist: CWD(ND(50, 1), 0, OneTerabyte, OneTerabyte/2),
		free: OneTerabyte / 2,
	}
}

func (m *DiskMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	// m.freeBytesDist.Advance()
}

func (m *DiskMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(DiskByteString)
	// p.SetTimestamp(&m.timestamp)

	p.AppendTag(DiskTags[0], m.path)
	p.AppendTag(DiskTags[1], m.fsType)

	// the only thing that actually changes is the free byte count:
	// free := int64(m.freeBytesDist.Get())
	free := atomic.AddInt64(&m.free, int64(fastrand.Uint32n(50)+1))
	if free > OneTerabyte {
		free = OneTerabyte
	}

	total := OneTerabyte
	used := total - free
	usedPercent := 100 * used / total

	// inodes are 4096b in size:
	inodesTotal := total / 4096
	inodesFree := free / 4096
	inodesUsed := used / 4096

	p.AppendField(TotalByteString, total)
	p.AppendField(FreeByteString, free)
	p.AppendField(UsedByteString, used)
	p.AppendField(UsedPercentByteString, usedPercent)
	p.AppendField(INodesTotalByteString, inodesTotal)
	p.AppendField(INodesFreeByteString, inodesFree)
	p.AppendField(INodesUsedByteString, inodesUsed)
	return true
}
