package serializers

import (
	"io"

	data_gen "git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

type Serializer interface {
	SerializePoint(w io.Writer, p *data_gen.Point) error
	SerializeSize(w io.Writer, points int64, values int64) error
}
