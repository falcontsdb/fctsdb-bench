package serializers

import (
	data_gen "git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
)

type Serializer interface {
	SerializePrepare(w []byte, p *data_gen.Point) []byte
	SerializePoint(w []byte, p *data_gen.Point) []byte
	SerializeEnd(w []byte, p *data_gen.Point) []byte
	SerializeSize(w []byte, points int64, values int64) []byte
}
