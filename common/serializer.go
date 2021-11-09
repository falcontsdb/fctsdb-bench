package common

import (
	"io"
)

type Serializer interface {
	SerializePoint(w io.Writer, p *Point) error
	SerializeSize(w io.Writer, points int64, values int64) error
}
