package geom

import "github.com/apache/arrow-go/v18/arrow"

type GeometryType string

const (
	LRS    GeometryType = "lrs"
	POINTS GeometryType = "points"
)

type Geometry interface {
	GetCRS() string
	GetRecords() []arrow.RecordBatch
	GetGeometryType() GeometryType
	Release()
	GetAttributes() map[string]any
}
