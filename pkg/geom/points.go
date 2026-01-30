package geom

import (
	"github.com/apache/arrow-go/v18/arrow"
)

type Points struct {
	records         []arrow.RecordBatch
	LatitudeColumn  string
	LongitudeColumn string
	MValueColumn    string
	crs             string
}

func NewPoints(recs []arrow.RecordBatch, crs string) Points {
	return Points{
		records:         recs,
		LatitudeColumn:  "LAT",
		LongitudeColumn: "LON",
		MValueColumn:    "MVAL",
		crs:             crs,
	}
}

// Get Apache Arrow Records of the Points
func (p *Points) GetRecords() []arrow.RecordBatch {
	return p.records
}

// Release the Apache Arrow Recordsd buffer
func (p *Points) Release() {
	for i := range len(p.records) {
		p.records[i].Release()
	}
}

// Get CRS
func (p *Points) GetCRS() string {
	return p.crs
}

// Get geometry type
func (p *Points) GetGeometryType() GeometryType {
	return POINTS
}

// Get attributes
func (p *Points) GetAttributes() map[string]any {
	out := make(map[string]any)

	return out
}
