package route_event

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// GeoJSONFeatureCollection represents a GeoJSON FeatureCollection
type GeoJSONFeatureCollection struct {
	Type     string           `json:"type"`
	Features []GeoJSONFeature `json:"features"`
}

// GeoJSONFeature represents a GeoJSON Feature
type GeoJSONFeature struct {
	Type       string                 `json:"type"`
	Geometry   GeoJSONGeometry        `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

// GeoJSONGeometry represents a GeoJSON Geometry
type GeoJSONGeometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

// ToGeoJSON converts LRSEvents to GeoJSON FeatureCollection bytes
func (e *LRSEvents) ToGeoJSON() ([]byte, error) {
	if len(e.records) == 0 {
		return nil, fmt.Errorf("no records to convert")
	}

	fc := GeoJSONFeatureCollection{
		Type:     "FeatureCollection",
		Features: make([]GeoJSONFeature, 0),
	}

	for _, batch := range e.records {
		schema := batch.Schema()

		// Find LAT and LON column indices
		latIndices := schema.FieldIndices(e.latCol)
		lonIndices := schema.FieldIndices(e.lonCol)

		if len(latIndices) == 0 || len(lonIndices) == 0 {
			return nil, fmt.Errorf("LAT or LON column not found in records")
		}

		latIdx := latIndices[0]
		lonIdx := lonIndices[0]

		latCol := batch.Column(latIdx)
		lonCol := batch.Column(lonIdx)

		numRows := int(batch.NumRows())

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			// Get lat/lon values
			lat, err := getFloat64Value(latCol, rowIdx)
			if err != nil {
				return nil, fmt.Errorf("failed to get LAT value at row %d: %v", rowIdx, err)
			}
			lon, err := getFloat64Value(lonCol, rowIdx)
			if err != nil {
				return nil, fmt.Errorf("failed to get LON value at row %d: %v", rowIdx, err)
			}

			// Build properties from other columns
			properties := make(map[string]interface{})
			for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
				fieldName := schema.Field(colIdx).Name
				// Skip LAT and LON columns as they go into geometry
				if fieldName == e.latCol || fieldName == e.lonCol {
					continue
				}

				col := batch.Column(colIdx)
				val, err := getColumnValue(col, rowIdx)
				if err == nil && val != nil {
					properties[fieldName] = val
				}
			}

			feature := GeoJSONFeature{
				Type: "Feature",
				Geometry: GeoJSONGeometry{
					Type:        "Point",
					Coordinates: []float64{lon, lat}, // GeoJSON uses [lon, lat] order
				},
				Properties: properties,
			}

			fc.Features = append(fc.Features, feature)
		}
	}

	return json.MarshalIndent(fc, "", "  ")
}

// getFloat64Value extracts a float64 value from an Arrow column at a given index
func getFloat64Value(col interface{}, idx int) (float64, error) {
	switch c := col.(type) {
	case *array.Float64:
		if c.IsNull(idx) {
			return 0, fmt.Errorf("null value")
		}
		return c.Value(idx), nil
	case *array.Float32:
		if c.IsNull(idx) {
			return 0, fmt.Errorf("null value")
		}
		return float64(c.Value(idx)), nil
	case *array.Int64:
		if c.IsNull(idx) {
			return 0, fmt.Errorf("null value")
		}
		return float64(c.Value(idx)), nil
	case *array.Int32:
		if c.IsNull(idx) {
			return 0, fmt.Errorf("null value")
		}
		return float64(c.Value(idx)), nil
	default:
		return 0, fmt.Errorf("unsupported column type for float conversion: %T", col)
	}
}

// getColumnValue extracts a value from an Arrow column at a given index
func getColumnValue(col interface{}, idx int) (interface{}, error) {
	switch c := col.(type) {
	case *array.Float64:
		if c.IsNull(idx) {
			return nil, nil
		}
		return c.Value(idx), nil
	case *array.Float32:
		if c.IsNull(idx) {
			return nil, nil
		}
		return float64(c.Value(idx)), nil
	case *array.Int64:
		if c.IsNull(idx) {
			return nil, nil
		}
		return c.Value(idx), nil
	case *array.Int32:
		if c.IsNull(idx) {
			return nil, nil
		}
		return int64(c.Value(idx)), nil
	case *array.String:
		if c.IsNull(idx) {
			return nil, nil
		}
		return c.Value(idx), nil
	case *array.LargeString:
		if c.IsNull(idx) {
			return nil, nil
		}
		return c.Value(idx), nil
	case *array.Boolean:
		if c.IsNull(idx) {
			return nil, nil
		}
		return c.Value(idx), nil
	case *array.Binary:
		if c.IsNull(idx) {
			return nil, nil
		}
		return string(c.Value(idx)), nil
	case *array.LargeBinary:
		if c.IsNull(idx) {
			return nil, nil
		}
		return string(c.Value(idx)), nil
	default:
		return nil, fmt.Errorf("unsupported column type: %T", col)
	}
}
