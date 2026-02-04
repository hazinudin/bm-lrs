package route_event

import (
	"bm-lrs/pkg/geom"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type LRSEvents struct {
	routeIDCol   string
	latCol       string
	lonCol       string
	mValCol      string
	distToLRSCol string
	records      []arrow.RecordBatch
	crs          string
	tempDir      string
	sourceFile   *string
}

func NewLRSEvents(records []arrow.RecordBatch, crs string) (*LRSEvents, error) {
	out := &LRSEvents{
		routeIDCol:   "ROUTEID",
		latCol:       "LAT",
		lonCol:       "LON",
		mValCol:      "MVAL",
		distToLRSCol: "DIST_TO_LRS",
		records:      records,
		crs:          crs,
	}

	if len(records) > 0 {
		if err := out.validate(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (e *LRSEvents) validate() error {
	if len(e.records) == 0 {
		return nil
	}

	schema := e.records[0].Schema()
	requiredCols := []string{e.routeIDCol, e.latCol, e.lonCol}

	for _, col := range requiredCols {
		indices := schema.FieldIndices(col)
		if len(indices) == 0 {
			return fmt.Errorf("required column %s not found in records", col)
		}
	}

	return nil
}

// NewLRSEventsFromGeoJSON creates LRSEvents from GeoJSON byte array
func NewLRSEventsFromGeoJSON(data []byte, crs string) (*LRSEvents, error) {
	var fc struct {
		Type     string `json:"type"`
		Features []struct {
			Type     string `json:"type"`
			Geometry struct {
				Type        string    `json:"type"`
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
		} `json:"features"`
	}

	if err := json.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal geojson: %w", err)
	}

	pool := memory.NewGoAllocator()

	// Create a list of all property keys to build the schema
	propKeys := make(map[string]struct{})
	for _, f := range fc.Features {
		for k := range f.Properties {
			propKeys[k] = struct{}{}
		}
	}

	fields := []arrow.Field{
		{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
	}

	for k := range propKeys {
		// Basic type inference: try to find the first non-nil value to determine type
		var fieldType arrow.DataType = arrow.BinaryTypes.String // Default to string
		for _, f := range fc.Features {
			if v, ok := f.Properties[k]; ok && v != nil {
				switch v.(type) {
				case float64:
					fieldType = arrow.PrimitiveTypes.Float64
				case int, int64:
					fieldType = arrow.PrimitiveTypes.Int64
				case bool:
					fieldType = arrow.FixedWidthTypes.Boolean
				}
				break
			}
		}
		fields = append(fields, arrow.Field{Name: k, Type: fieldType})
	}

	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	for _, f := range fc.Features {
		// coordinates[0] is lon, coordinates[1] is lat
		lon := f.Geometry.Coordinates[0]
		lat := f.Geometry.Coordinates[1]

		builder.Field(0).(*array.Float64Builder).Append(lat)
		builder.Field(1).(*array.Float64Builder).Append(lon)

		for i := 2; i < len(fields); i++ {
			fieldName := fields[i].Name
			val, ok := f.Properties[fieldName]
			if !ok || val == nil {
				builder.Field(i).AppendNull()
				continue
			}

			switch b := builder.Field(i).(type) {
			case *array.Float64Builder:
				b.Append(val.(float64))
			case *array.Int64Builder:
				// JSON numbers are float64 by default
				if fv, ok := val.(float64); ok {
					b.Append(int64(fv))
				} else {
					b.Append(val.(int64))
				}
			case *array.StringBuilder:
				b.Append(fmt.Sprint(val))
			case *array.BooleanBuilder:
				b.Append(val.(bool))
			default:
				builder.Field(i).AppendNull()
			}
		}
	}

	rec := builder.NewRecordBatch()
	return NewLRSEvents([]arrow.RecordBatch{rec}, crs)
}

// GetCRS returns the coordinate reference system of the events
func (e *LRSEvents) GetCRS() string {
	return e.crs
}

// GetRecords returns the arrow record batches
func (e *LRSEvents) GetRecords() []arrow.RecordBatch {
	return e.records
}

// GetGeometryType returns the geometry type (POINTS)
func (e *LRSEvents) GetGeometryType() geom.GeometryType {
	return geom.POINTS
}

// Release releases the arrow records and cleans up temporary files
func (e *LRSEvents) Release() {
	for _, rec := range e.records {
		rec.Release()
	}
	e.records = nil

	if e.tempDir != "" {
		os.RemoveAll(e.tempDir)
	}
}

// Sink the source record batch into parquet file
func (e *LRSEvents) Sink() error {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "lrs_events_*")
	if err != nil {
		return fmt.Errorf("failed to create temporary directory: %v", err)
	}
	e.tempDir = tempDir

	filePath := filepath.Join(tempDir, "events.parquet")

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer f.Close()

	if len(e.records) == 0 {
		return fmt.Errorf("records are empty")
	}

	schema := e.records[0].Schema()
	writer, err := pqarrow.NewFileWriter(
		schema,
		f,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps(),
	)

	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %v", err)
	}
	defer writer.Close()

	for _, rec := range e.records {
		if err := writer.WriteBuffered(rec); err != nil {
			return fmt.Errorf("failed to write record batch: %v", err)
		}
	}

	e.sourceFile = &filePath

	return nil
}

// GetSourceFile returns the path to the parquet file if materialized
func (e *LRSEvents) GetSourceFile() *string {
	return e.sourceFile
}

// GetAttributes returns a map of attributes related to the events
func (e *LRSEvents) GetAttributes() map[string]any {
	return map[string]any{
		"routeIDCol":   e.routeIDCol,
		"latCol":       e.latCol,
		"lonCol":       e.lonCol,
		"mValCol":      e.mValCol,
		"distToLRSCol": e.distToLRSCol,
	}
}

// GetRouteIDs returns all unique route IDs from the records
func (e *LRSEvents) GetRouteIDs() []string {
	routeIDs := make(map[string]struct{})
	for _, batch := range e.records {
		schema := batch.Schema()
		indices := schema.FieldIndices(e.routeIDCol)
		if len(indices) == 0 {
			continue
		}
		colIdx := indices[0]
		col := batch.Column(colIdx)
		strCol, ok := col.(*array.String)
		if !ok {
			// Try as binary if not string
			binCol, ok := col.(*array.Binary)
			if !ok {
				continue
			}
			for i := 0; i < binCol.Len(); i++ {
				if binCol.IsNull(i) {
					continue
				}
				routeIDs[string(binCol.Value(i))] = struct{}{}
			}
			continue
		}
		for i := 0; i < strCol.Len(); i++ {
			if strCol.IsNull(i) {
				continue
			}
			routeIDs[strCol.Value(i)] = struct{}{}
		}
	}

	out := make([]string, 0, len(routeIDs))
	for k := range routeIDs {
		out = append(out, k)
	}
	return out
}

// RouteIDColumn returns the name of the route ID column
func (e *LRSEvents) RouteIDColumn() string {
	return e.routeIDCol
}

// SetRouteIDColumn sets the name of the route ID column
func (e *LRSEvents) SetRouteIDColumn(col string) {
	e.routeIDCol = col
}

// LatitudeColumn returns the name of the latitude column
func (e *LRSEvents) LatitudeColumn() string {
	return e.latCol
}

// SetLatitudeColumn sets the name of the latitude column
func (e *LRSEvents) SetLatitudeColumn(col string) {
	e.latCol = col
}

// LongitudeColumn returns the name of the longitude column
func (e *LRSEvents) LongitudeColumn() string {
	return e.lonCol
}

// SetLongitudeColumn sets the name of the longitude column
func (e *LRSEvents) SetLongitudeColumn(col string) {
	e.lonCol = col
}

// MValueColumn returns the name of the m-value column
func (e *LRSEvents) MValueColumn() string {
	return e.mValCol
}

// SetMValueColumn sets the name of the m-value column
func (e *LRSEvents) SetMValueColumn(col string) {
	e.mValCol = col
}

// DistanceToLRSColumn returns the name of the distance to LRS column
func (e *LRSEvents) DistanceToLRSColumn() string {
	return e.distToLRSCol
}

// SetDistanceToLRSColumn sets the name of the distance to LRS column
func (e *LRSEvents) SetDistanceToLRSColumn(col string) {
	e.distToLRSCol = col
}
