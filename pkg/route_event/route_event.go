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
	// Flag to indicate if records are in memory or only stored in file
	materialized bool
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
		materialized: false,
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
			Properties map[string]any `json:"properties"`
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

// IsMaterialized returns true if the events are stored in memory, false if only in file
func (e *LRSEvents) IsMaterialized() bool {
	return e.materialized
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

	for _, rec := range e.records {
		if err := writer.WriteBuffered(rec); err != nil {
			writer.Close()
			return fmt.Errorf("failed to write record batch: %v", err)
		}
	}

	// Close writer before releasing records to ensure statistics are written correctly
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %v", err)
	}

	// Make a copy of the file path on the heap before storing
	sourceFile := filePath
	e.sourceFile = &sourceFile

	// Release the in-memory RecordBatch buffers to free memory, but don't delete temp files
	for _, rec := range e.records {
		rec.Release()
	}
	e.records = nil
	e.materialized = true

	return nil
}

// NewLRSEventsFromFile creates LRSEvents from a parquet file path
// The records are NOT eagerly loaded; the file path is stored and records remain nil
// until LoadToBuffer() is called
// Column names are detected from the file schema
func NewLRSEventsFromFile(filePath string, crs string) (*LRSEvents, error) {
	// Open the parquet file to read its schema
	pf, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %v", err)
	}
	defer pf.Close()

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %v", err)
	}

	schema, err := reader.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %v", err)
	}

	// Detect column names from schema with common aliases
	routeIDCol := detectColumn(schema, []string{"ROUTEID", "LINKID", "route_id", "id"})
	latCol := detectColumn(schema, []string{"LAT", "TO_STA_LAT", "latitude", "lat"})
	lonCol := detectColumn(schema, []string{"LON", "TO_STA_LONG", "longitude", "lon"})
	mValCol := detectColumn(schema, []string{"MVAL", "MVAL", "m", "m_value"})
	distToLRSCol := detectColumn(schema, []string{"DIST_TO_LRS", "dist_to_lrs", "distance"})

	// Make a copy of the file path on the heap
	filePathCopy := filePath

	out := &LRSEvents{
		routeIDCol:   routeIDCol,
		latCol:       latCol,
		lonCol:       lonCol,
		mValCol:      mValCol,
		distToLRSCol: distToLRSCol,
		records:      nil, // Not loaded yet
		crs:          crs,
		sourceFile:   &filePathCopy,
		materialized: true,
	}

	return out, nil
}

// detectColumn finds a column name in the schema from a list of possible names
func detectColumn(schema *arrow.Schema, possibleNames []string) string {
	for _, name := range possibleNames {
		if indices := schema.FieldIndices(name); len(indices) > 0 {
			return name
		}
	}
	return possibleNames[0] // Return first option as default
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

		switch c := col.(type) {
		case *array.String:
			for i := 0; i < c.Len(); i++ {
				if !c.IsNull(i) {
					routeIDs[c.Value(i)] = struct{}{}
				}
			}
		case *array.LargeString:
			for i := 0; i < c.Len(); i++ {
				if !c.IsNull(i) {
					routeIDs[c.Value(i)] = struct{}{}
				}
			}
		case *array.Binary:
			for i := 0; i < c.Len(); i++ {
				if !c.IsNull(i) {
					routeIDs[string(c.Value(i))] = struct{}{}
				}
			}
		case *array.LargeBinary:
			for i := 0; i < c.Len(); i++ {
				if !c.IsNull(i) {
					routeIDs[string(c.Value(i))] = struct{}{}
				}
			}
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
