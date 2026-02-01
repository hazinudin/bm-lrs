package route

import (
	"bm-lrs/pkg/geom"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type sourceFiles struct {
	Point      *string
	Segment    *string
	LineString *string
}

type LRSRoute struct {
	route_id        string
	records         []arrow.RecordBatch
	LatitudeColumn  string
	LongitudeColumn string
	MValueColumn    string
	VertexSeqColumn string
	crs             string
	source_files    *sourceFiles
	temp_dir        string
}

func NewLRSRoute(route_id string, recs []arrow.RecordBatch, crs string) LRSRoute {
	return LRSRoute{
		route_id:        route_id,
		records:         recs,
		LatitudeColumn:  "LAT",
		LongitudeColumn: "LON",
		MValueColumn:    "MVAL",
		VertexSeqColumn: "VERTEX_SEQ",
		crs:             crs,
	}
}

// Create LRSRoute from ESRI GeoJSON
func NewLRSRouteFromESRIGeoJSON(route_id string, jsonbyte []byte, feature_idx int, crs string) LRSRoute {
	var jsonContent map[string]any

	json.Unmarshal([]byte(jsonbyte), &jsonContent)

	// Parse the LRS Vertex
	var vertexes []any
	WKT := jsonContent["spatialReference"].(map[string]any)["wkt"].(string)
	features := jsonContent["features"].([]any)
	feature := features[feature_idx].(map[string]any)["geometry"].(map[string]any)
	vertexes = feature["paths"].([]any)[0].([]any)

	pool := memory.NewGoAllocator()

	// Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
			{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
			{Name: "VERTEX_SEQ", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// Builder
	lat_builder := array.NewFloat64Builder(pool)
	long_builder := array.NewFloat64Builder(pool)
	mval_builder := array.NewFloat64Builder(pool)
	vertex_seq_builder := array.NewInt32Builder(pool)

	defer lat_builder.Release()
	defer long_builder.Release()
	defer mval_builder.Release()
	defer vertex_seq_builder.Release()

	// Append data
	var lat_rows []float64
	var long_rows []float64
	var mval_rows []float64
	var vertex_seq_rows []int32

	for i, vertex := range vertexes {
		long_rows = append(long_rows, vertex.([]any)[0].(float64))
		lat_rows = append(lat_rows, vertex.([]any)[1].(float64))
		mval_rows = append(mval_rows, vertex.([]any)[2].(float64))
		vertex_seq_rows = append(vertex_seq_rows, int32(i))
	}

	lat_builder.AppendValues(lat_rows, nil)
	long_builder.AppendValues(long_rows, nil)
	mval_builder.AppendValues(mval_rows, nil)
	vertex_seq_builder.AppendValues(vertex_seq_rows, nil)

	// Arrays
	lat_arr := lat_builder.NewArray()
	long_arr := long_builder.NewArray()
	mval_arr := mval_builder.NewArray()
	vertex_seq_arr := vertex_seq_builder.NewArray()

	rec := array.NewRecordBatch(
		schema,
		[]arrow.Array{
			lat_arr,
			long_arr,
			mval_arr,
			vertex_seq_arr,
		},
		int64(vertex_seq_arr.Len()),
	)

	lrs := NewLRSRoute(
		route_id,
		[]arrow.RecordBatch{rec},
		WKT,
	)

	return lrs
}

// Get Apache Arrow Records of the LRS Route
func (l *LRSRoute) GetRecords() []arrow.RecordBatch {
	return l.records
}

// Release the Apache Arrow Records buffer
func (l *LRSRoute) Release() {
	for i := range len(l.records) {
		l.records[i].Release()
	}

	// Clean up temp dir if exists
	if l.temp_dir != "" {
		os.RemoveAll(l.temp_dir)
	}
}

// Get CRS
func (l *LRSRoute) GetCRS() string {
	return l.crs
}

// Get point source file
func (l *LRSRoute) GetPointFile() *string {
	if l.source_files == nil {
		return nil
	} else {
		return l.source_files.Point
	}
}

// Get Route ID
func (l *LRSRoute) GetRouteID() string {
	return l.route_id
}

// Get geometry type
func (l *LRSRoute) GetGeometryType() geom.GeometryType {
	return geom.LRS
}

// Get attributes
func (l *LRSRoute) GetAttributes() map[string]any {
	out := make(map[string]any)

	out["RouteID"] = l.GetRouteID()

	return out
}

// DuckDB table view name
func (l *LRSRoute) ViewName() string {
	if l.IsMaterialized() {
		return fmt.Sprintf(`select *, '%s' as ROUTEID from "%s"`, l.GetRouteID(), *l.source_files.Point)
	} else {
	return "lrs_recordbatch"
}
}

// If the data is materialized into a file
func (l *LRSRoute) IsMaterialized() bool {
	if l.GetPointFile() != nil {
		return true
	} else {
		return false
	}
}

// Sink the source record batch into parquet file
func (l *LRSRoute) Sink() error {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "lrs_route_*")
	if err != nil {
		return fmt.Errorf("Failed to create temporary directory: %v", err)
	}
	l.temp_dir = tempDir

	filePath := filepath.Join(tempDir, fmt.Sprintf("temp_%s.parquet", l.route_id))

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Failed to create file: %v", err)
	}
	defer f.Close()

	if len(l.records) == 0 {
		return fmt.Errorf("records are empty")
	}

	schema := l.records[0].Schema()
	writer, err := pqarrow.NewFileWriter(
		schema,
		f,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy)),
		pqarrow.DefaultWriterProps(),
	)

	if err != nil {
		return fmt.Errorf("Failed to create parquet writer: %v", err)
	}
	defer writer.Close()

	for _, rec := range l.records {
		if err := writer.WriteBuffered(rec); err != nil {
			return fmt.Errorf("Failed to write record batch: %v", err)
		}
	}

	if l.source_files == nil {
		l.source_files = &sourceFiles{}
	}
	l.source_files.Point = &filePath

	return nil
}

// LRS segment along with M-Value gradient and coefficient query
func (l *LRSRoute) SegmentQuery() string {
	query := `
	select *,
	({{.LatCol}}1-{{.LatCol}})/({{.LongCol}}-{{.LongCol}}1) as mvgradient,
	{{.LatCol}}-(mvgradient*{{.LongCol}})as c
	from
	(
		select 
		* exclude({{.LatCol}}, {{.LongCol}}, {{.MvalCol}}, {{.VertexSeqCol}}),
		{{.LongCol}}, {{.LatCol}}, {{.MvalCol}}, {{.VertexSeqCol}},
		LEAD({{.LongCol}}, 1, null) over (order by {{.VertexSeqCol}}) as {{.LongCol}}1,
		LEAD({{.LatCol}}, 1, null) over (order by {{.VertexSeqCol}}) as {{.LatCol}}1,
		LEAD({{.MvalCol}}, 1, null) over (order by {{.VertexSeqCol}}) as {{.MvalCol}}1
		from {{.ViewName}}
	)
	where {{.LongCol}}1 is not null
	`

	data := map[string]string{
		"LongCol":      l.LongitudeColumn,
		"LatCol":       l.LatitudeColumn,
		"MvalCol":      l.MValueColumn,
		"ViewName":     l.ViewName(),
		"VertexSeqCol": l.VertexSeqColumn,
	}

	templ, err := template.New("queryTemplate").Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, data)
	if err != nil {
		log.Fatal(err)
	}

	return buf.String()
}

// LRS Route line string query.
func (l *LRSRoute) LinestringQuery() string {
	query := `
	select ST_Makeline(
	list(ST_Point({{.LatCol}}, {{.LongCol}}) order by {{.VertexSeqCol}} asc)
	) as linestr 
	 from {{.ViewName}}
	`

	data := map[string]string{
		"LongCol":      l.LongitudeColumn,
		"LatCol":       l.LatitudeColumn,
		"MvalCol":      l.MValueColumn,
		"ViewName":     l.ViewName(),
		"VertexSeqCol": l.VertexSeqColumn,
	}

	templ, err := template.New("queryTemplate").Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, data)
	if err != nil {
		log.Fatal(err)
	}

	return buf.String()

}
