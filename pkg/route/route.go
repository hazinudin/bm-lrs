package route

import (
	"bm-lrs/pkg/geom"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/apache/arrow-go/v18/arrow"
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
	latitudeCol     string
	longitudeCol    string
	mValueCol       string
	VertexSeqColumn string
	crs             string
	source_files    *sourceFiles
	temp_dir        string
	push_down       bool
}

type LRSRouteInterface interface {
	ViewName() string
	SegmentQuery() string
	LinestringQuery() string
	Release()
	LatitudeColumn() string
	LongitudeColumn() string
	MValueColumn() string
}

func NewLRSRoute(route_id string, recs []arrow.RecordBatch, crs string) LRSRoute {
	out := LRSRoute{
		route_id:        route_id,
		records:         recs,
		latitudeCol:     "LAT",
		longitudeCol:    "LON",
		mValueCol:       "MVAL",
		VertexSeqColumn: "VERTEX_SEQ",
		crs:             crs,
		push_down:       false,
	}

	return out
}

func (l *LRSRoute) setPushDown(enable bool) {
	l.push_down = enable
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

// Get segment source file
func (l *LRSRoute) GetSegmentFile() *string {
	if l.source_files == nil {
		return nil
	} else {
		return l.source_files.Segment
	}
}

// Get linestring source file
func (l *LRSRoute) GetLineFile() *string {
	if l.source_files == nil {
		return nil
	} else {
		return l.source_files.LineString
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

func (l *LRSRoute) LatitudeColumn() string {
	return l.latitudeCol
}

func (l *LRSRoute) LongitudeColumn() string {
	return l.longitudeCol
}

func (l *LRSRoute) MValueColumn() string {
	return l.mValueCol
}

// DuckDB table view name
func (l *LRSRoute) ViewName() string {
	if l.IsMaterialized() {
		if l.push_down {
			return fmt.Sprintf(`(select * from "%s" where ROUTEID = '%s')`, *l.source_files.Point, l.GetRouteID())
		}
		return fmt.Sprintf(`(select * from "%s")`, *l.source_files.Point)
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

	// Release the RecordBatch buffer before setting the temp_dir attribute
	l.Release()

	l.source_files.Point = &filePath
	l.temp_dir = tempDir

	return nil
}

// LRS segment along with M-Value gradient and coefficient query
func (l *LRSRoute) SegmentQuery() string {
	if l.GetSegmentFile() != nil {
		if l.push_down {
			return fmt.Sprintf(`select * from "%s" where ROUTEID = '%s'`, *l.GetSegmentFile(), l.GetRouteID())
		} else {
			return fmt.Sprintf(`select * from "%s"`, *l.GetSegmentFile())
		}
	} else {
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
			"LongCol":      l.longitudeCol,
			"LatCol":       l.latitudeCol,
			"MvalCol":      l.mValueCol,
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
}

// LRS Route line string query.
func (l *LRSRoute) LinestringQuery() string {
	if l.GetSegmentFile() != nil {
		if l.push_down {
			return fmt.Sprintf(`select * from "%s" where ROUTEID = '%s'`, *l.GetLineFile(), l.GetRouteID())
		} else {
			return fmt.Sprintf(`select * from "%s"`, *l.GetLineFile())
		}
	} else {
		query := `
		select {{.RouteID}} as ROUTEID, ST_Makeline(
		list(ST_Point({{.LatCol}}, {{.LongCol}}) order by {{.VertexSeqCol}} asc)
		) as linestr 
		from {{.ViewName}}
		`

		data := map[string]string{
			"LongCol":      l.longitudeCol,
			"LatCol":       l.latitudeCol,
			"MvalCol":      l.mValueCol,
			"ViewName":     l.ViewName(),
			"VertexSeqCol": l.VertexSeqColumn,
			"RouteID":      fmt.Sprintf("'%s'", l.GetRouteID()),
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
}
