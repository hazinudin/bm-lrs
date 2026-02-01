package route

import (
	"bm-lrs/pkg/geom"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"
)

func TestLRSRoute(t *testing.T) {
	c, err := duckdb.NewConnector("", nil)

	if err != nil {
		log.Fatal(err)
	}

	conn, err := c.Connect(context.Background())
	db := sql.OpenDB(c)
	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}

	ar, err := duckdb.NewArrowFromConn(conn)
	defer c.Close()

	// Read test data JSON
	// Parse from ESRI GeoJSON fetched from a feature service.
	jsonFile, err := os.Open("./testdata/lrs_01001.json")

	if err != nil {
		fmt.Println(err)
		return
	}
	defer jsonFile.Close()

	jsonByte, _ := io.ReadAll(jsonFile)

	var jsonContent map[string]any

	json.Unmarshal([]byte(jsonByte), &jsonContent)

	// Parse the LRS Vertex
	var vertexes []any
	WKT := jsonContent["spatialReference"].(map[string]any)["wkt"].(string)
	features := jsonContent["features"].([]any)
	feature := features[0].(map[string]any)["geometry"].(map[string]any)
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

	t.Run(
		"initialize from arrow record", func(t *testing.T) {
			lrs := NewLRSRoute(
				"01001",
				[]arrow.RecordBatch{rec},
				WKT,
			)
			defer lrs.Release()
		},
	)

	t.Run(
		"initialize from geojson", func(t *testing.T) {
			lrs := NewLRSRouteFromESRIGeoJSON(
				"01001",
				jsonByte,
				0,
				WKT,
			)
			defer lrs.Release()
		},
	)

	t.Run(
		"geometry type test", func(t *testing.T) {
			lrs := NewLRSRouteFromESRIGeoJSON(
				"01001",
				jsonByte,
				0,
				WKT,
			)
			defer lrs.Release()

			if lrs.GetGeometryType() != geom.LRS {
				t.Errorf("")
			}
		},
	)

	t.Run(
		"segment table view test", func(t *testing.T) {
			lrs := NewLRSRouteFromESRIGeoJSON(
				"01001",
				jsonByte,
				0,
				WKT,
			)
			defer lrs.Release()

			rr, err := array.NewRecordReader(lrs.GetRecords()[0].Schema(), lrs.GetRecords())
			if err != nil {
				t.Error(err)
			}

			release, err := ar.RegisterView(rr, lrs.ViewName())
			defer release()

			// Write to parquet file for evaluation
			_, err = db.QueryContext(context.Background(), fmt.Sprintf("copy (%s) to 'testdata/lrs_01001_segment.parquet' (FORMAT parquet);", lrs.SegmentQuery()))
			if err != nil {
				t.Error(err)
			}

			out_reader, err := ar.QueryContext(context.Background(), lrs.SegmentQuery())
			if err != nil {
				t.Error(err)
			}

			for out_reader.Next() {
				rec := out_reader.RecordBatch()
				fmt.Printf("%v %v\n", rec.NumRows(), rec.NumCols())
			}
		},
	)

	t.Run(
		"linestring table view test", func(t *testing.T) {
			lrs := NewLRSRouteFromESRIGeoJSON(
				"01001",
				jsonByte,
				0,
				WKT,
			)
			defer lrs.Release()

			rr, err := array.NewRecordReader(lrs.GetRecords()[0].Schema(), lrs.GetRecords())
			_, err = db.ExecContext(context.Background(), "install spatial; load spatial;")
			if err != nil {
				t.Error(err)
			}

			release, err := ar.RegisterView(rr, lrs.ViewName())
			defer release()

			// Write to parquet file for evaluation
			_, err = db.QueryContext(context.Background(), fmt.Sprintf("copy (%s) to 'testdata/lrs_01001_linestr.parquet' (FORMAT parquet);", lrs.LinestringQuery()))
			if err != nil {
				t.Error(err)
			}
		},
	)

	t.Run(
		"sink function test", func(t *testing.T) {
			lrs := NewLRSRouteFromESRIGeoJSON(
				"01001",
				jsonByte,
				0,
				WKT,
			)
			defer lrs.Release()

			lrs.Sink()

			// 3. Verify
			if lrs.source_files == nil || lrs.source_files.Point == nil {
				t.Fatal("Sink() did not populate source_files.Point")
			}

			filePath := *lrs.source_files.Point
			t.Logf("Generated file path: %s", filePath)

			// Check if file exists
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				t.Errorf("File %s does not exist", filePath)
			}

			// Check if file is in a temp dir
			if lrs.temp_dir == "" {
				t.Error("temp_dir is empty")
			}

			// 4. Call Release() and verify cleanup
			lrs.Release()

			// Check if file is gone
			if _, err := os.Stat(filePath); !os.IsNotExist(err) {
				t.Errorf("File %s still exists after Release()", filePath)
			}

			// Check if temp dir is gone
			if _, err := os.Stat(lrs.temp_dir); !os.IsNotExist(err) {
				t.Errorf("Temp dir %s still exists after Release()", lrs.temp_dir)
			}

		},
	)
}
