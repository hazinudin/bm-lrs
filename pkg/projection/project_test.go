package projection

import (
	"bm-lrs/pkg/geom"
	"bm-lrs/pkg/route"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestTransform(t *testing.T) {
	t.Run(
		"transform Points object", func(t *testing.T) {
			pool := memory.NewGoAllocator()

			// Schema
			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
					{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
					{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
				},
				nil,
			)

			// Builder
			lat_builder := array.NewFloat64Builder(pool)
			long_builder := array.NewFloat64Builder(pool)
			mval_builder := array.NewFloat64Builder(pool)

			defer lat_builder.Release()
			defer long_builder.Release()
			defer mval_builder.Release()

			lat_builder.AppendValues([]float64{5.647860000331377}, nil)
			long_builder.AppendValues([]float64{95.42103999972832}, nil)
			mval_builder.AppendValues(make([]float64, 1), nil)

			// Arrays
			lat_arr := lat_builder.NewArray()
			long_arr := long_builder.NewArray()
			mval_arr := mval_builder.NewArray()

			// Record
			rec := array.NewRecordBatch(
				schema,
				[]arrow.Array{
					lat_arr,
					long_arr,
					mval_arr,
				},
				int64(lat_arr.Len()),
			)

			p := geom.NewPoints([]arrow.RecordBatch{rec}, "EPSG:4326")
			defer p.Release()

			t.Run(
				"project to lambert", func(t *testing.T) {
					// Lambert WKT
					lambert_wkt := `PROJCS["Indonesia Lambert Conformal Conic",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",115.0],PARAMETER["Standard_Parallel_1",2.0],PARAMETER["Standard_Parallel_2",-7.0],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]`
					new_p, err := Transform(&p, lambert_wkt, false) // Transform to Lambert

					if err != nil {
						t.Error(err)
					}

					defer new_p.Release()
				},
			)

			t.Run(
				"project back lambert to 4326", func(t *testing.T) {
					// Lambert WKT
					lambert_wkt := `PROJCS["Indonesia Lambert Conformal Conic",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",115.0],PARAMETER["Standard_Parallel_1",2.0],PARAMETER["Standard_Parallel_2",-7.0],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]`
					new_p, err := Transform(&p, lambert_wkt, false) // Transform to Lambert

					if err != nil {
						t.Error(err)
					}

					defer new_p.Release()

					test_p, err := Transform(new_p, "EPSG:4326", true) // Transform back to 4326

					if err != nil {
						t.Error(err)
					}
					defer test_p.Release()
				},
			)
		},
	)

	t.Run(
		"project LRSRoute object", func(t *testing.T) {
			lambert_wkt := `PROJCS["Indonesia Lambert Conformal Conic",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",115.0],PARAMETER["Standard_Parallel_1",2.0],PARAMETER["Standard_Parallel_2",-7.0],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]`

			// Read test data JSON
			// Parse from ESRI GeoJSON fetched from a feature service.
			jsonFile, err := os.Open("../route/testdata/lrs_01001.json")
			if err != nil {
				t.Error(err)
			}

			defer jsonFile.Close()

			jsonByte, _ := io.ReadAll(jsonFile)

			lrs := route.NewLRSRouteFromESRIGeoJSON(
				jsonByte,
				0,
				lambert_wkt,
			)
			defer lrs.Release()

			new_lrs, err := Transform(&lrs, "EPSG:4326", true)
			if err != nil {
				t.Error(err)
			}
			new_lrs.Release()
		},
	)
}
