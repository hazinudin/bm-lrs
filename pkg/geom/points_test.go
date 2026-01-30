package geom

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestPoints(t *testing.T) {
	t.Run(
		"initialize points", func(t *testing.T) {
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

			p := NewPoints([]arrow.RecordBatch{rec}, "EPSG:4326")
			p.Release()
		},
	)
}
