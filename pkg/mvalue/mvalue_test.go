package mvalue

import (
	"bm-lrs/pkg/geom"
	"bm-lrs/pkg/route"
	"bm-lrs/pkg/route_event"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestCalculatePointsMValue(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
			{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
			{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Builder
	lat_builder := array.NewFloat64Builder(pool)
	long_builder := array.NewFloat64Builder(pool)
	mval_builder := array.NewFloat64Builder(pool)
	linkid_builder := array.NewStringBuilder(pool)

	defer lat_builder.Release()
	defer long_builder.Release()
	defer mval_builder.Release()
	defer linkid_builder.Release()

	long_builder.AppendValues([]float64{-2191377.9268000014, -2191367.4395999983}, nil)
	lat_builder.AppendValues([]float64{602211.73600000143, 602215.71829999983}, nil)
	mval_builder.AppendValues(make([]float64, 2), nil)
	linkid_builder.AppendValues([]string{"01001", "01001"}, nil)

	// Arrays
	lat_arr := lat_builder.NewArray()
	long_arr := long_builder.NewArray()
	mval_arr := mval_builder.NewArray()
	linkid_arr := linkid_builder.NewArray()

	// Record
	rec := array.NewRecordBatch(
		schema,
		[]arrow.Array{
			lat_arr,
			long_arr,
			mval_arr,
			linkid_arr,
		},
		int64(lat_arr.Len()),
	)

	points, err := route_event.NewLRSEvents([]arrow.RecordBatch{rec}, geom.LAMBERT_WKT)
	assert.NoError(t, err)
	defer points.Release()

	// Read test data JSON
	// Parse from ESRI GeoJSON fetched from a feature service.
	jsonFile, err := os.Open("../route/testdata/lrs_01001.json")

	if err != nil {
		fmt.Println(err)
		return
	}
	defer jsonFile.Close()

	jsonByte, _ := io.ReadAll(jsonFile)

	var jsonContent map[string]any

	json.Unmarshal([]byte(jsonByte), &jsonContent)

	lrs, err := route.NewLRSRouteFromESRIGeoJSON(
		jsonByte,
		0,
		geom.LAMBERT_WKT,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lrs.Release()

	lrs.Sink()

	// 3. Calculate M-Values
	result, err := CalculatePointsMValue(context.Background(), &lrs, *points)
	if err != nil {
		t.Fatalf("CalculatePointsMValue failed: %v", err)
	}

	resultRecs := result.GetRecords()
	if len(resultRecs) != 1 {
		t.Fatalf("Expected 1 record batch, got %d", len(resultRecs))
	}

	// Check results
	mvals := resultRecs[0].Column(3).(*array.Float64)
	expectedMVals := []float64{0, 0.0111}
	for i, expected := range expectedMVals {
		if math.Abs(mvals.Value(i)-expected) > 0.001 {
			t.Errorf("Point %d: expected MVAL %f, got %f", i, expected, mvals.Value(i))
		}
	}

	// Check dist_to_line
	dists := resultRecs[0].Column(4).(*array.Float64)
	expectedDists := []float64{0, 0}
	for i, expected := range expectedDists {
		if math.Abs(dists.Value(i)-expected) > 0.001 {
			t.Errorf("Point %d: expected dist %f, got %f", i, expected, dists.Value(i))
		}
	}

	result.Sink()
	eventFile := result.GetSourceFile()
	assert.NotNil(t, eventFile)

	result.Release()
}

func TestCalculatePointsMValueBatch(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 1. Setup Points
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
			{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
			{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	lat_builder := array.NewFloat64Builder(pool)
	long_builder := array.NewFloat64Builder(pool)
	mval_builder := array.NewFloat64Builder(pool)
	routeid_builder := array.NewStringBuilder(pool)

	defer lat_builder.Release()
	defer long_builder.Release()
	defer mval_builder.Release()
	defer routeid_builder.Release()

	// Points for Route 01001
	long_builder.AppendValues([]float64{-2191377.9268000014, -2191367.4395999983}, nil)
	lat_builder.AppendValues([]float64{602211.73600000143, 602215.71829999983}, nil)

	// Points for Route 01002
	long_builder.AppendValues([]float64{-2190936.8995999992, -2190911.0421999991}, nil)
	lat_builder.AppendValues([]float64{593568.98829999566, 593544.87000000477}, nil)

	mval_builder.AppendValues(make([]float64, 4), nil)
	routeid_builder.AppendValues([]string{"01001", "01001", "01002", "01002"}, nil)

	// Arrays
	lat_arr := lat_builder.NewArray()
	long_arr := long_builder.NewArray()
	mval_arr := mval_builder.NewArray()
	routeid_arr := routeid_builder.NewArray()

	// Record
	rec := array.NewRecordBatch(
		schema,
		[]arrow.Array{
			lat_arr,
			long_arr,
			mval_arr,
			routeid_arr,
		},
		int64(lat_arr.Len()),
	)

	points, err := route_event.NewLRSEvents([]arrow.RecordBatch{rec}, geom.LAMBERT_WKT)
	assert.NoError(t, err)
	defer points.Release()

	// 2. Setup LRSRouteBatch
	batch := &route.LRSRouteBatch{}

	// Load Route 01001
	jsonByte1, _ := os.ReadFile("../route/testdata/lrs_01001.json")
	lrs1, err := route.NewLRSRouteFromESRIGeoJSON(jsonByte1, 0, geom.LAMBERT_WKT)
	if err != nil {
		t.Fatal(err)
	}
	defer lrs1.Release()
	lrs1.Sink()
	batch.AddRoute(lrs1)

	// Load Route 01002
	jsonByte2, _ := os.ReadFile("../route/testdata/lrs_01002.json")
	lrs2, err := route.NewLRSRouteFromESRIGeoJSON(jsonByte2, 0, geom.LAMBERT_WKT)
	if err != nil {
		t.Fatal(err)
	}
	defer lrs2.Release()
	lrs2.Sink()
	batch.AddRoute(lrs2)

	// 3. Calculate M-Values
	result, err := CalculatePointsMValue(context.Background(), batch, *points)
	if err != nil {
		t.Fatalf("CalculatePointsMValue failed: %v", err)
	}

	resultRecs := result.GetRecords()
	if len(resultRecs) != 1 {
		t.Fatalf("Expected 1 record batch, got %d", len(resultRecs))
	}

	// Check results
	mvals := resultRecs[0].Column(3).(*array.Float64)
	expectedMVals := []float64{0, 0.0111, 0, 0.03536}
	for i, expected := range expectedMVals {
		if math.Abs(mvals.Value(i)-expected) > 0.001 {
			t.Errorf("Point %d: expected MVAL %f, got %f", i, expected, mvals.Value(i))
		}
	}

	result.Sink()
	defer result.Release()

	eventFile := result.GetSourceFile()
	assert.NotNil(t, eventFile)
}
