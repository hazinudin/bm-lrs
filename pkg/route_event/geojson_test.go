package route_event

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestToGeoJSON_BasicConversion(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Create schema with LAT, LON, and ROUTEID columns
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Add test data
	builder.Field(0).(*array.Float64Builder).Append(5.5072984)
	builder.Field(1).(*array.Float64Builder).Append(95.3588172)
	builder.Field(2).(*array.StringBuilder).Append("01002")

	builder.Field(0).(*array.Float64Builder).Append(5.506638)
	builder.Field(1).(*array.Float64Builder).Append(95.3594017)
	builder.Field(2).(*array.StringBuilder).Append("01002")

	rec := builder.NewRecordBatch()
	defer rec.Release()

	events, err := NewLRSEvents([]arrow.RecordBatch{rec}, "EPSG:4326")
	if err != nil {
		t.Fatalf("Failed to create LRSEvents: %v", err)
	}

	// Convert to GeoJSON
	geojsonBytes, err := events.ToGeoJSON()
	if err != nil {
		t.Fatalf("ToGeoJSON failed: %v", err)
	}

	// Parse the result
	var fc GeoJSONFeatureCollection
	if err := json.Unmarshal(geojsonBytes, &fc); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	// Verify structure
	if fc.Type != "FeatureCollection" {
		t.Errorf("Expected type FeatureCollection, got %s", fc.Type)
	}

	if len(fc.Features) != 2 {
		t.Errorf("Expected 2 features, got %d", len(fc.Features))
	}

	// Verify first feature
	f := fc.Features[0]
	if f.Type != "Feature" {
		t.Errorf("Expected Feature type, got %s", f.Type)
	}

	if f.Geometry.Type != "Point" {
		t.Errorf("Expected Point geometry, got %s", f.Geometry.Type)
	}

	// GeoJSON uses [lon, lat] order
	if f.Geometry.Coordinates[0] != 95.3588172 || f.Geometry.Coordinates[1] != 5.5072984 {
		t.Errorf("Unexpected coordinates: %v", f.Geometry.Coordinates)
	}

	if f.Properties["ROUTEID"] != "01002" {
		t.Errorf("Expected ROUTEID 01002, got %v", f.Properties["ROUTEID"])
	}
}

func TestToGeoJSON_WithMValueAndDist(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Create schema with LAT, LON, ROUTEID, MVAL, and dist columns
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
		{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
		{Name: "dist_to_line", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Add test data with M-value and distance
	builder.Field(0).(*array.Float64Builder).Append(5.5072984)
	builder.Field(1).(*array.Float64Builder).Append(95.3588172)
	builder.Field(2).(*array.StringBuilder).Append("01002")
	builder.Field(3).(*array.Float64Builder).Append(1234.567)
	builder.Field(4).(*array.Float64Builder).Append(5.5)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	events, err := NewLRSEvents([]arrow.RecordBatch{rec}, "EPSG:4326")
	if err != nil {
		t.Fatalf("Failed to create LRSEvents: %v", err)
	}

	// Convert to GeoJSON
	geojsonBytes, err := events.ToGeoJSON()
	if err != nil {
		t.Fatalf("ToGeoJSON failed: %v", err)
	}

	// Parse the result
	var fc GeoJSONFeatureCollection
	if err := json.Unmarshal(geojsonBytes, &fc); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if len(fc.Features) != 1 {
		t.Fatalf("Expected 1 feature, got %d", len(fc.Features))
	}

	f := fc.Features[0]

	// Verify MVAL and dist properties are present
	mval, ok := f.Properties["MVAL"]
	if !ok {
		t.Error("MVAL property not found")
	} else if mval != 1234.567 {
		t.Errorf("Expected MVAL 1234.567, got %v", mval)
	}

	dist, ok := f.Properties["dist_to_line"]
	if !ok {
		t.Error("dist_to_line property not found")
	} else if dist != 5.5 {
		t.Errorf("Expected dist_to_line 5.5, got %v", dist)
	}
}

func TestToGeoJSON_EmptyRecords(t *testing.T) {
	events, err := NewLRSEvents([]arrow.RecordBatch{}, "EPSG:4326")
	if err != nil {
		t.Fatalf("Failed to create LRSEvents: %v", err)
	}

	_, err = events.ToGeoJSON()
	if err == nil {
		t.Error("Expected error for empty records, got nil")
	}
}
