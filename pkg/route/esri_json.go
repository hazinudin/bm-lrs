package route

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type EsriRouteJson struct {
	SpatialReference spatRef      `json:"spatialReference"`
	Features         []featureRow `json:"features"`
}

type spatRef struct {
	WKT  string `json:"wkt"`
	WKT2 string `json:"wkt2"`
}

type featureRow struct {
	Geometry   featureGeom    `json:"geometry"`
	Attributes map[string]any `json:"attributes"`
}

type featureGeom struct {
	HasM  bool         `json:"hasM"`
	Paths [][]vertexes `json:"paths"`
}

type vertexes [3]float64

// Feature count in the JSON
func (e *EsriRouteJson) FeatureCount() int {
	return len(e.Features)
}

// Create LRSRoute from ESRI GeoJSON
func NewLRSRouteFromESRIGeoJSON(jsonbyte []byte, feature_idx int, crs string) (LRSRoute, error) {
	var esriJson EsriRouteJson
	if err := json.Unmarshal(jsonbyte, &esriJson); err != nil {
		return LRSRoute{}, fmt.Errorf("failed to unmarshal esri json: %w, JSON: %s", err, string(jsonbyte))
	}

	if feature_idx < 0 || feature_idx >= len(esriJson.Features) {
		return LRSRoute{}, fmt.Errorf("feature_idx %d out of range", feature_idx)
	}

	feature := esriJson.Features[feature_idx]

	// Parse the LRS Vertex
	if len(feature.Geometry.Paths) == 0 {
		return LRSRoute{}, fmt.Errorf("missing or invalid paths")
	}

	route_id, ok := feature.Attributes["LINKID"].(string)
	if !ok {
		return LRSRoute{}, fmt.Errorf("missing or invalid LINKID")
	}

	wkt := esriJson.SpatialReference.WKT
	if wkt == "" {
		wkt = esriJson.SpatialReference.WKT2
	}
	if wkt == "" {
		return LRSRoute{}, fmt.Errorf("missing or invalid wkt")
	}

	pool := memory.NewGoAllocator()

	// Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
			{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
			{Name: "VERTEX_SEQ", Type: arrow.PrimitiveTypes.Int32},
			{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Builder
	lat_builder := array.NewFloat64Builder(pool)
	long_builder := array.NewFloat64Builder(pool)
	mval_builder := array.NewFloat64Builder(pool)
	vertex_seq_builder := array.NewInt32Builder(pool)
	routeid_builder := array.NewStringBuilder(pool)

	defer lat_builder.Release()
	defer long_builder.Release()
	defer mval_builder.Release()
	defer vertex_seq_builder.Release()
	defer routeid_builder.Release()

	// Append data
	vertex_seq := 0
	for _, path := range feature.Geometry.Paths {
		for _, v := range path {
			long_builder.Append(v[0])
			lat_builder.Append(v[1])
			mval_builder.Append(v[2])
			vertex_seq_builder.Append(int32(vertex_seq))
			routeid_builder.Append(route_id)
			vertex_seq++
		}
	}

	// Arrays
	lat_arr := lat_builder.NewArray()
	long_arr := long_builder.NewArray()
	mval_arr := mval_builder.NewArray()
	vertex_seq_arr := vertex_seq_builder.NewArray()
	routeid_arr := routeid_builder.NewArray()

	defer lat_arr.Release()
	defer long_arr.Release()
	defer mval_arr.Release()
	defer vertex_seq_arr.Release()
	defer routeid_arr.Release()

	rec := array.NewRecordBatch(
		schema,
		[]arrow.Array{
			lat_arr,
			long_arr,
			mval_arr,
			vertex_seq_arr,
			routeid_arr,
		},
		int64(vertex_seq_arr.Len()),
	)

	lrs := NewLRSRoute(
		route_id,
		[]arrow.RecordBatch{rec},
		wkt,
	)

	return lrs, nil
}
