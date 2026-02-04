package route_event_test

import (
	"bm-lrs/pkg/geom"
	"bm-lrs/pkg/route_event"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func createMockRecordBatch() arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
			{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
			{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
			{Name: "MVAL", Type: arrow.PrimitiveTypes.Float64},
			{Name: "DIST_TO_LRS", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	rb := array.NewRecordBuilder(pool, schema)
	defer rb.Release()

	rb.Field(0).(*array.StringBuilder).AppendValues([]string{"01002", "01002"}, nil)
	rb.Field(1).(*array.Float64Builder).AppendValues([]float64{5.5072984, 5.506638}, nil)
	rb.Field(2).(*array.Float64Builder).AppendValues([]float64{95.3588172, 95.3594017}, nil)
	rb.Field(3).(*array.Float64Builder).AppendValues(make([]float64, 2), nil)
	rb.Field(4).(*array.Float64Builder).AppendValues([]float64{0.1, 0.2}, nil)

	return rb.NewRecordBatch()
}

func TestNewLRSEvents(t *testing.T) {
	rec := createMockRecordBatch()
	crs := "EPSG:4326"
	events, err := route_event.NewLRSEvents([]arrow.RecordBatch{rec}, crs)
	assert.NoError(t, err)

	assert.NotNil(t, events)
	assert.Equal(t, crs, events.GetCRS())
	assert.Equal(t, 1, len(events.GetRecords()))
	assert.Equal(t, geom.POINTS, events.GetGeometryType())

	attrs := events.GetAttributes()
	assert.Equal(t, "ROUTEID", attrs["routeIDCol"])
	assert.Equal(t, "LAT", attrs["latCol"])
	assert.Equal(t, "LON", attrs["lonCol"])

	events.Release()
	assert.Nil(t, events.GetRecords())
}

func TestGetRouteIDs(t *testing.T) {
	rec := createMockRecordBatch()
	defer rec.Release()
	events, err := route_event.NewLRSEvents([]arrow.RecordBatch{rec}, "")
	assert.NoError(t, err)

	routeIDs := events.GetRouteIDs()
	assert.ElementsMatch(t, []string{"01002"}, routeIDs)
}

func TestNewLRSEventsFromGeoJSON(t *testing.T) {
	data, err := os.ReadFile("testdata/events.geojson")
	assert.NoError(t, err)

	events, err := route_event.NewLRSEventsFromGeoJSON(data, "EPSG:4326")
	assert.NoError(t, err)
	assert.NotNil(t, events)

	routeIDs := events.GetRouteIDs()
	assert.True(t, len(routeIDs) > 0)
	assert.Contains(t, routeIDs, "01002")

	recs := events.GetRecords()
	assert.Equal(t, 1, len(recs))
	assert.True(t, recs[0].NumRows() > 0)

	events.Sink()
	sourceFile := events.GetSourceFile()
	assert.NotNil(t, sourceFile)

	events.Release()
}

func TestLRSEventsSinkAndRelease(t *testing.T) {
	rec := createMockRecordBatch()
	events, err := route_event.NewLRSEvents([]arrow.RecordBatch{rec}, "")
	assert.NoError(t, err)

	err = events.Sink()
	assert.NoError(t, err)

	sourceFile := events.GetSourceFile()
	assert.NotNil(t, sourceFile)

	// Verify file exists
	_, err = os.Stat(*sourceFile)
	assert.NoError(t, err)

	// Get temp dir path
	tempDir := filepath.Dir(*sourceFile)

	events.Release()

	// Verify file and dir are gone
	_, err = os.Stat(*sourceFile)
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(tempDir)
	assert.True(t, os.IsNotExist(err))
}

func TestLRSEventsColumnGettersSetters(t *testing.T) {
	events, err := route_event.NewLRSEvents(nil, "")
	assert.NoError(t, err)

	assert.Equal(t, "ROUTEID", events.RouteIDColumn())
	events.SetRouteIDColumn("LINKID")
	assert.Equal(t, "LINKID", events.RouteIDColumn())

	assert.Equal(t, "LAT", events.LatitudeColumn())
	events.SetLatitudeColumn("latitude")
	assert.Equal(t, "latitude", events.LatitudeColumn())

	assert.Equal(t, "LON", events.LongitudeColumn())
	events.SetLongitudeColumn("longitude")
	assert.Equal(t, "longitude", events.LongitudeColumn())

	assert.Equal(t, "MVAL", events.MValueColumn())
	events.SetMValueColumn("m_value")
	assert.Equal(t, "m_value", events.MValueColumn())

	assert.Equal(t, "DIST_TO_LRS", events.DistanceToLRSColumn())
	events.SetDistanceToLRSColumn("distance")
	assert.Equal(t, "distance", events.DistanceToLRSColumn())
}

func TestNewLRSEventsValidation(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
			{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	rb := array.NewRecordBuilder(pool, schema)
	defer rb.Release()
	rec := rb.NewRecordBatch()
	defer rec.Release()

	// Missing ROUTEID
	_, err := route_event.NewLRSEvents([]arrow.RecordBatch{rec}, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required column ROUTEID not found")
}
