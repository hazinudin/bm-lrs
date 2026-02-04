package route

import (
	"bm-lrs/pkg/geom"
	"os"
	"testing"
)

func createTestLRSRouteFromJSON(t *testing.T, routeID string, filename string) LRSRoute {
	jsonByte, err := os.ReadFile("testdata/" + filename)
	if err != nil {
		t.Fatalf("Failed to read test data %s: %v", filename, err)
	}

	// Assuming CRS is available in the JSON or use a default one like geom.LAMBERT_WKT
	// NewLRSRouteFromESRIGeoJSON handles the parsing
	route := NewLRSRouteFromESRIGeoJSON(routeID, jsonByte, 0, geom.LAMBERT_WKT)
	return route
}

func TestLRSRouteBatch(t *testing.T) {
	t.Run("AddRoute", func(t *testing.T) {
		batch := &LRSRouteBatch{
			sourceFiles: &batchSourceFiles{},
		}

		route1 := createTestLRSRouteFromJSON(t, "01001", "lrs_01001.json")
		defer route1.Release()

		route2 := createTestLRSRouteFromJSON(t, "15010", "lrs_15010.json")
		defer route2.Release()

		// Add first route
		batch.AddRoute(route1)

		if len(batch.routes) != 1 {
			t.Errorf("Expected 1 route, got %d", len(batch.routes))
		}

		if len(batch.sourceFiles.Point) != 1 {
			t.Errorf("Expected 1 point source file, got %d", len(batch.sourceFiles.Point))
		}

		// Add second route
		batch.AddRoute(route2)

		if len(batch.routes) != 2 {
			t.Errorf("Expected 2 routes, got %d", len(batch.routes))
		}

		// Since route1 and route2 have different Sunk files (they are fresh GeoJSON), they should have different point source entries.
		if len(batch.sourceFiles.Point) != 2 {
			t.Errorf("Expected 2 point source files, got %d", len(batch.sourceFiles.Point))
		}
	})

	t.Run("GroupingBySourceFile", func(t *testing.T) {
		batch := &LRSRouteBatch{
			sourceFiles: &batchSourceFiles{},
		}

		// Create two routes that share the same underlying files (simulating GetLatest/Merged scenario)
		pointFile := "shared_point.parquet"
		segmentFile := "shared_segment.parquet"
		lineFile := "shared_line.parquet"

		route1 := LRSRoute{
			route_id: "01001",
			source_files: &sourceFiles{
				Point:      &pointFile,
				Segment:    &segmentFile,
				LineString: &lineFile,
			},
		}
		route1.setPushDown(true)

		route2 := LRSRoute{
			route_id: "01002",
			source_files: &sourceFiles{
				Point:      &pointFile,
				Segment:    &segmentFile,
				LineString: &lineFile,
			},
		}
		route2.setPushDown(true)

		anotherPoint := "another_point.parquet"
		anotherSegment := "another_segment.parquet"
		anotherLine := "another_line.parquet"

		route3 := LRSRoute{
			route_id: "01003",
			source_files: &sourceFiles{
				Point:      &anotherPoint,
				Segment:    &anotherSegment,
				LineString: &anotherLine,
			},
		}
		route3.setPushDown(false)

		batch.AddRoute(route1)
		batch.AddRoute(route2)
		batch.AddRoute(route3)

		if len(batch.sourceFiles.Point) != 2 {
			t.Errorf("Expected 1 shared point file, got %d", len(batch.sourceFiles.Point))
		}

		if len(batch.sourceFiles.Point[0].routes) != 2 {
			t.Errorf("Expected 2 routes in shared point file, got %d", len(batch.sourceFiles.Point[0].routes))
		}

		if batch.sourceFiles.Point[0].routes[0] != "01001" || batch.sourceFiles.Point[0].routes[1] != "01002" {
			t.Errorf("Expected routes [01001, 01002], got %v", batch.sourceFiles.Point[0].routes)
		}

		if len(batch.sourceFiles.Point[1].routes) != 0 {
			t.Errorf("Should not have routes because there is no pushdown.")
		}
	})

	t.Run("Release", func(t *testing.T) {
		batch := &LRSRouteBatch{
			sourceFiles: &batchSourceFiles{},
		}

		route1 := createTestLRSRouteFromJSON(t, "01002", "lrs_01002.json")
		// No manual release here, batch.Release() should handle it

		batch.AddRoute(route1)

		pointFile := batch.sourceFiles.Point[0].filePath

		batch.Release()

		if _, err := os.Stat(pointFile); !os.IsNotExist(err) {
			t.Errorf("File %s should not exist after Release", pointFile)
		}
	})

	t.Run("ViewName", func(t *testing.T) {
		batch := &LRSRouteBatch{}

		pointFile := "shared_point.parquet"
		route1 := LRSRoute{
			route_id: "01001",
			source_files: &sourceFiles{
				Point: &pointFile,
			},
		}
		route1.setPushDown(true)

		route2 := LRSRoute{
			route_id: "01002",
			source_files: &sourceFiles{
				Point: &pointFile,
			},
		}
		route2.setPushDown(true)

		batch.AddRoute(route1)
		batch.AddRoute(route2)

		query := batch.ViewName()
		expected := `(SELECT * FROM "shared_point.parquet" WHERE ROUTEID IN ['01001','01002'])`
		if query != expected {
			t.Errorf("Expected query %s, got %s", expected, query)
		}
	})
}
