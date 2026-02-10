package route

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

var testPgConnStr string

func init() {
	if err := godotenv.Load("../../.env"); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}
	testPgConnStr = fmt.Sprintf("dbname=%s user=%s password=%s host=%s",
		os.Getenv("DB_NAME"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
	)
}

func TestSyncFromGeoJSON(t *testing.T) {
	// Setup DuckDB connector
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("Failed to create DuckDB connector: %v", err)
	}
	defer connector.Close()

	// Open DB for SQL operation
	db := sql.OpenDB(connector)
	defer db.Close()

	// Cleanup any stale catalog entries from previous/other runs
	// This prevents IO Errors when merging with deleted temp files.
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, "install postgres; load postgres;")
	_, err = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", testPgConnStr))
	if err == nil {
		_, _ = db.ExecContext(ctx, "DELETE FROM postgres_db.lrs_catalogs")
	}

	// Create temp directory for output files
	tempDir, err := os.MkdirTemp("", "lrs_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	// Delete all the temporary files
	defer os.RemoveAll(tempDir)

	// Set env for data directory
	os.Setenv("LRS_DATA_DIR", tempDir)
	defer os.Unsetenv("LRS_DATA_DIR")

	// Read test GeoJSON data
	jsonFile, err := os.Open("./testdata/lrs_01001.json")
	if err != nil {
		t.Fatalf("Failed to open test JSON: %v", err)
	}
	defer jsonFile.Close()

	jsonBytes, err := io.ReadAll(jsonFile)
	if err != nil {
		t.Fatalf("Failed to read test JSON: %v", err)
	}

	// Test full sync with Postgres
	t.Run("sync with postgres", func(t *testing.T) {
		ctx := context.Background()

		// Create repository with Postgres connection
		repo := NewLRSRouteRepository(connector, testPgConnStr, db)

		// sync
		err := repo.SyncFromGeoJSON(ctx, jsonBytes, SyncOptions{Author: "SYSTEM", CommitMsg: "TEST"})
		if err != nil {
			t.Fatal(err)
		}

		t.Run("fetch the route data", func(t *testing.T) {
			lrs, err := repo.GetLatest(ctx, "01001")
			if err != nil {
				t.Fatal(err)
			}
			defer lrs.Release()

			if !lrs.IsMaterialized() {
				t.Errorf("LRS is not materialized")
			}
		})

		t.Run("sync second route and merge", func(t *testing.T) {
			// Read test GeoJSON data
			jsonFile2, err := os.Open("./testdata/lrs_01002.json")
			if err != nil {
				t.Fatalf("Failed to open test JSON: %v", err)
			}
			defer jsonFile2.Close()

			jsonBytes2, err := io.ReadAll(jsonFile2)
			if err != nil {
				t.Fatalf("Failed to read test JSON: %v", err)
			}

			// Sync same json but as different route
			err = repo.SyncFromGeoJSON(ctx, jsonBytes2, SyncOptions{Author: "SYSTEM", CommitMsg: "TEST2"})
			if err != nil {
				t.Fatalf("Failed to sync second route: %v", err)
			}

			// Verify we can get the second route
			lrs2, err := repo.GetLatest(ctx, "01002")
			if err != nil {
				t.Fatalf("Failed to get second route: %v", err)
			}
			defer lrs2.Release()

			// Check if push_down is working and we only see routeID2 count
			// ViewName() should return a query with filtering
			// We can query duckdb to check the count

			// We need to attach the parquet file to check raw content to verify merge
			pointFile := *lrs2.GetPointFile()

			var count int64
			// Check total rows in the parquet file (should be 2 routes * points per route)
			// The json has 340 points. So total should be 680.
			err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM '%s'", pointFile)).Scan(&count)
			if err != nil {
				t.Errorf("Failed to query parquet file: %v", err)
			}
			if count == 0 {
				t.Error("Parquet file is empty")
			}
			// Exact count might vary if points are different, but here we used same json.
			// Let's check distinctive ROUTEIDs
			rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT DISTINCT ROUTEID FROM '%s'", pointFile))
			if err != nil {
				t.Errorf("Failed to query distinct routeIDs: %v", err)
			}
			defer rows.Close()

			routeCount := 0
			for rows.Next() {
				routeCount++
			}
			if routeCount != 2 {
				t.Errorf("Expected 2 distinct routes in merged file, got %d", routeCount)
			}

			// Verify push_down works regarding ViewName
			// lrs2.ViewName() should already include the filter
			viewQuery := lrs2.ViewName()
			// Execute this query and check if we only get records for 01002
			rows2, err := db.QueryContext(ctx, fmt.Sprintf("SELECT distinct ROUTEID FROM %s", viewQuery))
			if err != nil {
				t.Errorf("Failed to query view with push down: %v", err)
			}
			defer rows2.Close()

			var fetchedRouteID string
			if rows2.Next() {
				rows2.Scan(&fetchedRouteID)
				if fetchedRouteID != "01002" {
					t.Errorf("Expected routeID %s, got %s", "01002", fetchedRouteID)
				}
			}
			if rows2.Next() {
				t.Error("Projected view returned more than one route ID")
			}
		})

		// Cleanup: delete test data from postgres
		_, err = db.ExecContext(ctx, "install postgres; load postgres;")
		_, err = db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS postgres_db (TYPE POSTGRES)", repo.pgConnStr))
		_, err = db.ExecContext(ctx, "DROP TABLE postgres_db.lrs_catalogs;")

		if err != nil {
			t.Logf("Warning: Failed to cleanup test data: %v", err)
		}
	})
}

func TestNewLRSRouteRepository(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("Failed to create DuckDB connector: %v", err)
	}
	defer connector.Close()

	// Open DB for SQL operation
	cleanupConn, err := duckdb.NewConnector("", nil)
	db := sql.OpenDB(cleanupConn)
	defer db.Close()

	repo := NewLRSRouteRepository(connector, testPgConnStr, db)

	if repo == nil {
		t.Error("NewLRSRouteRepository returned nil")
	}
	if repo.connector != connector {
		t.Error("Repository connector not set correctly")
	}
	if repo.pgConnStr != testPgConnStr {
		t.Error("Repository pgConnStr not set correctly")
	}
}
func TestGenerateArcGISToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.FormValue("username") != "subditadps" {
			t.Errorf("Expected username subditadps, got %s", r.FormValue("username"))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"token": "test-token-123", "expires": 123456789}`)
	}))
	defer ts.Close()

	repo := &LRSRouteRepository{
		tokenURL: ts.URL,
	}

	token, err := repo.GenerateArcGISToken(context.Background())
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	if token != "test-token-123" {
		t.Errorf("Expected token test-token-123, got %s", token)
	}
}

func TestFetchArcGISFeatures(t *testing.T) {
	repo := &LRSRouteRepository{}
	repo.arcgisFetchLimit = 250
	repo.tokenURL = "https://gisportal.binamarga.pu.go.id/portal/sharing/rest/generateToken"
	repo.featureServiceURL = "https://gisportal.binamarga.pu.go.id/arcgis/rest/services/Jalan/BinaMargaLRS/MapServer/0/query"

	token, err := repo.GenerateArcGISToken(context.Background())
	if err != nil {
		t.Errorf("failed to generate access token: %v", err)
	}
	t.Logf("generated token: %s", token)

	data, err := repo.FetchArcGISFeatures(context.Background(), token, []string{"01001", "01002"}, false, nil)
	if err == nil {
		os.WriteFile("testdata/arcgis_fetched_debug.json", data, 0644)
	}

	dataStr := string(data)
	assert.NotNil(t, dataStr)

	if err != nil {
		t.Fatalf("Failed to fetch features: %v", err)
	}

	if !strings.Contains(string(data), "esriGeometryPolyline") {
		t.Errorf("Expected FeatureCollection in response, got %s", string(data))
	}
}
func TestSync(t *testing.T) {
	// Setup DuckDB
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	db := sql.OpenDB(connector)
	defer db.Close()

	// Initial cleanup
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, "install postgres; load postgres;")
	_, _ = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", testPgConnStr))
	_, _ = db.ExecContext(ctx, "DELETE FROM postgres_db.lrs_catalogs")

	// Temp dir
	tempDir, _ := os.MkdirTemp("", "sync_test_*")
	defer os.RemoveAll(tempDir)
	os.Setenv("LRS_DATA_DIR", tempDir)
	defer os.Unsetenv("LRS_DATA_DIR")

	repo := NewLRSRouteRepository(connector, testPgConnStr, db)

	ctx = context.Background()
	err = repo.Sync(ctx, []string{"01001", "01002", "15001"}, SyncOptions{Author: "TESTER", CommitMsg: "MOCK SYNC"})
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Verify catalog exists and has entry
	_, err = db.ExecContext(ctx, "install postgres; load postgres;")
	_, err = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", testPgConnStr))
	if err != nil {
		t.Fatalf("Failed to attach postgres: %v", err)
	}

	var version int
	err = db.QueryRowContext(ctx, "SELECT VERSION FROM postgres_db.lrs_catalogs WHERE AUTHOR = 'TESTER'").Scan(&version)
	if err != nil {
		t.Fatalf("Failed to verify catalog entry: %v", err)
	}

	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}

	// Cleanup
	db.ExecContext(ctx, "DELETE FROM postgres_db.lrs_catalogs WHERE AUTHOR = 'TESTER'")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS postgres_db.lrs_routes")
}

func TestSyncAll(t *testing.T) {
	// Setup DuckDB
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	db := sql.OpenDB(connector)
	defer db.Close()

	// Initial cleanup
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, "install postgres; load postgres;")
	_, _ = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", testPgConnStr))
	_, _ = db.ExecContext(ctx, "DELETE FROM postgres_db.lrs_catalogs")

	// Temp dir
	tempDir, _ := os.MkdirTemp("", "sync_all_test_*")
	defer os.RemoveAll(tempDir)
	os.Setenv("LRS_DATA_DIR", tempDir)
	defer os.Unsetenv("LRS_DATA_DIR")

	// Mock server for ArcGIS
	featureCount := 500 // simulate 500 features
	limit := 250        // mock limit same as default
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle token generation
		if strings.Contains(r.URL.Path, "generateToken") {
			fmt.Fprint(w, `{"token": "mock-token", "expires": 123456789}`)
			return
		}

		// Handle feature query (count only)
		if r.URL.Query().Get("returnCountOnly") == "true" {
			fmt.Fprintf(w, `{"count": %d}`, featureCount)
			return
		}

		// Handle feature query (data)
		// We expect pagination params: resultOffset and resultRecordCount
		offsetStr := r.URL.Query().Get("resultOffset")
		countStr := r.URL.Query().Get("resultRecordCount")

		offset, _ := strconv.Atoi(offsetStr)
		count, _ := strconv.Atoi(countStr)
		if count == 0 {
			count = limit
		}

		// Generate mock features
		features := []string{}

		// Use a template for a simple feature
		featureTpl := `{
			"attributes": {
				"OBJECTID": %d,
				"RouteId": "R%d",
				"LINKID": "L%d", 
				"LINK_NAME": "Link %d",
				"SK_LENGTH": 100.0,
				"LAT": 0.0,
				"LON": 0.0,
				"MVAL": 0.0,
				"VERTEX_SEQ": 0
			},
			"geometry": {
				"paths": [
					[
						[110.0, -7.0, 0, 0],
						[110.1, -7.1, 10, 1]
					]
				]
			}
		}`

		end := offset + count
		if end > featureCount {
			end = featureCount
		}

		for i := offset; i < end; i++ {
			features = append(features, fmt.Sprintf(featureTpl, i, i, i, i))
		}

		responseTpl := `{
			"spatialReference": {"wkt": "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]]"},
			"features": [%s]
		}`
		fmt.Fprintf(w, responseTpl, strings.Join(features, ","))
	}))
	defer mockServer.Close()

	repo := NewLRSRouteRepository(connector, testPgConnStr, db)
	// Override URLs to point to mock server
	repo.tokenURL = mockServer.URL + "/generateToken"
	repo.featureServiceURL = mockServer.URL + "/query"
	repo.arcgisFetchLimit = limit

	ctx = context.Background()
	err = repo.SyncAll(ctx, SyncOptions{Author: "TESTER_ALL", CommitMsg: "MOCK SYNC ALL"})
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	// Verify catalog exists and has entry
	_, err = db.ExecContext(ctx, "install postgres; load postgres;")
	_, err = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", testPgConnStr))
	if err != nil {
		t.Fatalf("Failed to attach postgres: %v", err)
	}

	var version int
	err = db.QueryRowContext(ctx, "SELECT VERSION FROM postgres_db.lrs_catalogs WHERE AUTHOR = 'TESTER_ALL'").Scan(&version)
	if err != nil {
		t.Fatalf("Failed to verify catalog entry: %v", err)
	}

	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}

	// Cleanup
	db.ExecContext(ctx, "DELETE FROM postgres_db.lrs_catalogs WHERE AUTHOR = 'TESTER_ALL'")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS postgres_db.lrs_routes")
}
