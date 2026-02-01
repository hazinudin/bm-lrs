package route

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

type LRSRouteRepository struct {
	connector *duckdb.Connector
	pgConnStr string
	db        *sql.DB
}

func NewLRSRouteRepository(connector *duckdb.Connector, pgConnStr string, db *sql.DB) *LRSRouteRepository {
	return &LRSRouteRepository{
		connector: connector,
		pgConnStr: pgConnStr,
		db:        db,
	}
}

// SyncOptions contains options for syncing LRS data
type SyncOptions struct {
	Author    string
	CommitMsg string
}

// Sync fetches data from ArcGIS, processes it into Parquet files, and updates the Postgres catalog.
func (r *LRSRouteRepository) Sync(ctx context.Context, routeID string, opts SyncOptions) error {
	// 1. Load ArcGIS URL from env
	arcgisURL := os.Getenv("ARCGIS_FEATURE_SERVICE_URL")
	if arcgisURL == "" {
		return fmt.Errorf("ARCGIS_FEATURE_SERVICE_URL environment variable not set")
	}

	// 2. Fetch GeoJSON from ArcGIS
	geoJSON, err := r.fetchArcGISData(ctx, arcgisURL, routeID)
	if err != nil {
		return fmt.Errorf("failed to fetch arcgis data: %w", err)
	}

	return r.syncFromGeoJSON(ctx, routeID, geoJSON, opts)
}

// syncFromGeoJSON processes GeoJSON data into Parquet files and updates the Postgres catalog.
// This is the internal method that can be called directly for testing.
func (r *LRSRouteRepository) syncFromGeoJSON(ctx context.Context, routeID string, geoJSON []byte, opts SyncOptions) error {
	// Create LRSRoute object
	lrsRoute := NewLRSRouteFromESRIGeoJSON(routeID, geoJSON, 0, "EPSG:4326")
	defer lrsRoute.Release()

	// 4. DuckDB Processing
	conn, err := r.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get db connection: %w", err)
	}
	defer conn.Close()

	// Sink the record batch first
	err = lrsRoute.Sink()
	if err != nil {
		return fmt.Errorf("Failed to sink the LRS object: %v", err)
	}

	// Install spatial extension
	if _, err := r.db.ExecContext(ctx, "INSTALL spatial; LOAD spatial;"); err != nil {
		return fmt.Errorf("failed to load spatial extension: %w", err)
	}

	// Paths for parquet files
	dataDir := os.Getenv("LRS_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	segmentFile := filepath.Join(dataDir, fmt.Sprintf("lrs_segment_%s_%d.parquet", routeID, time.Now().Unix()))
	linestringFile := filepath.Join(dataDir, fmt.Sprintf("lrs_linestring_%s_%d.parquet", routeID, time.Now().Unix()))

	// Export Segment Query to Parquet
	segmentQuery := lrsRoute.SegmentQuery()
	_, err = r.db.ExecContext(ctx, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET);", segmentQuery, segmentFile))
	if err != nil {
		return fmt.Errorf("failed to export segment parquet: %w", err)
	}

	// Export Linestring Query to Parquet
	linestringQuery := lrsRoute.LinestringQuery()
	_, err = r.db.ExecContext(ctx, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET);", linestringQuery, linestringFile))
	if err != nil {
		return fmt.Errorf("failed to export linestring parquet: %w", err)
	}

	// 5. Postgres Transaction
	// Install and load postgres extension
	if _, err := r.db.ExecContext(ctx, "INSTALL postgres; LOAD postgres;"); err != nil {
		return fmt.Errorf("failed to load postgres extension: %w", err)
	}

	// Attach Postgres database
	_, err = r.db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS postgres_db (TYPE POSTGRES)", r.pgConnStr))
	if err != nil {
		return fmt.Errorf("failed to attach postgres: %w", err)
	}

	// Begin transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// Create catalog table if not exists
	createTable := `
	CREATE TABLE IF NOT EXISTS postgres_db.lrs_catalogs (
		VERSION INTEGER,
		START_DATE DATE,
		END_DATE DATE,
		LRS_POINT_FILE TEXT,
		LRS_SEGMENT_FILE TEXT,
		LRS_LINESTR_FILE TEXT,
		AUTHOR TEXT,
		COMMIT_MSG TEXT
	)`
	_, err = tx.ExecContext(ctx, createTable)
	if err != nil {
		return fmt.Errorf("failed to create catalog table: %w", err)
	}

	// Get next version number
	var nextVersion int
	err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(VERSION), 0) + 1 FROM postgres_db.lrs_catalogs").Scan(&nextVersion)
	if err != nil {
		return fmt.Errorf("failed to get next version: %w", err)
	}

	// Insert catalog record
	insertQuery := `INSERT INTO postgres_db.lrs_catalogs 
		(VERSION, START_DATE, END_DATE, LRS_POINT_FILE, LRS_SEGMENT_FILE, LRS_LINESTR_FILE, AUTHOR, COMMIT_MSG) 
		VALUES (?, CURRENT_DATE, NULL, ?, ?, ?, ?, ?)`

	_, err = tx.ExecContext(ctx, insertQuery, nextVersion, lrsRoute.GetPointFile(), segmentFile, linestringFile, opts.Author, opts.CommitMsg)
	if err != nil {
		return fmt.Errorf("failed to insert catalog record: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetLatest retrieves the latest LRSRoute data from the catalog.
func (r *LRSRouteRepository) GetLatest(ctx context.Context, routeID string) (*LRSRoute, error) {
	// Install postgres extension
	if _, err := r.db.ExecContext(ctx, "INSTALL postgres; LOAD postgres;"); err != nil {
		return nil, fmt.Errorf("failed to load postgres extension: %w", err)
	}

	// Attach Postgres
	_, err := r.db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", r.pgConnStr))
	if err != nil {
		return nil, fmt.Errorf("failed to attach postgres: %w", err)
	}

	// Query for latest active catalog entry (END_DATE is NULL means active)
	query := `
		SELECT LRS_POINT_FILE, LRS_SEGMENT_FILE, LRS_LINESTR_FILE, VERSION
		FROM postgres_db.lrs_catalogs 
		WHERE END_DATE IS NULL
		ORDER BY VERSION DESC 
		LIMIT 1
	`
	var segmentPath, linestringPath, pointPath string
	var version int
	err = r.db.QueryRowContext(ctx, query).Scan(&pointPath, &segmentPath, &linestringPath, &version)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no active catalog entry found")
		}
		return nil, fmt.Errorf("failed to query latest catalog: %w", err)
	}

	return &LRSRoute{
		route_id:        routeID,
		LatitudeColumn:  "LAT",
		LongitudeColumn: "LON",
		MValueColumn:    "MVAL",
		VertexSeqColumn: "VERTEX_SEQ",
		crs:             "EPSG:4326",
		source_files: &sourceFiles{
			Point:      &pointPath,
			Segment:    &segmentPath,
			LineString: &linestringPath,
		},
	}, nil
}

// fetchArcGISData fetches GeoJSON data from ArcGIS Feature Service
func (r *LRSRouteRepository) fetchArcGISData(ctx context.Context, baseURL string, routeID string) ([]byte, error) {
	// Construct query URL
	url := fmt.Sprintf("%s/query?where=RouteId='%s'&outFields=*&f=geojson", baseURL, routeID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Add authentication if needed from env
	token := os.Getenv("ARCGIS_TOKEN")
	if token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("arcgis request failed with status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}
