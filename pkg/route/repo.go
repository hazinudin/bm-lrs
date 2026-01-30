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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/duckdb/duckdb-go/v2"
)

type LRSRouteRepository struct {
	connector *duckdb.Connector
	pgConnStr string
}

func NewLRSRouteRepository(connector *duckdb.Connector, pgConnStr string) *LRSRouteRepository {
	return &LRSRouteRepository{
		connector: connector,
		pgConnStr: pgConnStr,
	}
}

// Sync fetches data from ArcGIS, processes it into Parquet files, and updates the Postgres catalog.
func (r *LRSRouteRepository) Sync(ctx context.Context, routeID string) error {
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

	// 3. Create LRSRoute object
	lrsRoute := NewLRSRouteFromESRIGeoJSON(routeID, geoJSON, 0, "EPSG:4326")
	defer lrsRoute.Release()

	// 4. DuckDB Processing
	conn, err := r.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get db connection: %w", err)
	}
	defer conn.Close()

	// Get Arrow interface
	ar, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		return fmt.Errorf("failed to create arrow interface: %w", err)
	}

	// Register Arrow View
	records := lrsRoute.GetRecords()
	rr, err := array.NewRecordReader(records[0].Schema(), records)
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}

	release, err := ar.RegisterView(rr, lrsRoute.ViewName())
	if err != nil {
		return fmt.Errorf("failed to register arrow view: %w", err)
	}
	defer release()

	// Open DB for SQL operations
	db := sql.OpenDB(r.connector)
	defer db.Close()

	// Install spatial extension
	if _, err := db.ExecContext(ctx, "INSTALL spatial; LOAD spatial;"); err != nil {
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
	_, err = db.ExecContext(ctx, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", segmentQuery, segmentFile))
	if err != nil {
		return fmt.Errorf("failed to export segment parquet: %w", err)
	}

	// Export Linestring Query to Parquet
	linestringSelect := fmt.Sprintf(`
		SELECT ST_Makeline(
			list(ST_Point(%s, %s) ORDER BY %s ASC)
		) as linestr FROM %s
	`, lrsRoute.LatitudeColumn, lrsRoute.LongitudeColumn, lrsRoute.VertexSeqColumn, lrsRoute.ViewName())

	_, err = db.ExecContext(ctx, fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", linestringSelect, linestringFile))
	if err != nil {
		return fmt.Errorf("failed to export linestring parquet: %w", err)
	}

	// 5. Postgres Transaction
	// Attach Postgres database
	_, err = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", r.pgConnStr))
	if err != nil {
		return fmt.Errorf("failed to attach postgres: %w", err)
	}

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// Create catalog table if not exists
	createTable := `
	CREATE TABLE IF NOT EXISTS postgres_db.lrs_route_files (
		route_id TEXT,
		file_type TEXT,
		file_path TEXT,
		created_at TIMESTAMP
	)`
	_, err = tx.ExecContext(ctx, createTable)
	if err != nil {
		return fmt.Errorf("failed to create catalog table: %w", err)
	}

	// Insert file records
	insertQuery := `INSERT INTO postgres_db.lrs_route_files (route_id, file_type, file_path, created_at) VALUES (?, ?, ?, ?)`

	_, err = tx.ExecContext(ctx, insertQuery, routeID, "segment", segmentFile, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert segment file record: %w", err)
	}

	_, err = tx.ExecContext(ctx, insertQuery, routeID, "linestring", linestringFile, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert linestring file record: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetLatest retrieves the latest LRSRoute data from the catalog.
func (r *LRSRouteRepository) GetLatest(ctx context.Context, routeID string) (*LRSRoute, error) {
	db := sql.OpenDB(r.connector)
	defer db.Close()

	// Attach Postgres
	_, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", r.pgConnStr))
	if err != nil {
		return nil, fmt.Errorf("failed to attach postgres: %w", err)
	}

	// Query for latest segment file
	query := `
		SELECT file_path 
		FROM postgres_db.lrs_route_files 
		WHERE route_id = ? AND file_type = ? 
		ORDER BY created_at DESC 
		LIMIT 1
	`
	var segmentPath string
	err = db.QueryRowContext(ctx, query, routeID, "segment").Scan(&segmentPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no segment file found for route %s", routeID)
		}
		return nil, fmt.Errorf("failed to query latest segment file: %w", err)
	}

	var linestringPath string
	err = db.QueryRowContext(ctx, query, routeID, "linestring").Scan(&linestringPath)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to query latest linestring file: %w", err)
	}

	return &LRSRoute{
		route_id:        routeID,
		LatitudeColumn:  "LAT",
		LongitudeColumn: "LON",
		MValueColumn:    "MVAL",
		VertexSeqColumn: "VERTEX_SEQ",
		crs:             "EPSG:4326",
		source_files: &lrsFiles{
			Segment:    segmentPath,
			LineString: linestringPath,
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
