package route

import (
	"bm-lrs/pkg/geom"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

type LRSRouteRepository struct {
	connector         *duckdb.Connector
	pgConnStr         string
	db                *sql.DB
	tokenURL          string
	featureServiceURL string
}

func NewLRSRouteRepository(connector *duckdb.Connector, pgConnStr string, db *sql.DB) *LRSRouteRepository {
	return &LRSRouteRepository{
		connector:         connector,
		pgConnStr:         pgConnStr,
		db:                db,
		tokenURL:          "https://gisportal.binamarga.pu.go.id/portal/sharing/rest/generateToken",
		featureServiceURL: "https://gisportal.binamarga.pu.go.id/arcgis/rest/services/Jalan/BinaMargaLRS/MapServer/0/query",
	}
}

// SyncOptions contains options for syncing LRS data
type SyncOptions struct {
	Author    string
	CommitMsg string
}

// Sync fetches data from ArcGIS, processes it into Parquet files, and updates the Postgres catalog.
func (r *LRSRouteRepository) Sync(ctx context.Context, routeIDs []string, opts SyncOptions) error {
	// 1. Generate ArcGIS Token
	token, err := r.GenerateArcGISToken(ctx)
	if err != nil {
		return fmt.Errorf("failed to generate arcgis token: %w", err)
	}

	// 2. Fetch GeoJSON from ArcGIS
	geoJSON, err := r.FetchArcGISFeatures(ctx, token, routeIDs)
	if err != nil {
		return fmt.Errorf("failed to fetch arcgis features: %w", err)
	}

	return r.SyncFromGeoJSON(ctx, geoJSON, opts)
}

// SyncFromGeoJSON processes GeoJSON data into Parquet files and updates the Postgres catalog.
// This is the internal method that can be called directly for testing.
func (r *LRSRouteRepository) SyncFromGeoJSON(ctx context.Context, geoJSON []byte, opts SyncOptions) error {
	var jsonContent map[string]any

	err := json.Unmarshal(geoJSON, &jsonContent)
	if err != nil {
		return fmt.Errorf("failed to unmarshal geojson: %w", err)
	}
	// The total count of features included in the ESRI JSON
	featuresCount := len(jsonContent["features"].([]any))

	// Create LRSBatch
	lrsBatch := LRSRouteBatch{
		latitudeCol:  "LAT",
		longitudeCol: "LON",
	}
	defer lrsBatch.Release()

	for idx := range featuresCount {
		lrsRoute := NewLRSRouteFromESRIGeoJSON(geoJSON, idx, geom.LAMBERT_WKT)
		defer lrsRoute.Release()

		lrsBatch.AddRoute(lrsRoute)
	}

	return r.mergeWithExisting(ctx, &lrsBatch, opts)
}

func (r *LRSRouteRepository) mergeWithExisting(ctx context.Context, lrsBatch *LRSRouteBatch, opts SyncOptions) error {
	// DuckDB Processing
	conn, err := r.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get db connection: %w", err)
	}
	defer conn.Close()

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

	nanoStamp := time.Now().UnixNano()
	mergedPointFile := filepath.Join(dataDir, fmt.Sprintf("lrs_point_merged_%d.parquet", nanoStamp))
	mergedSegmentFile := filepath.Join(dataDir, fmt.Sprintf("lrs_segment_merged_%d.parquet", nanoStamp))
	mergedLinestringFile := filepath.Join(dataDir, fmt.Sprintf("lrs_linestring_merged_%d.parquet", nanoStamp))

	// Get latest merged files to merge with
	latestLRS, err := r.GetLatest(ctx, "")
	// If err is nil, we have a previous version. If not nil (likely no rows), we start fresh.
	// Note: GetLatest requires a routeID, but here we might want just any latest catalog entry.
	// Let's modify GetLatest logic slightly or query directly.
	// Actually GetLatest takes a routeID but currently ignores it for file retrieval (logic finds *latest catalog*).
	// So we can re-use GetLatest with a dummy ID, or better, query explicitly here safely.

	var prevPointFile, prevSegmentFile, prevLinestrFile string
	// Helper to check if previous files exist
	hasPrev := false
	if err == nil && latestLRS != nil {
		// Valid previous catalog
		if latestLRS.GetPointFile() != nil {
			prevPointFile = *latestLRS.GetPointFile()
			hasPrev = true
		}
		if latestLRS.source_files != nil {
			if latestLRS.source_files.Segment != nil {
				prevSegmentFile = *latestLRS.source_files.Segment
			}
			if latestLRS.source_files.LineString != nil {
				prevLinestrFile = *latestLRS.source_files.LineString
			}
		}
	}

	// 1. Merge Points
	// Logic: Union (Previous - CurrentRoute) + CurrentRoute
	var queryPoint string
	if hasPrev {
		queryPoint = fmt.Sprintf(`
			SELECT * FROM '%s' WHERE ROUTEID NOT IN (SELECT DISTINCT(ROUTEID) FROM %s)
			UNION ALL
			SELECT * FROM %s
		`, prevPointFile, lrsBatch.ViewName(), lrsBatch.ViewName())
	} else {
		// First time, just current route
		// If ViewName() returns something like (select ...), we can use it directly in COPY
		queryPoint = lrsBatch.ViewName()
	}

	copyPointSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET)", queryPoint, mergedPointFile)
	if !strings.HasPrefix(strings.TrimSpace(queryPoint), "(") {
		copyPointSQL = fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", queryPoint, mergedPointFile)
	}

	_, err = r.db.ExecContext(ctx, copyPointSQL)
	if err != nil {
		return fmt.Errorf("failed to export merged point parquet: %w", err)
	}

	// 2. Merge Segments
	// Current route segment query
	currentSegmentQuery := fmt.Sprintf(`SELECT * FROM (%s)`, lrsBatch.SegmentQuery())

	var querySegment string
	if hasPrev && prevSegmentFile != "" {
		querySegment = fmt.Sprintf(`
			SELECT * FROM '%s' WHERE ROUTEID NOT IN (SELECT DISTINCT(ROUTEID) FROM (%s))
			UNION ALL
			%s
		`, prevSegmentFile, currentSegmentQuery, currentSegmentQuery)
	} else {
		querySegment = currentSegmentQuery
	}
	copySegmentSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET)", querySegment, mergedSegmentFile)
	if !strings.HasPrefix(strings.TrimSpace(querySegment), "(") {
		copySegmentSQL = fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", querySegment, mergedSegmentFile)
	}

	_, err = r.db.ExecContext(ctx, copySegmentSQL)
	if err != nil {
		return fmt.Errorf("failed to export merged segment parquet: %w", err)
	}

	// 3. Merge Linestrings
	// Current route linestring query
	// Note: LinestringQuery returns just 'linestr'. We need to add ROUTEID.
	currentLinestrQuery := fmt.Sprintf(`SELECT * FROM (%s)`, lrsBatch.LinestringQuery())

	var queryLinestr string
	if hasPrev && prevLinestrFile != "" {
		queryLinestr = fmt.Sprintf(`
			SELECT * FROM '%s' WHERE ROUTEID NOT IN (SELECT DISTINCT(ROUTEID) FROM (%s))
			UNION ALL
			%s
		`, prevLinestrFile, currentLinestrQuery, currentLinestrQuery)
	} else {
		queryLinestr = currentLinestrQuery
	}

	copyLinestrSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET)", queryLinestr, mergedLinestringFile)
	if !strings.HasPrefix(strings.TrimSpace(queryLinestr), "(") {
		copyLinestrSQL = fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", queryLinestr, mergedLinestringFile)
	}

	_, err = r.db.ExecContext(ctx, copyLinestrSQL)
	if err != nil {
		return fmt.Errorf("failed to export merged linestring parquet: %w", err)
	}

	// 5. Postgres Transaction
	// Install and load postgres extension
	if _, err := r.db.ExecContext(ctx, "INSTALL postgres; LOAD postgres;"); err != nil {
		return fmt.Errorf("failed to load postgres extension: %w", err)
	}

	// Attach Postgres database
	_, err = r.db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", r.pgConnStr))
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

	// Update END_DATE for previous latest version if exists
	_, err = tx.ExecContext(ctx, "UPDATE postgres_db.lrs_catalogs SET END_DATE = CURRENT_DATE WHERE END_DATE IS NULL")
	if err != nil {
		return fmt.Errorf("failed to update previous catalog entries: %w", err)
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

	_, err = tx.ExecContext(ctx, insertQuery, nextVersion, mergedPointFile, mergedSegmentFile, mergedLinestringFile, opts.Author, opts.CommitMsg)
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

	out := &LRSRoute{
		route_id:        routeID,
		latitudeCol:     "LAT",
		longitudeCol:    "LON",
		mValueCol:       "MVAL",
		VertexSeqColumn: "VERTEX_SEQ",
		crs:             "EPSG:4326",
		source_files: &sourceFiles{
			Point:      &pointPath,
			Segment:    &segmentPath,
			LineString: &linestringPath,
		},
	}
	out.setPushDown(true)
	return out, nil
}

// GenerateArcGISToken generates a token for ArcGIS Portal
func (r *LRSRouteRepository) GenerateArcGISToken(ctx context.Context) (string, error) {
	username := os.Getenv("ARCGIS_USER")
	password := os.Getenv("ARCGIS_PASSWORD")

	data := url.Values{}
	data.Set("username", username)
	data.Set("password", password)
	data.Set("f", "json")
	data.Set("expiration", "60")
	// data.Set("client", "requestip")
	data.Set("client", "referer")
	data.Set("referer", "https://sipdjn.binamarga.pu.go.id/")

	req, err := http.NewRequestWithContext(ctx, "POST", r.tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed with status: %d", resp.StatusCode)
	}

	var result struct {
		Token string `json:"token"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if result.Error != nil {
		return "", fmt.Errorf("arcgis error: %s (code %d)", result.Error.Message, result.Error.Code)
	}

	return result.Token, nil
}

// FetchArcGISFeatures fetches GeoJSON features for the given route IDs
func (r *LRSRouteRepository) FetchArcGISFeatures(ctx context.Context, token string, routeIDs []string) ([]byte, error) {
	// Construct WHERE clause: RouteId IN ('id1', 'id2', ...)
	var where string
	if len(routeIDs) == 1 {
		where = fmt.Sprintf("RouteId='%s'", routeIDs[0])
	} else if len(routeIDs) > 1 {
		quotedIDs := make([]string, len(routeIDs))
		for i, id := range routeIDs {
			quotedIDs[i] = fmt.Sprintf("'%s'", id)
		}
		where = fmt.Sprintf("RouteId IN (%s)", strings.Join(quotedIDs, ","))
	} else {
		where = "1=1"
	}

	params := url.Values{}
	params.Set("where", where)
	params.Set("outfields", "LINKID,LINK_NAME,SK_LENGTH") // Including necessary fields for LRSRoute
	params.Set("f", "json")
	params.Set("token", token)
	params.Set("returnGeometry", "true")
	params.Set("returnM", "true")
	params.Set("returnZ", "true")

	fullURL := fmt.Sprintf("%s?%s", r.featureServiceURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("feature request failed with status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}
