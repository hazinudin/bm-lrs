package mvalue

import (
	"bm-lrs/pkg/route"
	"bm-lrs/pkg/route_event"
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/duckdb/duckdb-go/v2"
)

// CalculatePointsMValue calculates the M-Value of points relative to an LRS route.
// It uses DuckDB spatial extension for shortest line and interpolation.
func CalculatePointsMValue(ctx context.Context, lrs route.LRSRouteInterface, points route_event.LRSEvents) (*route_event.LRSEvents, error) {
	c, err := duckdb.NewConnector("", nil)

	if err != nil {
		return nil, err
	}

	defer c.Close()

	conn, err := c.Connect(ctx)
	if err != nil {
		return nil, err
	}

	ar, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow from duckdb: %v", err)
	}

	// Get a single connection to ensure temporary tables are visible across calls
	db := sql.OpenDB(c)
	defer db.Close()

	conn_sql, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sql connection: %v", err)
	}
	defer conn_sql.Close()

	if _, err := conn_sql.ExecContext(ctx, "INSTALL spatial; LOAD spatial;"); err != nil {
		return nil, fmt.Errorf("failed to load spatial extension: %v", err)
	}

	// Register Points as view
	pointsRecords := points.GetRecords()
	if len(pointsRecords) == 0 {
		return nil, fmt.Errorf("points records are empty")
	}

	pointsReader, err := array.NewRecordReader(pointsRecords[0].Schema(), pointsRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to create points record reader: %v", err)
	}
	defer pointsReader.Release()

	releasePoints, err := ar.RegisterView(pointsReader, "points_raw_view")
	if err != nil {
		return nil, fmt.Errorf("failed to register points view: %v", err)
	}
	defer releasePoints()

	if _, err := ar.QueryContext(ctx, "CREATE TEMP TABLE points_table AS SELECT *, row_number() OVER () as point_id FROM points_raw_view"); err != nil {
		return nil, fmt.Errorf("failed to create points table view: %v", err)
	}

	// Create view for LRS line and segments
	if _, err := ar.QueryContext(ctx, fmt.Sprintf("CREATE TEMP TABLE lrs_line_table AS (%s)", lrs.LinestringQuery())); err != nil {
		return nil, fmt.Errorf("failed to create lrs line view: %v", err)
	}

	if _, err := ar.QueryContext(ctx, fmt.Sprintf("CREATE TEMP TABLE lrs_segment_table AS (%s)", lrs.SegmentQuery())); err != nil {
		return nil, fmt.Errorf("failed to create lrs segment view: %v", err)
	}

	// Check if MVAL column exists in input to decide on EXCLUDE
	hasMVAL := false
	for _, f := range pointsRecords[0].Schema().Fields() {
		if f.Name == points.MValueColumn() {
			hasMVAL = true
			break
		}
	}

	excludeClause := "point_id"
	if hasMVAL {
		excludeClause = fmt.Sprintf(`"%s", point_id`, points.MValueColumn())
	}

	// Construct interpolation query
	// Using ST_Point(LAT, LON) to match LinestringQuery convention
	query := fmt.Sprintf(`
	WITH shortest_to_lrs AS (
		SELECT
			a.ROUTEID,
			point_id, 
			ST_ShortestLine(ST_Point("%s", "%s"), linestr) AS shortestline
		FROM points_table a 
		JOIN lrs_line_table b
		ON a.ROUTEID = b.ROUTEID
	),
	point_on_line AS (
		SELECT
			ROUTEID,
			point_id,
			ST_EndPoint(shortestline) AS lambert_vertex,
			ST_Length(shortestline) AS dist_to_line
		FROM shortest_to_lrs
	),
	nearest_vertex AS (
		SELECT 
			a.point_id, 
			a.dist_to_line, 
			a.lambert_vertex AS on_line, 
			b."%s" as mval_start, 
			b."%s1" as mval_end, 
			b."%s" as lat_start, 
			b."%s" as lon_start, 
			b."%s1" as lat_end, 
			b."%s1" as lon_end
		FROM point_on_line a
		INNER JOIN lrs_segment_table b
		ON ST_Y(a.lambert_vertex) <= GREATEST(b."%s", b."%s1") AND ST_Y(a.lambert_vertex) >= LEAST(b."%s", b."%s1")
		AND ST_X(a.lambert_vertex) <= GREATEST(b."%s", b."%s1") AND ST_X(a.lambert_vertex) >= LEAST(b."%s", b."%s1")
		AND a.ROUTEID = b.ROUTEID
	),
	interpolated AS (
		SELECT
			point_id,
			dist_to_line AS dist,
			((mval_end - mval_start) / NULLIF(ST_Distance(ST_Point(lat_start, lon_start), ST_Point(lat_end, lon_end)), 0) * ST_Distance(ST_Point(lat_start, lon_start), on_line)) + mval_start AS m_val
		FROM nearest_vertex
	),
	best_interpolated AS (
		SELECT DISTINCT ON (point_id) * FROM interpolated ORDER BY point_id, dist ASC
	)
	SELECT 
		p.* EXCLUDE (%s), 
		COALESCE(i.m_val, 0) as "%s",
		i.dist as dist_to_line
	FROM points_table p
	LEFT JOIN best_interpolated i ON p.point_id = i.point_id
	ORDER BY p.point_id
	`,
		points.LatitudeColumn(), points.LongitudeColumn(),
		lrs.MValueColumn(), lrs.MValueColumn(),
		lrs.LatitudeColumn(), lrs.LongitudeColumn(),
		lrs.LatitudeColumn(), lrs.LongitudeColumn(),
		lrs.LongitudeColumn(), lrs.LongitudeColumn(), lrs.LongitudeColumn(), lrs.LongitudeColumn(),
		lrs.LatitudeColumn(), lrs.LatitudeColumn(), lrs.LatitudeColumn(), lrs.LatitudeColumn(),
		excludeClause, points.MValueColumn())

	// Debug: check counts
	var pointsCount, lrsLineCount, lrsSegmentCount int
	conn_sql.QueryRowContext(ctx, "SELECT count(*) FROM points_table").Scan(&pointsCount)
	conn_sql.QueryRowContext(ctx, "SELECT count(*) FROM lrs_line_table").Scan(&lrsLineCount)
	conn_sql.QueryRowContext(ctx, "SELECT count(*) FROM lrs_segment_table").Scan(&lrsSegmentCount)

	// Use ar.QueryContext to get a RecordReader back
	outReader, err := ar.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute interpolation query: %v", err)
	}
	defer outReader.Release()

	var outRecs []arrow.RecordBatch
	for outReader.Next() {
		rec := outReader.RecordBatch()
		rec.Retain()
		outRecs = append(outRecs, rec)
	}

	if len(outRecs) == 0 {
		return nil, fmt.Errorf("expected records, got 0. Counts: points=%d, lrs_line=%d, lrs_segment=%d", pointsCount, lrsLineCount, lrsSegmentCount)
	}

	out, err := route_event.NewLRSEvents(outRecs, points.GetCRS())
	if err != nil {
		return nil, err
	}

	return out, nil
}
