package flight

import (
	"bm-lrs/pkg/route"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDoExchange(t *testing.T) {
	if err := godotenv.Load("../../.env"); err != nil {
		t.Log("Warning: .env file not found")
	}

	testPgConnStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s",
		os.Getenv("DB_NAME"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
	)

	// Create temp directory for output files
	tempDir, err := os.MkdirTemp("", "lrs_flight_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Ensure tempDir is absolute
	tempDir, _ = filepath.Abs(tempDir)

	os.Setenv("LRS_DATA_DIR", tempDir)
	defer os.Unsetenv("LRS_DATA_DIR")

	ctx := context.Background()

	// Setup DuckDB connector
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	// Initialize spatial and catalog
	_, err = db.ExecContext(ctx, "INSTALL spatial; LOAD spatial;")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSTALL postgres; LOAD postgres;")
	require.NoError(t, err)

	// Clean up stale catalog entries from previous runs to avoid IO Errors with temp files
	_, err = db.ExecContext(ctx, fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS postgres_db (TYPE POSTGRES)", testPgConnStr))
	if err == nil {
		db.ExecContext(ctx, "DELETE FROM postgres_db.lrs_catalogs")
	}

	repo := route.NewLRSRouteRepository(connector, testPgConnStr, db)

	// Read test GeoJSON data
	jsonBytes, err := os.ReadFile("../route/testdata/lrs_01001.json")
	require.NoError(t, err)

	err = repo.SyncFromGeoJSON(ctx, jsonBytes, route.SyncOptions{Author: "TEST", CommitMsg: "FLIGHT TEST"})
	require.NoError(t, err)

	// Start server on static port 8080 for test
	addr := "127.0.0.1:8080"

	server := flight.NewServerWithMiddleware(nil, grpc.Creds(insecure.NewCredentials()))
	server.RegisterFlightService(NewLRSFlightServer(repo))

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Server panicked: %v\n", r)
			}
		}()
		fmt.Printf("Server starting on %s...\n", addr)
		if err := server.Init(addr); err != nil {
			fmt.Printf("Server Init failed: %v\n", err)
			return
		}
		if err := server.Serve(); err != nil {
			fmt.Printf("Server Serve failed: %v\n", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Create client
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	// Prepare data to send
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ROUTEID", Type: arrow.BinaryTypes.String},
		{Name: "LAT", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LON", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).Append("01001")
	builder.Field(1).(*array.Float64Builder).Append(3.14)   // Sample lat
	builder.Field(2).(*array.Float64Builder).Append(101.68) // Sample lon

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Perform DoExchange
	stream, err := client.DoExchange(ctx)
	require.NoError(t, err)

	// Send operation metadata in first message
	err = stream.Send(&flight.FlightData{
		AppMetadata: []byte(`{"operation": "calculate_m_value", "crs": "EPSG:4326"}`),
	})
	require.NoError(t, err)

	// Send data
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	err = writer.Write(rec)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Close send side
	err = stream.CloseSend()
	require.NoError(t, err)

	// Receive results
	reader, err := flight.NewRecordReader(stream)
	require.NoError(t, err)
	defer reader.Release()

	var results []arrow.RecordBatch
	for reader.Next() {
		res := reader.RecordBatch()
		res.Retain()
		results = append(results, res)
	}

	assert.Len(t, results, 1)
	assert.Equal(t, int64(1), results[0].NumRows())

	// Check if MVAL and dist_to_line columns exist in result
	resultSchema := results[0].Schema()
	_, found := resultSchema.FieldsByName("MVAL")
	assert.True(t, found)
	_, found = resultSchema.FieldsByName("dist_to_line")
	assert.True(t, found)

	for _, r := range results {
		r.Release()
	}
}
