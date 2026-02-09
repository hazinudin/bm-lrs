package main

import (
	"bm-lrs/pkg/api"
	"bm-lrs/pkg/flight"
	"bm-lrs/pkg/route"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

	// Database setup (Postgres)
	pgConnStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s",
		os.Getenv("DB_NAME"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
	)

	// DuckDB setup
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		log.Fatal("Failed to create DuckDB connector:", err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	// Repository setup
	repo := route.NewLRSRouteRepository(connector, pgConnStr, db)

	// Start REST API server in goroutine
	restPort := 8080
	apiServer := api.NewAPIServer(repo, restPort)
	go func() {
		if err := apiServer.Start(); err != nil {
			log.Printf("REST API server error: %v", err)
		}
	}()

	// Start Flight server
	flightPort := 50051
	if err := flight.StartFlightServer(repo, flightPort); err != nil {
		log.Fatal("Flight server failed:", err)
	}
}
