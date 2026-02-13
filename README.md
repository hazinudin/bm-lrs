# Bina Marga Linear Referencing System Service

A high-performance **Linear Referencing System (LRS)** service built in Go, designed to ingest, process, and serve road network route data from Indonesia's Directorate General of Highways (Bina Marga). The system combines **DuckDB** as an analytical engine, **Apache Parquet** for columnar storage, **Apache Arrow** for zero-copy data exchange, and **PostgreSQL** for version-controlled catalog management.

---

## Architecture Overview

```
┌──────────────┐        ┌──────────────────────────────────────────────────┐
│  ArcGIS      │        │                     LRS Server                   │
│  Portal      │◄─────► │                                                  │
│  (Data Src)  │ ESRI   │  ┌────────────┐   ┌─────────────────────────┐    │
└──────────────┘ JSON   │  │  REST API  │   │   Arrow Flight (gRPC)   │    │
                        │  │  :8080     │   │   :50051                │    │
                        │  └─────┬──────┘   └──────────┬──────────────┘    │
                        │        │                     │                   │
                        │        └──────────┬──────────┘                   │
                        │                   ▼                              │
                        │       ┌───────────────────────┐                  │
                        │       │   LRS Route Repository│                  │
                        │       │   (DuckDB Engine)     │                  │
                        │       └─────┬──────────┬──────┘                  │
                        │             │          │                         │
                        │    ┌────────▼──┐  ┌────▼──────────┐              │
                        │    │  Parquet  │  │  PostgreSQL   │              │
                        │    │  Files    │  │  Catalog      │              │
                        │    │  (Data)   │  │  (Metadata)   │              │
                        │    └───────────┘  └───────────────┘              │
                        └──────────────────────────────────────────────────┘
```

---

## How it Works

### The Data Pipeline

**BM-LRS** solves the challenge of managing large-scale road route geometry with linear referencing (M-Values) by separating **compute**, **storage**, and **cataloging** into distinct layers:

| Layer | Technology | Purpose |
|---|---|---|
| **Compute Engine** | DuckDB | Analytical SQL queries: spatial joins, M-Value interpolation, CRS projection, segment generation |
| **Data Storage** | Apache Parquet | Columnar, compressed route data (Points, Segments, LineStrings) persisted on disk |
| **Data Exchange** | Apache Arrow | Zero-copy in-memory columnar format for high-throughput client-server communication |
| **Catalog** | PostgreSQL | Version-controlled metadata — tracks which Parquet files are active, with start/end dates, authors, and commit messages |

### Why This Combination?

1. **DuckDB** acts as an embedded analytical engine — no separate database server required. It runs spatial queries (via the `spatial` extension) directly against Parquet files and Arrow RecordBatches, enabling SQL-based geometry operations like `ST_ShortestLine`, `ST_Transform`, and `ST_MakeLine` without loading entire datasets into memory.

2. **Parquet** files provide efficient, compressed columnar storage for route geometry. Routes are merged into shared Parquet files (Point, Segment, LineString) with **predicate pushdown** — DuckDB can filter by `ROUTEID` directly at the file scan level, avoiding full-file reads.

3. **Apache Arrow** enables zero-copy data exchange between DuckDB and the application layer. Route data flows as Arrow RecordBatches through the entire pipeline — from DuckDB query results, through M-Value calculation, to client responses via Arrow Flight — without serialization overhead.

4. **PostgreSQL** serves as a lightweight catalog layer, tracking Parquet file versions with a `lrs_catalogs` table. Each sync operation creates a new catalog entry with versioning, enabling rollback and audit trails (author, commit message, start/end dates).

---

## Features

### ArcGIS Data Sync
- Fetches LRS route geometry from Bina Marga's ArcGIS Portal Feature Service
- Concurrent worker pool (4 workers) for parallel feature fetching with pagination
- Parses ESRI JSON geometry (polylines with M-values) into Arrow RecordBatches
- Supports both full sync (`SyncAll`) and selective sync by Route IDs

### M-Value Calculation
- Calculates the M-Value (linear measure) for arbitrary point events relative to LRS routes
- Uses DuckDB's spatial extension for:
  - **Shortest line computation** (`ST_ShortestLine`) to find the nearest point on the route
  - **Linear interpolation** between route segments to determine the M-Value at the projected point
  - **Distance calculation** from input points to the LRS route
- Accepts GeoJSON input and returns GeoJSON with computed `MVAL` and `dist_to_line` attributes

### Parquet Merge & Version Control
- Route data is consolidated into shared Parquet files (Point, Segment, LineString)
- On each sync, new routes are **merged** with existing data using DuckDB's UNION queries
- Previous route versions are automatically replaced (by `ROUTEID` deduplication)
- Catalog entries in PostgreSQL track file versions with temporal validity (`START_DATE` / `END_DATE`)
- Conditional materialization: queries are materialized to temporary Parquet files for large batches (>1000 routes) to manage memory

### Dual API Interface
- **REST API** (`:8080`) — accepts GeoJSON `POST` requests for M-Value calculation
  - `POST /api/v1/calculate-mvalue` — compute M-Values for point events
  - `GET /health` — health check
- **Apache Arrow Flight** (`:50051`) — high-performance gRPC streaming for bulk Arrow RecordBatch exchange via `DoExchange`
  - Clients send Arrow RecordBatches and receive computed results as Arrow RecordBatches
  - No JSON serialization overhead — ideal for Python/R analytics clients

### CRS Projection
- Automatic coordinate reference system transformation using DuckDB's `ST_Transform`
- Supports projection between EPSG:4326 (WGS84) and Indonesia Lambert Conformal Conic
- Input data is transparently reprojected before spatial calculations

---

## Project Structure

```
bm-lrs/
├── cmd/
│   ├── lrs-server/        # Main server: REST API + Arrow Flight
│   └── init/              # One-shot repository initialization & sync
├── pkg/
│   ├── api/               # REST API handlers (GeoJSON M-Value endpoint)
│   ├── flight/            # Arrow Flight server (gRPC DoExchange)
│   ├── geom/              # Geometry interfaces and types
│   ├── mvalue/            # M-Value calculation engine (DuckDB spatial queries)
│   ├── projection/        # CRS projection transformation
│   ├── route/             # Core: LRS Route, Batch, Repository, ESRI JSON parsing
│   └── route_event/       # LRS Events: GeoJSON ↔ Arrow RecordBatch conversion
├── client/                # Python Flight client
├── Dockerfile.server      # Server deployment image
├── Dockerfile.init        # Repository init image
├── go.mod
└── go.sum
```

---

## Getting Started

### Prerequisites

- **Go 1.25+**
- **PostgreSQL** (for catalog storage)
- **ArcGIS Portal credentials** (for data sync)

### Environment Variables

Create a `.env` file:

```env
DB_NAME=lrs
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=127.0.0.1
ARCGIS_USER=your_arcgis_user
ARCGIS_PASSWORD=your_arcgis_password
LRS_DATA_DIR=./data
```

### Build

```bash
# Build with DuckDB Arrow integration
go build -tags duckdb_arrow -o lrs-server ./cmd/lrs-server
go build -tags duckdb_arrow -o lrs-init ./cmd/init
```

### Run

```bash
# 1. Initialize the repository (fetches all routes from ArcGIS and creates Parquet files)
./lrs-init

# 2. Start the server
./lrs-server
```

---

## Docker Deployment

### Build Images

```bash
docker build -f Dockerfile.server -t bm-lrs-server .
docker build -f Dockerfile.init   -t bm-lrs-init .
```

### Run

```bash
# Initialize the repository
docker run --rm \
  --env-file .env \
  -e DB_HOST=host.docker.internal \
  -v $(pwd)/data:/data \
  bm-lrs-init

# Start the server
docker run -d \
  --env-file .env \
  -e DB_HOST=host.docker.internal \
  -v $(pwd)/data:/data \
  -p 8080:8080 -p 50051:50051 \
  bm-lrs-server
```

> **Note:** Use `DB_HOST=host.docker.internal` to reach the host machine's PostgreSQL from within Docker.

---

## API Usage

### REST API — Calculate M-Value

```bash
curl -X POST http://localhost:8080/api/v1/calculate-mvalue?crs=EPSG:4326 \
  -H "Content-Type: application/json" \
  -d '{
    "type": "FeatureCollection",
    "features": [
      {
        "type": "Feature",
        "geometry": {
          "type": "Point",
          "coordinates": [106.845, -6.208]
        },
        "properties": {
          "ROUTEID": "001.11.K"
        }
      }
    ]
  }'
```

**Response:** GeoJSON FeatureCollection with added `MVAL` and `dist_to_line` properties.

### Arrow Flight — Python Client

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:50051")

# Send Arrow RecordBatch via DoExchange
descriptor = flight.FlightDescriptor.for_command(
    b'{"operation": "calculate_m_value", "crs": "EPSG:4326"}'
)

writer, reader = client.do_exchange(descriptor)
writer.write_batch(record_batch)  # Send your data
writer.done_writing()

for batch in reader:  # Receive computed M-Values as Arrow RecordBatches
    print(batch.data)
```

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Language | Go | 1.25 |
| Analytical Engine | DuckDB | v2.5.0 (via duckdb-go) |
| Columnar Format | Apache Arrow | v18.4.1 (arrow-go) |
| File Storage | Apache Parquet | via arrow-go |
| Catalog Database | PostgreSQL | (via DuckDB postgres extension) |
| RPC Framework | gRPC | v1.75.0 |
| Spatial Operations | DuckDB Spatial Extension | — |
