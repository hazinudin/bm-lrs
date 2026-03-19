# BM-LRS Client

Python client for BM-LRS server via Arrow Flight.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from bm_lrs_client import LRSClient, ColumnMapping
import polars as pl

client = LRSClient("grpc://127.0.0.1:50051")

df = pl.read_parquet("events.parquet")
result = client.calculate_m_value(
    df=df,
    column_mapping=ColumnMapping(),
    crs="EPSG:4326"
)
```

## API Reference

### LRSClient

- `__init__(location: str)` - Initialize client with server location
- `calculate_m_value(df, column_mapping, crs)` - Calculate M-Value for route events
- `close()` - Close the connection
- Context manager support

### ColumnMapping

| Field | Default | Description |
|-------|---------|-------------|
| `route_id` | "ROUTEID" | Route identifier column |
| `latitude` | "LAT" | Latitude column |
| `longitude` | "LON" | Longitude column |
| `m_value` | "MVAL" | Output M-value column |
| `distance` | "dist_to_line" | Output distance column |

## Testing

```bash
cd lrs-client
python -m unittest discover -s tests
```

## Running the Server

```bash
cd pkg/server
go build -tags=duckdb_arrow -o server .
./server
```
