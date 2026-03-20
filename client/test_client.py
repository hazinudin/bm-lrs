import time
import polars as pl
from bm_lrs_client import LRSClient, ColumnMapping

# Read the parquet file
df = pl.read_parquet("scratch/rni_2_2025_original.parquet")
print(f"Loaded {len(df)} rows")
print(f"Columns: {df.columns}")

# Connect to the LRS server and calculate M values
client = LRSClient("grpc://127.0.0.1:50051")
start_time = time.time()
result = client.calculate_m_value(
    df, 
    ColumnMapping(
        route_id="LINKID",
        latitude='TO_STA_LAT',
        longitude='TO_STA_LON',
    ), 
    crs="EPSG:4326"
)
end_time = time.time()

print(f"\nResult: {len(result)} rows")
print(f"Columns: {result.columns}")
print(result.head())
print(f"\nTotal time taken: {end_time - start_time:.2f} seconds")
