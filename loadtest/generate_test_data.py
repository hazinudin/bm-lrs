#!/usr/bin/env python3
import json
import random
import sys

try:
    import pyarrow.parquet as pq
except ImportError:
    print("Error: pyarrow not installed. Run: pip install pyarrow")
    sys.exit(1)


def generate_test_data(
    parquet_path: str, num_samples: int = 500, output_path: str = "test_data.json"
):
    print(f"Loading parquet file: {parquet_path}")
    table = pq.read_table(parquet_path)

    df = table.to_pandas()
    print(f"Total rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")

    linkid_col = None
    lat_col = None
    lon_col = None

    for col in df.columns:
        col_lower = col.lower()
        if "linkid" in col_lower or "link_id" in col_lower:
            linkid_col = col
        if "lat" in col_lower and (
            "sta" in col_lower or "from" in col_lower or "to" in col_lower
        ):
            if lat_col is None:
                lat_col = col
        if "lon" in col_lower or "lng" in col_lower:
            if lon_col is None:
                lon_col = col

    if linkid_col is None:
        for col in df.columns:
            if "id" in col.lower():
                linkid_col = col
                break

    print(f"Using columns: LINKID={linkid_col}, LAT={lat_col}, LON={lon_col}")

    df_unique = df.drop_duplicates(subset=[linkid_col])
    sample_size = min(num_samples, len(df_unique))
    df_sample = df_unique.sample(n=sample_size, random_state=42)

    features = []
    for _, row in df_sample.iterrows():
        linkid = str(row[linkid_col])
        lat = float(row[lat_col]) if lat_col else 0.0
        lon = float(row[lon_col]) if lon_col else 0.0

        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"LINKID": linkid},
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    print(f"Generated {len(features)} test payloads")
    print(f"Writing to: {output_path}")
    with open(output_path, "w") as f:
        json.dump(geojson, f, indent=2)

    print(f"Done! Generated {output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate test data for k6 load testing"
    )
    parser.add_argument(
        "--parquet",
        default="../scratch/rni_2_2025_original.parquet",
        help="Path to parquet file",
    )
    parser.add_argument(
        "--samples", type=int, default=500, help="Number of unique LINKIDs to sample"
    )
    parser.add_argument("--output", default="test_data.json", help="Output JSON file")

    args = parser.parse_args()
    generate_test_data(args.parquet, args.samples, args.output)
