import json

from .models import ColumnMapping


def validate_crs(crs: str) -> bool:
    return (
        crs.startswith("EPSG:")
        or crs.startswith("PROJCS=")
        or crs.startswith("GEOGCS=")
    )


def build_request_message(crs: str, column_mapping: ColumnMapping) -> bytes:
    message = {
        "operation": "calculate_m_value",
        "crs": crs,
        "column_mappings": {
            "route_id": column_mapping.route_id,
            "latitude": column_mapping.latitude,
            "longitude": column_mapping.longitude,
        },
    }
    return json.dumps(message).encode("utf-8")


def table_to_record_batch(table) -> list:
    batches = table.to_batches(max_chunksize=table.num_rows)
    return batches
