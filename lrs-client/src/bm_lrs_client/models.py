from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import polars as pl


@dataclass
class ColumnMapping:
    route_id: str = "ROUTEID"
    latitude: str = "LAT"
    longitude: str = "LON"
    m_value: str = "MVAL"
    distance: str = "dist_to_line"

    def validate(self, table: pa.Table | pl.DataFrame) -> None:
        columns = (
            table.columns if isinstance(table, pl.DataFrame) else table.column_names
        )
        missing = []
        if self.route_id not in columns:
            missing.append(self.route_id)
        if self.latitude not in columns:
            missing.append(self.latitude)
        if self.longitude not in columns:
            missing.append(self.longitude)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    def rename_to_standard(self, df: pl.DataFrame) -> pl.DataFrame:
        rename_map = {}
        if self.route_id != "ROUTEID":
            rename_map[self.route_id] = "ROUTEID"
        if self.latitude != "LAT":
            rename_map[self.latitude] = "LAT"
        if self.longitude != "LON":
            rename_map[self.longitude] = "LON"
        if rename_map:
            return df.rename(rename_map)
        return df

    def rename_from_standard(self, df: pl.DataFrame) -> pl.DataFrame:
        rename_map = {}
        if self.m_value != "MVAL":
            rename_map["MVAL"] = self.m_value
        if self.distance != "dist_to_line":
            rename_map["dist_to_line"] = self.distance
        if rename_map:
            return df.rename(rename_map)
        return df


@dataclass
class LRSResponse:
    table: pa.Table
    num_records: int
    columns_added: list[str]

    def to_polars(self) -> pl.DataFrame:
        return pl.from_arrow(self.table)

    def to_pyarrow(self) -> pa.Table:
        return self.table
