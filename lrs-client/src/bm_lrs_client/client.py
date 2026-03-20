from typing import Union

import pyarrow.flight as pa_flight
import pyarrow as pa
import polars as pl
import pandas as pd

from .exceptions import LRSConnectionError, LRSValidationError, LRSServerError
from .models import ColumnMapping, LRSResponse
from .utils import build_request_message, table_to_record_batch, validate_crs


class LRSClient:
    def __init__(self, location: str) -> None:
        self._location = location
        self._client: pa_flight.FlightClient | None = None
        self._chunk_size = 4096

    def connect(self) -> None:
        try:
            self._client = pa_flight.FlightClient(self._location)
        except Exception as e:
            raise LRSConnectionError(
                f"Failed to connect to LRS server at {self._location}: {e}"
            )

    def calculate_m_value(
        self,
        df: Union[pl.DataFrame, pa.Table, pd.DataFrame],
        column_mapping: ColumnMapping | None = None,
        crs: str = "EPSG:4326",
    ) -> Union[pl.DataFrame, pa.Table]:
        if column_mapping is None:
            column_mapping = ColumnMapping()

        if not validate_crs(crs):
            raise LRSValidationError(f"Invalid CRS format: {crs}")

        if isinstance(df, pl.DataFrame):
            df = column_mapping.rename_to_standard(df)
            table = df.to_arrow()
            input_is_polars = True
        elif isinstance(df, pl.DataFrame):
            df = pl.from_pandas(df)
            df = column_mapping.rename_to_standard(df)
            table = df.to_arrow()
            input_is_polars = True # Currently only returns polars dataframe.
        else:
            table = df
            input_is_polars = False

        ColumnMapping().validate(table)

        if self._client is None:
            self.connect()

        try:
            descriptor = pa_flight.FlightDescriptor.for_command(
                build_request_message(crs, column_mapping)
            )

            options = pa_flight.FlightCallOptions()
            writer, reader = self._client.do_exchange(descriptor, options=options)

            input_schema = pa.schema(
                [
                    pa.field("ROUTEID", pa.string()),
                    pa.field("LAT", pa.float64()),
                    pa.field("LON", pa.float64()),
                ]
            )

            table = table.select(["ROUTEID", "LAT", "LON"])
            table = table.cast(input_schema)
            writer.begin(input_schema)

            total_rows = table.num_rows
            for offset in range(0, total_rows, self._chunk_size):
                chunk_end = min(offset + self._chunk_size, total_rows)
                chunk_table = table.slice(offset, chunk_end - offset)
                chunk_batches = chunk_table.to_batches()
                for batch in chunk_batches:
                    writer.write(batch)

            writer.done_writing()

            result_chunks: list[pa.RecordBatch] = []
            for chunk in reader:
                if chunk.data is not None:
                    result_chunks.append(chunk.data)

            writer.close()

            if not result_chunks:
                raise LRSServerError("Server returned empty response")

            result_table = pa.Table.from_batches(result_chunks)

            response = LRSResponse(
                table=result_table,
                num_records=result_table.num_rows,
                columns_added=[column_mapping.m_value, column_mapping.distance],
            )

            if input_is_polars:
                result = response.to_polars()
                return column_mapping.rename_from_standard(result)
            return response.to_pyarrow()

        except LRSValidationError:
            raise
        except Exception as e:
            raise LRSServerError(f"Server error during M-value calculation: {e}")

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> "LRSClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
