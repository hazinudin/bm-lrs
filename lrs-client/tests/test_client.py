import unittest
from unittest.mock import patch, MagicMock
import pyarrow as pa
import pyarrow.flight as pa_flight
import polars as pl

from bm_lrs_client import LRSClient, ColumnMapping
from bm_lrs_client.exceptions import LRSConnectionError, LRSValidationError


class TestLRSClient(unittest.TestCase):
    def test_init(self):
        client = LRSClient("grpc://127.0.0.1:50051")
        self.assertEqual(client._location, "grpc://127.0.0.1:50051")
        self.assertIsNone(client._client)

    def test_invalid_crs(self):
        client = LRSClient("grpc://127.0.0.1:50051")
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1"],
                "LAT": [1.0],
                "LON": [100.0],
            }
        )
        with self.assertRaises(LRSValidationError) as ctx:
            client.calculate_m_value(df, ColumnMapping(), crs="invalid")
        self.assertIn("Invalid CRS format", str(ctx.exception))

    def test_missing_columns(self):
        client = LRSClient("grpc://127.0.0.1:50051")
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1"],
                "LAT": [1.0],
            }
        )
        with self.assertRaises(ValueError) as ctx:
            client.calculate_m_value(df, ColumnMapping(), crs="EPSG:4326")
        self.assertIn("Missing required columns", str(ctx.exception))

    def test_calculate_m_value_polars_input(self):
        client = LRSClient("grpc://127.0.0.1:50051")
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1"],
                "LAT": [1.0],
                "LON": [100.0],
            }
        )

        mock_writer = MagicMock()
        mock_reader = MagicMock()
        record_batch = pa.RecordBatch.from_pydict(
            {
                "point_id": [0],
                "ROUTEID": ["R1"],
                "LAT": [1.0],
                "LON": [100.0],
                "MVAL": [123.5],
                "dist_to_line": [0.1],
            }
        )
        flight_chunk = MagicMock()
        flight_chunk.data = record_batch
        flight_chunk.app_metadata = None
        mock_reader.__iter__ = lambda self: iter([flight_chunk])

        with patch("bm_lrs_client.client.pa_flight.FlightClient") as mock_flight_client:
            mock_instance = MagicMock()
            mock_instance.do_exchange.return_value = (mock_writer, mock_reader)
            mock_flight_client.return_value = mock_instance

            result = client.calculate_m_value(df, ColumnMapping(), crs="EPSG:4326")

            self.assertIsInstance(result, pl.DataFrame)
            self.assertIn("MVAL", result.columns)
            self.assertIn("dist_to_line", result.columns)

    def test_custom_column_mapping_rename(self):
        client = LRSClient("grpc://127.0.0.1:50051")
        df = pl.DataFrame(
            {
                "LINKID": ["R1"],
                "Y": [1.0],
                "X": [100.0],
            }
        )

        mock_writer = MagicMock()
        mock_reader = MagicMock()
        record_batch = pa.RecordBatch.from_pydict(
            {
                "point_id": [0],
                "LINKID": ["R1"],
                "Y": [1.0],
                "X": [100.0],
                "MEASURE": [123.5],
                "DIST": [0.1],
            }
        )
        flight_chunk = MagicMock()
        flight_chunk.data = record_batch
        flight_chunk.app_metadata = None
        mock_reader.__iter__ = lambda self: iter([flight_chunk])

        with patch("bm_lrs_client.client.pa_flight.FlightClient") as mock_flight_client:
            mock_instance = MagicMock()
            mock_instance.do_exchange.return_value = (mock_writer, mock_reader)
            mock_flight_client.return_value = mock_instance

            mapping = ColumnMapping(
                route_id="LINKID",
                latitude="Y",
                longitude="X",
                m_value="MEASURE",
                distance="DIST",
            )
            result = client.calculate_m_value(df, mapping, crs="EPSG:4326")

            self.assertIsInstance(result, pl.DataFrame)
            self.assertIn("MEASURE", result.columns)
            self.assertIn("DIST", result.columns)
            self.assertNotIn("MVAL", result.columns)

    def test_context_manager(self):
        with patch("bm_lrs_client.client.pa_flight.FlightClient") as mock_client:
            with LRSClient("grpc://127.0.0.1:50051") as client:
                mock_client.assert_called_once_with("grpc://127.0.0.1:50051")

    def test_close(self):
        client = LRSClient("grpc://127.0.0.1:50051")
        mock_flight = MagicMock()
        client._client = mock_flight
        client.close()
        mock_flight.close.assert_called_once()
        self.assertIsNone(client._client)


if __name__ == "__main__":
    unittest.main()
