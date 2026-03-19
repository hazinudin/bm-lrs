import unittest
import pyarrow as pa
import polars as pl

from bm_lrs_client.models import ColumnMapping, LRSResponse


class TestColumnMapping(unittest.TestCase):
    def test_default_values(self):
        mapping = ColumnMapping()
        self.assertEqual(mapping.route_id, "ROUTEID")
        self.assertEqual(mapping.latitude, "LAT")
        self.assertEqual(mapping.longitude, "LON")
        self.assertEqual(mapping.m_value, "MVAL")
        self.assertEqual(mapping.distance, "dist_to_line")

    def test_custom_values(self):
        mapping = ColumnMapping(
            route_id="LINKID",
            latitude="LATITUDE",
            longitude="LONGITUDE",
            m_value="MEASURE",
            distance="DIST",
        )
        self.assertEqual(mapping.route_id, "LINKID")
        self.assertEqual(mapping.latitude, "LATITUDE")
        self.assertEqual(mapping.longitude, "LONGITUDE")
        self.assertEqual(mapping.m_value, "MEASURE")
        self.assertEqual(mapping.distance, "DIST")

    def test_validate_polars_success(self):
        mapping = ColumnMapping()
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1", "R2"],
                "LAT": [1.0, 2.0],
                "LON": [100.0, 101.0],
            }
        )
        mapping.validate(df)

    def test_validate_pyarrow_success(self):
        mapping = ColumnMapping()
        table = pa.table(
            {
                "ROUTEID": ["R1", "R2"],
                "LAT": [1.0, 2.0],
                "LON": [100.0, 101.0],
            }
        )
        mapping.validate(table)

    def test_validate_missing_column(self):
        mapping = ColumnMapping()
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1", "R2"],
                "LAT": [1.0, 2.0],
            }
        )
        with self.assertRaises(ValueError) as ctx:
            mapping.validate(df)
        self.assertIn("Missing required columns", str(ctx.exception))

    def test_rename_to_standard_no_change(self):
        mapping = ColumnMapping()
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1"],
                "LAT": [1.0],
                "LON": [100.0],
            }
        )
        result = mapping.rename_to_standard(df)
        self.assertEqual(df.columns, result.columns)

    def test_rename_to_standard_with_change(self):
        mapping = ColumnMapping(route_id="ID", latitude="Y", longitude="X")
        df = pl.DataFrame(
            {
                "ID": ["R1"],
                "Y": [1.0],
                "X": [100.0],
            }
        )
        result = mapping.rename_to_standard(df)
        self.assertEqual(result.columns, ["ROUTEID", "LAT", "LON"])

    def test_rename_from_standard_no_change(self):
        mapping = ColumnMapping()
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1"],
                "MVAL": [1.0],
                "dist_to_line": [0.5],
            }
        )
        result = mapping.rename_from_standard(df)
        self.assertEqual(df.columns, result.columns)

    def test_rename_from_standard_with_change(self):
        mapping = ColumnMapping(m_value="MEASURE", distance="DIST")
        df = pl.DataFrame(
            {
                "ROUTEID": ["R1"],
                "MVAL": [1.0],
                "dist_to_line": [0.5],
            }
        )
        result = mapping.rename_from_standard(df)
        self.assertEqual(result.columns, ["ROUTEID", "MEASURE", "DIST"])


class TestLRSResponse(unittest.TestCase):
    def test_to_polars(self):
        table = pa.table(
            {
                "col1": ["a", "b"],
                "MVAL": [1.0, 2.0],
            }
        )
        response = LRSResponse(table=table, num_records=2, columns_added=["MVAL"])
        result = response.to_polars()
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.columns, ["col1", "MVAL"])

    def test_to_pyarrow(self):
        table = pa.table(
            {
                "col1": ["a", "b"],
                "MVAL": [1.0, 2.0],
            }
        )
        response = LRSResponse(table=table, num_records=2, columns_added=["MVAL"])
        result = response.to_pyarrow()
        self.assertIsInstance(result, pa.Table)
        self.assertEqual(result.column_names, ["col1", "MVAL"])


if __name__ == "__main__":
    unittest.main()
