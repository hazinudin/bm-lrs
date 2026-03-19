import unittest
from bm_lrs_client.utils import validate_crs, build_request_message
from bm_lrs_client.models import ColumnMapping


class TestUtils(unittest.TestCase):
    def test_validate_crs_epsg(self):
        self.assertTrue(validate_crs("EPSG:4326"))
        self.assertTrue(validate_crs("EPSG:3857"))

    def test_validate_crs_wkt(self):
        self.assertTrue(validate_crs('PROJCS=["test",...]'))
        self.assertTrue(validate_crs('GEOGCS=["test",...]'))

    def test_validate_crs_invalid(self):
        self.assertFalse(validate_crs("invalid"))
        self.assertFalse(validate_crs(""))

    def test_build_request_message(self):
        mapping = ColumnMapping()
        message = build_request_message("EPSG:4326", mapping)
        self.assertIsInstance(message, bytes)
        import json

        parsed = json.loads(message.decode("utf-8"))
        self.assertEqual(parsed["operation"], "calculate_m_value")
        self.assertEqual(parsed["crs"], "EPSG:4326")
        self.assertEqual(parsed["column_mappings"]["route_id"], "ROUTEID")


if __name__ == "__main__":
    unittest.main()
