import unittest
import csv
import os
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

CSV_REPORT = "catalog_test_report.csv"
TEST_RESULTS = []


def log_result(test_case, outcome, message=""):
    TEST_RESULTS.append({
        "Test Name": test_case,
        "Status": "PASS" if outcome else "FAIL",
        "Message": message
    })


def write_report():
    with open(CSV_REPORT, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Test Name", "Status", "Message"])
        writer.writeheader()
        writer.writerows(TEST_RESULTS)


class TestCatalogFunctions(unittest.TestCase):

    def run(self, result=None):
        """Override run to capture results per test."""
        test_name = self._testMethodName
        try:
            super().run(result)
            if result.wasSuccessful():
                log_result(test_name, True)
            else:
                # Look for any failures specific to this test
                failure_msgs = [str(e[1]) for e in result.failures + result.errors if e[0]._testMethodName == test_name]
                log_result(test_name, False, "; ".join(failure_msgs))
        except Exception as e:
            log_result(test_name, False, str(e))
            raise


    def test_to_dict(self):
        catalog = Catalog([
            CatalogEntry(tap_stream_id='a', schema=Schema(), metadata=[]),
            CatalogEntry(tap_stream_id='b', schema=Schema(), metadata=[])
        ])
        dict_version = catalog.to_dict()
        self.assertIn("streams", dict_version)

    def test_from_dict(self):
        dict_data = {
            "streams": [{
                "tap_stream_id": "a",
                "stream": "a",
                "schema": {
                    "type": "object",
                    "properties": {}
                },
                "metadata": [],
                "key_properties": []
            }]
        }
        catalog = Catalog.from_dict(dict_data)
        self.assertEqual(len(catalog.streams), 1)

    def test_get_stream(self):
        catalog = Catalog([
            CatalogEntry(tap_stream_id='x'),
            CatalogEntry(tap_stream_id='y')
        ])
        found = next((s for s in catalog.streams if s.tap_stream_id == 'y'), None)
        self.assertIsNotNone(found)
        self.assertEqual(found.tap_stream_id, 'y')


if __name__ == '__main__':
    unittest.main(exit=False)
    write_report()
    print(f"\n CSV test report saved as: {CSV_REPORT}")
