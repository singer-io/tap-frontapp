import os
import unittest
import csv
import json
from singer.schema import Schema

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), '../tap_frontapp/schemas')
CSV_REPORT = os.path.join(os.path.dirname(__file__), 'schema_test_report.csv')


class TestFrontAppSchemas(unittest.TestCase):
    results = []

    def log_result(self, test_name, schema_file, status, message=""):
        self.results.append({
            "Test Case": test_name,
            "Schema File": schema_file,
            "Status": status,
            "Message": message
        })

    def test_schema_files_are_valid(self):
        for filename in os.listdir(SCHEMA_DIR):
            if filename.endswith('.json'):
                schema_path = os.path.join(SCHEMA_DIR, filename)
                try:
                    with open(schema_path, 'r', encoding='utf-8') as f:
                        raw_dict = json.load(f)
                    raw_schema = Schema.from_dict(raw_dict)
                    schema_dict = raw_schema.to_dict()
                    reconstructed = Schema.from_dict(schema_dict)
                    self.assertEqual(raw_schema, reconstructed)
                    self.log_result("Schema Round-trip", filename, "PASS")
                except Exception as e:
                    self.log_result("Schema Round-trip", filename, "FAIL", str(e))
                    raise

    def test_required_structure(self):
        for filename in os.listdir(SCHEMA_DIR):
            if filename.endswith('.json'):
                schema_path = os.path.join(SCHEMA_DIR, filename)
                try:
                    with open(schema_path, 'r', encoding='utf-8') as f:
                        raw_dict = json.load(f)
                    raw_schema = Schema.from_dict(raw_dict)
                    schema_dict = raw_schema.to_dict()
                    self.assertIn('type', schema_dict)
                    self.assertIn('properties', schema_dict)
                    self.assertEqual(schema_dict['type'], ['null', 'object'])
                    self.log_result("Required Fields", filename, "PASS")
                except Exception as e:
                    self.log_result("Required Fields", filename, "FAIL", str(e))
                    raise

    @classmethod
    def tearDownClass(cls):
        with open(CSV_REPORT, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=["Test Case", "Schema File", "Status", "Message"])
            writer.writeheader()
            writer.writerows(cls.results)


if __name__ == '__main__':
    unittest.main()
