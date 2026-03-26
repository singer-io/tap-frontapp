"""Unit tests for tap_frontapp.schemas module."""

import unittest
from unittest.mock import patch, MagicMock

from tap_frontapp.schemas import (
    IDS,
    STATIC_SCHEMA_STREAM_IDS,
    PK_FIELDS,
    normalize_fieldname,
    load_schema,
    load_and_write_schema,
    get_schemas,
)


class TestNormalizeFieldname(unittest.TestCase):
    """Tests for normalize_fieldname function."""

    def test_lowercase_conversion(self):
        """Test that field names are lowercased."""
        self.assertEqual(normalize_fieldname("FirstName"), "firstname")

    def test_spaces_replaced_with_underscore(self):
        """Test that spaces are replaced with underscores."""
        self.assertEqual(normalize_fieldname("first name"), "first_name")

    def test_hyphens_replaced_with_underscore(self):
        """Test that hyphens are replaced with underscores."""
        self.assertEqual(normalize_fieldname("first-name"), "first_name")

    def test_special_chars_removed(self):
        """Test that special characters other than alphanumeric/underscore are removed."""
        self.assertEqual(normalize_fieldname("field@name!"), "fieldname")

    def test_already_normalized(self):
        """Test that already-normalized names pass through unchanged."""
        self.assertEqual(normalize_fieldname("already_normalized"), "already_normalized")

    def test_numbers_preserved(self):
        """Test that numbers in field names are preserved."""
        self.assertEqual(normalize_fieldname("field_123"), "field_123")

    def test_empty_string(self):
        """Test that empty string returns empty string."""
        self.assertEqual(normalize_fieldname(""), "")

    def test_mixed_case_with_spaces_and_specials(self):
        """Test complex normalization scenario."""
        self.assertEqual(normalize_fieldname("Avg First-Response Time!"), "avg_first_response_time")


class TestStaticConstants(unittest.TestCase):
    """Tests for module-level constants."""

    def test_static_schema_stream_ids_contains_all_six_streams(self):
        """Test that STATIC_SCHEMA_STREAM_IDS has exactly 6 entries."""
        self.assertEqual(len(STATIC_SCHEMA_STREAM_IDS), 6)

    def test_static_schema_stream_ids_contains_expected_streams(self):
        """Test that all expected stream IDs are present."""
        expected = {
            IDS.ACCOUNTS_TABLE,
            IDS.CHANNELS_TABLE,
            IDS.INBOXES_TABLE,
            IDS.TAGS_TABLE,
            IDS.TEAMMATES_TABLE,
            IDS.TEAMS_TABLE,
        }
        self.assertEqual(set(STATIC_SCHEMA_STREAM_IDS), expected)

    def test_pk_fields_defined_for_all_streams(self):
        """Test that PK_FIELDS has entries for every stream."""
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            self.assertIn(stream_id, PK_FIELDS, f"PK_FIELDS missing entry for '{stream_id}'")

    def test_pk_fields_contain_required_keys(self):
        """Test that each stream's PK fields include the four required keys."""
        required_pks = {"analytics_date", "analytics_range", "report_id", "metric_id"}
        for stream_id, pks in PK_FIELDS.items():
            self.assertEqual(
                set(pks),
                required_pks,
                f"Stream '{stream_id}' has unexpected PK fields: {pks}",
            )


class TestLoadSchema(unittest.TestCase):
    """Tests for load_schema function."""

    def test_load_schema_returns_dict(self):
        """Test that load_schema returns a dictionary for a valid stream ID."""
        schema = load_schema("teams_table")
        self.assertIsInstance(schema, dict)

    def test_load_schema_contains_properties(self):
        """Test that the loaded schema contains a 'properties' key."""
        schema = load_schema("accounts_table")
        self.assertIn("properties", schema)

    def test_load_schema_for_each_stream(self):
        """Test that all static streams have loadable schemas."""
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            with self.subTest(stream_id=stream_id):
                schema = load_schema(stream_id)
                self.assertIsInstance(schema, dict)
                self.assertIn("properties", schema)

    def test_load_schema_missing_stream_raises(self):
        """Test that loading a non-existent schema raises an error."""
        with self.assertRaises(Exception):
            load_schema("nonexistent_stream_xyz")


class TestLoadAndWriteSchema(unittest.TestCase):
    """Tests for load_and_write_schema function."""

    @patch("tap_frontapp.schemas.singer.write_schema")
    def test_load_and_write_schema_calls_singer_write_schema(self, mock_write_schema):
        """Test that load_and_write_schema calls singer.write_schema."""
        load_and_write_schema("teams_table")
        mock_write_schema.assert_called_once()

    @patch("tap_frontapp.schemas.singer.write_schema")
    def test_load_and_write_schema_passes_correct_stream_id(self, mock_write_schema):
        """Test that the correct stream ID is passed to singer.write_schema."""
        load_and_write_schema("channels_table")
        call_args = mock_write_schema.call_args[0]
        self.assertEqual(call_args[0], "channels_table")

    @patch("tap_frontapp.schemas.singer.write_schema")
    def test_load_and_write_schema_passes_correct_key_properties(self, mock_write_schema):
        """Test that correct key properties are passed to singer.write_schema."""
        load_and_write_schema("inboxes_table")
        call_args = mock_write_schema.call_args[0]
        # Third positional arg is key_properties
        self.assertEqual(sorted(call_args[2]), sorted(PK_FIELDS["inboxes_table"]))

    @patch("tap_frontapp.schemas.singer.write_schema")
    def test_load_and_write_schema_for_all_streams(self, mock_write_schema):
        """Test load_and_write_schema works for all static streams."""
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            mock_write_schema.reset_mock()
            with self.subTest(stream_id=stream_id):
                load_and_write_schema(stream_id)
                mock_write_schema.assert_called_once()


class TestGetSchemas(unittest.TestCase):
    """Tests for get_schemas function."""

    def test_get_schemas_returns_two_dicts(self):
        """Test that get_schemas returns (schemas_dict, metadata_dict)."""
        schemas, field_metadata = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertIsInstance(field_metadata, dict)

    def test_get_schemas_contains_all_streams(self):
        """Test that get_schemas includes all static streams."""
        schemas, _ = get_schemas()
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            self.assertIn(stream_id, schemas)

    def test_get_schemas_metadata_contains_all_streams(self):
        """Test that metadata map includes all static streams."""
        _, field_metadata = get_schemas()
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            self.assertIn(stream_id, field_metadata)

    def test_get_schemas_stream_level_metadata_has_inclusion(self):
        """Test that stream-level metadata has inclusion=available."""
        from singer import metadata as md
        _, field_metadata = get_schemas()
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            mdata_map = md.to_map(md.to_list(field_metadata[stream_id]))
            root = mdata_map.get((), {})
            self.assertEqual(
                root.get("inclusion"),
                "available",
                f"Stream '{stream_id}' should have inclusion=available at root",
            )

    def test_get_schemas_stream_level_metadata_has_key_properties(self):
        """Test that stream-level metadata has the correct key properties."""
        from singer import metadata as md
        _, field_metadata = get_schemas()
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            mdata_map = md.to_map(md.to_list(field_metadata[stream_id]))
            root = mdata_map.get((), {})
            self.assertEqual(
                sorted(root.get("table-key-properties", [])),
                sorted(PK_FIELDS[stream_id]),
            )

    def test_get_schemas_pk_fields_have_automatic_inclusion(self):
        """Test that PK fields have inclusion=automatic in metadata."""
        from singer import metadata as md
        _, field_metadata = get_schemas()
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            mdata_map = md.to_map(md.to_list(field_metadata[stream_id]))
            for pk_field in PK_FIELDS[stream_id]:
                field_meta = mdata_map.get(("properties", pk_field), {})
                self.assertEqual(
                    field_meta.get("inclusion"),
                    "automatic",
                    f"PK field '{pk_field}' in '{stream_id}' should have inclusion=automatic",
                )


if __name__ == "__main__":
    unittest.main()
