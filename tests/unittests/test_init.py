"""Unit tests for tap_frontapp.__init__ module."""

import os
import unittest
from unittest.mock import MagicMock, patch

import tap_frontapp
from tap_frontapp import get_abs_path, load_schema


class TestGetAbsPath(unittest.TestCase):
    """Tests for get_abs_path function."""

    def test_returns_absolute_path(self):
        """Test that get_abs_path returns an absolute path."""
        result = get_abs_path("schemas/teams_table.json")
        self.assertTrue(os.path.isabs(result))

    def test_path_includes_filename(self):
        """Test that the returned path ends with the given relative path component."""
        result = get_abs_path("schemas/teams_table.json")
        self.assertTrue(result.endswith("teams_table.json"))

    def test_existing_schema_path_is_valid(self):
        """Test that the resolved path for a known schema actually exists."""
        result = get_abs_path("schemas/teams_table.json")
        self.assertTrue(os.path.exists(result), f"Expected path to exist: {result}")


class TestLoadSchema(unittest.TestCase):
    """Tests for load_schema function in __init__.py."""

    def test_load_known_schema_returns_dict(self):
        """Test that load_schema returns a dict for a known stream ID."""
        schema = load_schema("teams_table")
        self.assertIsInstance(schema, dict)

    def test_load_schema_has_properties(self):
        """Test that the loaded schema has a properties key."""
        schema = load_schema("accounts_table")
        self.assertIn("properties", schema)

    def test_load_schema_strips_tap_schema_dependencies(self):
        """Test that tap_schema_dependencies key is removed from loaded schema."""
        schema = load_schema("teams_table")
        self.assertNotIn("tap_schema_dependencies", schema)

    def test_load_unknown_schema_raises(self):
        """Test that an unknown stream ID raises an exception."""
        with self.assertRaises(Exception):
            load_schema("nonexistent_stream_xyz")

    @patch("tap_frontapp.singer.resolve_schema_references")
    @patch("tap_frontapp.utils.load_json")
    def test_load_schema_resolves_dependencies(self, mock_load_json, mock_resolve):
        """Test that load_schema calls resolve_schema_references when dependencies present."""
        dep_schema = {"type": "object", "properties": {"id": {"type": "string"}}}
        mock_load_json.side_effect = [
            {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "tap_schema_dependencies": ["dep_stream"],
            },
            dep_schema,
        ]
        load_schema("any_stream")
        mock_resolve.assert_called_once()


class TestMain(unittest.TestCase):
    """Tests for the main() entry point."""

    @patch("tap_frontapp.sync")
    @patch("tap_frontapp.discover")
    @patch("tap_frontapp.Context")
    @patch("tap_frontapp.utils.parse_args")
    def test_main_sync_mode_creates_context_and_calls_sync(
        self, mock_parse_args, mock_context_cls, mock_discover, mock_sync
    ):
        """Test that main() in sync mode builds a Context and calls sync."""
        mock_args = MagicMock()
        mock_args.discover = False
        mock_args.config = {"token": "tok", "start_date": "2024-01-01"}
        mock_args.state = {}
        mock_args.properties = None
        mock_parse_args.return_value = mock_args

        mock_catalog = MagicMock()
        mock_discover.return_value = mock_catalog

        mock_atx = MagicMock()
        mock_context_cls.return_value = mock_atx

        tap_frontapp.main()

        mock_context_cls.assert_called_once_with(mock_args.config, mock_args.state)
        mock_sync.assert_called_once_with(mock_atx)

    @patch("tap_frontapp.json.dump")
    @patch("tap_frontapp.validate_credentials")
    @patch("tap_frontapp.discover")
    @patch("tap_frontapp.utils.parse_args")
    def test_main_discover_mode_calls_validate_and_discover(
        self, mock_parse_args, mock_discover, mock_validate, mock_json_dump
    ):
        """Test that main() in discover mode validates credentials and runs discovery."""
        mock_args = MagicMock()
        mock_args.discover = True
        mock_args.config = {"token": "tok"}
        mock_parse_args.return_value = mock_args

        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        tap_frontapp.main()

        mock_validate.assert_called_once_with("tok")
        mock_discover.assert_called_once()
        mock_json_dump.assert_called_once()

    @patch("tap_frontapp.sync")
    @patch("tap_frontapp.Catalog")
    @patch("tap_frontapp.Context")
    @patch("tap_frontapp.utils.parse_args")
    def test_main_uses_provided_properties_as_catalog(
        self, mock_parse_args, mock_context_cls, mock_catalog_cls, mock_sync
    ):
        """Test that main() uses args.properties when provided (not discovery)."""
        mock_args = MagicMock()
        mock_args.discover = False
        mock_args.config = {"token": "tok"}
        mock_args.state = {}
        mock_args.properties = {"streams": []}
        mock_parse_args.return_value = mock_args

        mock_atx = MagicMock()
        mock_context_cls.return_value = mock_atx

        catalog_instance = MagicMock()
        mock_catalog_cls.from_dict.return_value = catalog_instance

        tap_frontapp.main()

        mock_catalog_cls.from_dict.assert_called_once_with(mock_args.properties)
        self.assertEqual(mock_atx.catalog, catalog_instance)


if __name__ == "__main__":
    unittest.main()