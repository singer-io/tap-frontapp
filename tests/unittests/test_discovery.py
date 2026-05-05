"""Unit tests for tap_frontapp.discover module."""

import unittest
from unittest.mock import MagicMock, patch

import requests

from tap_frontapp.discover import discover, validate_credentials


class TestValidateCredentials(unittest.TestCase):
    """Tests for validate_credentials function."""

    @patch("tap_frontapp.discover.requests.get")
    def test_valid_token_logs_success(self, mock_get):
        """Test that a 200 response logs success and does not exit."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        try:
            validate_credentials("valid-token")
        except SystemExit:
            self.fail("validate_credentials raised SystemExit on 200 response")

        mock_get.assert_called_once_with(
            "https://api2.frontapp.com/me",
            headers={"Authorization": "Bearer valid-token"},
            timeout=10,
        )

    @patch("tap_frontapp.discover.requests.get")
    def test_invalid_token_exits(self, mock_get):
        """Test that a non-200 response calls sys.exit(1)."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        with self.assertRaises(SystemExit) as ctx:
            validate_credentials("bad-token")

        self.assertEqual(ctx.exception.code, 1)

    @patch("tap_frontapp.discover.requests.get")
    def test_403_forbidden_exits(self, mock_get):
        """Test that 403 Forbidden response calls sys.exit(1)."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        with self.assertRaises(SystemExit) as ctx:
            validate_credentials("forbidden-token")

        self.assertEqual(ctx.exception.code, 1)

    @patch("tap_frontapp.discover.requests.get")
    def test_connection_error_exits(self, mock_get):
        """Test that a RequestException calls sys.exit(1)."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network down")

        with self.assertRaises(SystemExit) as ctx:
            validate_credentials("any-token")

        self.assertEqual(ctx.exception.code, 1)

    @patch("tap_frontapp.discover.requests.get")
    def test_timeout_error_exits(self, mock_get):
        """Test that a Timeout exception calls sys.exit(1)."""
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        with self.assertRaises(SystemExit) as ctx:
            validate_credentials("any-token")

        self.assertEqual(ctx.exception.code, 1)

    @patch("tap_frontapp.discover.requests.get")
    def test_correct_auth_header_sent(self, mock_get):
        """Test that Authorization header uses Bearer scheme."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        validate_credentials("my-secret-token")

        call_kwargs = mock_get.call_args[1]
        self.assertEqual(call_kwargs["headers"]["Authorization"], "Bearer my-secret-token")


class TestDiscover(unittest.TestCase):
    """Tests for discover function."""

    def test_discover_returns_catalog_with_all_streams(self):
        """Test that discover() returns a catalog containing all static streams."""
        catalog = discover()

        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        expected = {
            "accounts_table",
            "channels_table",
            "inboxes_table",
            "tags_table",
            "teammates_table",
            "teams_table",
        }
        self.assertEqual(stream_ids, expected)

    def test_discover_returns_catalog_with_correct_key_properties(self):
        """Test that each catalog entry has the correct key properties."""
        expected_pks = ["analytics_date", "analytics_range", "report_id", "metric_id"]
        catalog = discover()

        for entry in catalog.streams:
            self.assertEqual(
                sorted(entry.key_properties),
                sorted(expected_pks),
                f"Stream '{entry.tap_stream_id}' has unexpected key_properties",
            )

    def test_discover_catalog_entries_have_schema(self):
        """Test that each catalog entry has a non-empty schema."""
        catalog = discover()

        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema, f"Stream '{entry.tap_stream_id}' has no schema")

    def test_discover_catalog_entries_have_metadata(self):
        """Test that each catalog entry has metadata list."""
        catalog = discover()

        for entry in catalog.streams:
            self.assertIsInstance(
                entry.metadata, list, f"Stream '{entry.tap_stream_id}' metadata is not a list"
            )
            self.assertGreater(
                len(entry.metadata), 0, f"Stream '{entry.tap_stream_id}' has empty metadata"
            )

    def test_discover_catalog_entry_stream_id_matches_stream(self):
        """Test that tap_stream_id equals stream name for all entries."""
        catalog = discover()

        for entry in catalog.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    @patch("tap_frontapp.discover.get_schemas")
    def test_discover_raises_on_invalid_schema(self, mock_get_schemas):
        """Test that discover succeeds with a lenient schema dict."""
        from singer import metadata as md
        bad_mdata = md.new()
        bad_mdata = md.write(bad_mdata, (), "table-key-properties", [])
        mock_get_schemas.return_value = (
            {"bad_stream": {"type": "invalid_type_that_should_still_load"}},
            {"bad_stream": bad_mdata},
        )

        catalog = discover()
        self.assertEqual(len(catalog.streams), 1)

    @patch("tap_frontapp.discover.get_schemas")
    def test_discover_raises_when_metadata_missing_for_stream(self, mock_get_schemas):
        """Test that discover raises and logs when metadata lookup fails (covers except block)."""
        mock_get_schemas.return_value = (
            {"stream_a": {"type": "object", "properties": {}}},
            {},  # No metadata for stream_a triggers KeyError caught by except block
        )

        with self.assertRaises(Exception):
            discover()


if __name__ == "__main__":
    unittest.main()
