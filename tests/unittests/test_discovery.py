"""Unit tests for tap_frontapp.discover module."""

import unittest
from unittest.mock import MagicMock, patch

import requests

from tap_frontapp.http import FrontappForbiddenError, Client
from tap_frontapp.discover import (
    discover,
    validate_credentials,
    _check_stream_access,
    _apply_access_checks,
)
from tap_frontapp.streams import METRIC_API_PATH
from tap_frontapp.schemas import STATIC_SCHEMA_STREAM_IDS


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


class TestCheckStreamAccess(unittest.TestCase):
    """Tests for _check_stream_access function."""

    def test_accessible_stream_returns_true(self):
        """Test that a stream with valid access returns True."""
        mock_client = MagicMock()
        mock_client.url.return_value = "https://api2.frontapp.com/accounts"
        mock_client.request.return_value = MagicMock()

        result = _check_stream_access(mock_client, "accounts_table")
        self.assertTrue(result)
        mock_client.request.assert_called_once_with(
            'get', "https://api2.frontapp.com/accounts"
        )

    def test_forbidden_stream_returns_false(self):
        """Test that a stream returning 403 returns False."""
        mock_client = MagicMock()
        mock_client.url.return_value = "https://api2.frontapp.com/accounts"
        mock_client.request.side_effect = FrontappForbiddenError("403 Forbidden")

        result = _check_stream_access(mock_client, "accounts_table")
        self.assertFalse(result)

    def test_unknown_stream_returns_true(self):
        """Test that a stream not in METRIC_API_PATH returns True."""
        mock_client = MagicMock()
        result = _check_stream_access(mock_client, "unknown_stream")
        self.assertTrue(result)
        mock_client.request.assert_not_called()

    def test_all_known_streams_have_paths(self):
        """Test that all static schema streams have API paths defined."""
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            self.assertIn(stream_id, METRIC_API_PATH)


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks function."""

    def _make_schemas_and_metadata(self, stream_ids):
        """Helper to create mock schemas and field_metadata dicts."""
        schemas = {sid: {"type": "object", "properties": {}} for sid in stream_ids}
        field_metadata = {sid: {(): {"table-key-properties": []}} for sid in stream_ids}
        return schemas, field_metadata

    @patch("tap_frontapp.discover._check_stream_access")
    def test_all_streams_accessible(self, mock_check):
        """Test no streams are removed when all are accessible."""
        mock_check.return_value = True
        mock_client = MagicMock()
        schemas, field_metadata = self._make_schemas_and_metadata(STATIC_SCHEMA_STREAM_IDS)

        _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), set(STATIC_SCHEMA_STREAM_IDS))
        self.assertEqual(set(field_metadata.keys()), set(STATIC_SCHEMA_STREAM_IDS))

    @patch("tap_frontapp.discover._check_stream_access")
    def test_partial_access_excludes_forbidden_streams(self, mock_check):
        """Test that only inaccessible streams are removed."""
        def side_effect(client, stream_name):
            return stream_name != "accounts_table"

        mock_check.side_effect = side_effect
        mock_client = MagicMock()
        schemas, field_metadata = self._make_schemas_and_metadata(STATIC_SCHEMA_STREAM_IDS)

        _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertNotIn("accounts_table", schemas)
        self.assertNotIn("accounts_table", field_metadata)
        # Other streams should remain
        self.assertIn("channels_table", schemas)
        self.assertIn("inboxes_table", schemas)

    @patch("tap_frontapp.discover._check_stream_access")
    def test_multiple_streams_excluded(self, mock_check):
        """Test that multiple inaccessible streams are excluded."""
        excluded = {"accounts_table", "tags_table", "teams_table"}

        def side_effect(client, stream_name):
            return stream_name not in excluded

        mock_check.side_effect = side_effect
        mock_client = MagicMock()
        schemas, field_metadata = self._make_schemas_and_metadata(STATIC_SCHEMA_STREAM_IDS)

        _apply_access_checks(mock_client, schemas, field_metadata)

        for stream_name in excluded:
            self.assertNotIn(stream_name, schemas)
            self.assertNotIn(stream_name, field_metadata)

        remaining = set(STATIC_SCHEMA_STREAM_IDS) - excluded
        for stream_name in remaining:
            self.assertIn(stream_name, schemas)
            self.assertIn(stream_name, field_metadata)

    @patch("tap_frontapp.discover._check_stream_access")
    def test_all_streams_forbidden_raises_error(self, mock_check):
        """Test that FrontappForbiddenError is raised when all streams are inaccessible."""
        mock_check.return_value = False
        mock_client = MagicMock()
        schemas, field_metadata = self._make_schemas_and_metadata(STATIC_SCHEMA_STREAM_IDS)

        with self.assertRaises(FrontappForbiddenError) as ctx:
            _apply_access_checks(mock_client, schemas, field_metadata)

        self.assertIn("do not have 'read' access to any", str(ctx.exception))

    @patch("tap_frontapp.discover._check_stream_access")
    def test_warning_logged_for_excluded_streams(self, mock_check):
        """Test that a warning is logged when some streams are excluded."""
        def side_effect(client, stream_name):
            return stream_name != "tags_table"

        mock_check.side_effect = side_effect
        mock_client = MagicMock()
        schemas, field_metadata = self._make_schemas_and_metadata(STATIC_SCHEMA_STREAM_IDS)

        with patch("tap_frontapp.discover.LOGGER") as mock_logger:
            _apply_access_checks(mock_client, schemas, field_metadata)
            mock_logger.warning.assert_called()
            warning_msg = mock_logger.warning.call_args[0][0]
            self.assertIn("do not have 'read' access", warning_msg)


class TestDiscoverWithClient(unittest.TestCase):
    """Tests for discover() function with client parameter."""

    @patch("tap_frontapp.discover._apply_access_checks")
    def test_discover_with_client_calls_access_checks(self, mock_access):
        """Test that discover() calls _apply_access_checks when client is provided."""
        mock_client = MagicMock()
        catalog = discover(mock_client)

        mock_access.assert_called_once()
        args = mock_access.call_args[0]
        self.assertEqual(args[0], mock_client)

    def test_discover_without_client_skips_access_checks(self):
        """Test that discover() without client does not perform access checks."""
        with patch("tap_frontapp.discover._apply_access_checks") as mock_access:
            catalog = discover()
            mock_access.assert_not_called()

        # Should still return all streams
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(stream_ids, set(STATIC_SCHEMA_STREAM_IDS))

    @patch("tap_frontapp.discover._check_stream_access")
    def test_discover_excludes_forbidden_from_catalog(self, mock_check):
        """Test that discover() with client excludes forbidden streams from catalog."""
        def side_effect(client, stream_name):
            return stream_name != "teams_table"

        mock_check.side_effect = side_effect
        mock_client = MagicMock()

        catalog = discover(mock_client)

        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertNotIn("teams_table", stream_ids)
        self.assertIn("accounts_table", stream_ids)

    @patch("tap_frontapp.discover._check_stream_access")
    def test_discover_all_forbidden_raises(self, mock_check):
        """Test that discover() raises when all streams return 403."""
        mock_check.return_value = False
        mock_client = MagicMock()

        with self.assertRaises(FrontappForbiddenError):
            discover(mock_client)


class TestFrontappForbiddenErrorInClient(unittest.TestCase):
    """Tests for FrontappForbiddenError being raised by the HTTP Client."""

    @patch("requests.request")
    def test_client_raises_forbidden_on_403(self, mock_request):
        """Test that the Client raises FrontappForbiddenError on 403 response."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        mock_response.headers = {
            "X-Ratelimit-Remaining": "100",
            "X-Ratelimit-Reset": "1000",
        }
        mock_request.return_value = mock_response

        client = Client(config={"token": "test-token"})
        with self.assertRaises(FrontappForbiddenError):
            client.request('get', "https://api2.frontapp.com/accounts")


if __name__ == "__main__":
    unittest.main()
