"""Unit tests for stream exclusion during discovery (403 handling)."""

import unittest
from unittest.mock import MagicMock, patch, call

from tap_frontapp.http import FrontappForbiddenError, Client
from tap_frontapp.discover import (
    discover,
    _check_stream_access,
    _apply_access_checks,
    STREAM_API_PATHS,
)
from tap_frontapp.schemas import STATIC_SCHEMA_STREAM_IDS


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
        """Test that a stream not in STREAM_API_PATHS returns True."""
        mock_client = MagicMock()
        result = _check_stream_access(mock_client, "unknown_stream")
        self.assertTrue(result)
        mock_client.request.assert_not_called()

    def test_all_known_streams_have_paths(self):
        """Test that all static schema streams have API paths defined."""
        for stream_id in STATIC_SCHEMA_STREAM_IDS:
            self.assertIn(stream_id, STREAM_API_PATHS)


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
