"""Unit tests for tap_frontapp.context module."""

import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from tap_frontapp.context import Context


def _make_context(config=None, state=None):
    """Helper to build a Context with minimal config."""
    config = config or {"token": "test-token"}
    state = state or {}
    with patch("tap_frontapp.context.Client"):
        return Context(config, state)


class TestContextInit(unittest.TestCase):
    """Tests for Context.__init__."""

    def test_config_stored(self):
        """Test that config is stored on the context."""
        cfg = {"token": "abc", "start_date": "2024-01-01"}
        ctx = _make_context(config=cfg)
        self.assertEqual(ctx.config, cfg)

    def test_state_stored(self):
        """Test that state is stored on the context."""
        state = {"bookmarks": {"stream1": {"date_to_resume": "2024-01-01"}}}
        ctx = _make_context(state=state)
        self.assertEqual(ctx.state, state)

    def test_client_created(self):
        """Test that a Client instance is created during init."""
        with patch("tap_frontapp.context.Client") as MockClient:
            MockClient.return_value = MagicMock()
            ctx = Context({"token": "tok"}, {})
            MockClient.assert_called_once_with({"token": "tok"})

    def test_catalog_is_none_initially(self):
        """Test that catalog is None before it is set."""
        ctx = _make_context()
        self.assertIsNone(ctx.catalog)

    def test_selected_stream_ids_is_none_initially(self):
        """Test that selected_stream_ids is None before catalog is set."""
        ctx = _make_context()
        self.assertIsNone(ctx.selected_stream_ids)

    def test_now_is_set(self):
        """Test that now is set to a datetime during init."""
        from datetime import datetime
        ctx = _make_context()
        self.assertIsInstance(ctx.now, datetime)


class TestContextCatalogSetter(unittest.TestCase):
    """Tests for Context.catalog property setter."""

    def _make_catalog_stream(self, stream_id, selected=True):
        """Build a mock catalog stream entry."""
        stream = MagicMock()
        stream.tap_stream_id = stream_id
        stream.metadata = [
            {
                "breadcrumb": [],
                "metadata": {"selected": selected},
            }
        ]
        return stream

    def test_setting_catalog_populates_selected_stream_ids(self):
        """Test that setting catalog extracts selected stream IDs."""
        ctx = _make_context()

        mock_catalog = MagicMock()
        stream_a = self._make_catalog_stream("stream_a", selected=True)
        stream_b = self._make_catalog_stream("stream_b", selected=True)
        mock_catalog.streams = [stream_a, stream_b]

        ctx.catalog = mock_catalog
        self.assertIn("stream_a", ctx.selected_stream_ids)
        self.assertIn("stream_b", ctx.selected_stream_ids)

    def test_unselected_streams_not_in_selected_ids(self):
        """Test that unselected streams are excluded from selected_stream_ids."""
        ctx = _make_context()

        mock_catalog = MagicMock()
        selected = self._make_catalog_stream("selected_stream", selected=True)
        unselected = self._make_catalog_stream("unselected_stream", selected=False)
        mock_catalog.streams = [selected, unselected]

        ctx.catalog = mock_catalog
        self.assertIn("selected_stream", ctx.selected_stream_ids)
        self.assertNotIn("unselected_stream", ctx.selected_stream_ids)

    def test_catalog_getter_returns_set_catalog(self):
        """Test that the catalog getter returns the set catalog."""
        ctx = _make_context()
        mock_catalog = MagicMock()
        mock_catalog.streams = []
        ctx.catalog = mock_catalog
        self.assertEqual(ctx.catalog, mock_catalog)


class TestContextGetBookmark(unittest.TestCase):
    """Tests for Context.get_bookmark."""

    def test_get_bookmark_returns_value(self):
        """Test that get_bookmark retrieves bookmark value from state."""
        state = {"bookmarks": {"my_stream": {"date_to_resume": "2024-01-15"}}}
        ctx = _make_context(state=state)
        result = ctx.get_bookmark(["my_stream", "date_to_resume"])
        self.assertEqual(result, "2024-01-15")

    def test_get_bookmark_returns_none_for_missing(self):
        """Test that get_bookmark returns None when bookmark doesn't exist."""
        ctx = _make_context(state={})
        result = ctx.get_bookmark(["nonexistent_stream", "date_to_resume"])
        self.assertIsNone(result)


class TestContextSetBookmark(unittest.TestCase):
    """Tests for Context.set_bookmark."""

    def test_set_bookmark_writes_string_value(self):
        """Test that set_bookmark writes a string value to state."""
        ctx = _make_context(state={})
        ctx.set_bookmark(["my_stream", "date_to_resume"], "2024-01-01T00:00:00Z")
        self.assertEqual(
            ctx.state["bookmarks"]["my_stream"]["date_to_resume"],
            "2024-01-01T00:00:00Z",
        )

    def test_set_bookmark_converts_date_to_isoformat(self):
        """Test that set_bookmark converts date objects to ISO format string."""
        ctx = _make_context(state={})
        d = date(2024, 6, 15)
        ctx.set_bookmark(["my_stream", "date_to_resume"], d)
        self.assertEqual(
            ctx.state["bookmarks"]["my_stream"]["date_to_resume"],
            "2024-06-15",
        )

    def test_set_bookmark_overrides_existing_value(self):
        """Test that set_bookmark can update an existing bookmark value."""
        state = {"bookmarks": {"my_stream": {"date_to_resume": "2024-01-01"}}}
        ctx = _make_context(state=state)
        ctx.set_bookmark(["my_stream", "date_to_resume"], "2024-06-01")
        self.assertEqual(
            ctx.state["bookmarks"]["my_stream"]["date_to_resume"],
            "2024-06-01",
        )


class TestContextWriteState(unittest.TestCase):
    """Tests for Context.write_state."""

    @patch("tap_frontapp.context.singer.write_state")
    def test_write_state_calls_singer_write_state(self, mock_write_state):
        """Test that write_state delegates to singer.write_state."""
        state = {"bookmarks": {}}
        ctx = _make_context(state=state)
        ctx.write_state()
        mock_write_state.assert_called_once_with(state)


if __name__ == "__main__":
    unittest.main()
