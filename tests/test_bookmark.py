"""Test tap sets a bookmark and respects it in subsequent runs."""
from base import FrontAppBaseTest, FULL_TABLE_STREAMS
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class FrontAppBookMarkTest(BookmarkTest, FrontAppBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    bookmark_format = "%Y-%m-%dT%H:%M:%SZ"
    initial_bookmarks = {
        "bookmarks": {
            "accounts_table": {"date_to_resume": "2020-01-01 00:00:00"},
            "channels_table": {"date_to_resume": "2020-01-01 00:00:00"},
            "inboxes_table": {"date_to_resume": "2020-01-01 00:00:00"},
            "tags_table": {"date_to_resume": "2020-01-01 00:00:00"},
            "teammates_table": {"date_to_resume": "2020-01-01 00:00:00"},
            "teams_table": {"date_to_resume": "2020-01-01 00:00:00"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_frontapp_bookmark_test"

    def streams_to_test(self):
        # Exclude any FULL_TABLE streams (none currently)
        streams_to_exclude = FULL_TABLE_STREAMS
        return self.expected_stream_names().difference(streams_to_exclude)
