"""Test that interrupted sync resumes correctly from the saved bookmark."""
from base import FrontAppBaseTest, FULL_TABLE_STREAMS
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class FrontAppInterruptedSyncTest(InterruptedSyncTest, FrontAppBaseTest):
    """Test that if a sync is interrupted, the next sync resumes from the correct bookmark."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_interrupted_sync_test"

    def streams_to_test(self):
        # Only test INCREMENTAL streams (FULL_TABLE re-syncs fully anyway).
        # channels_table and inboxes_table return 0 records as no entities are configured
        streams_to_exclude = FULL_TABLE_STREAMS.union({"channels_table", "inboxes_table"})
        return self.expected_stream_names().difference(streams_to_exclude)

    def expected_start_date_behavior(self, stream=None):
        """Interrupted sync expectations do not depend on config start_date.

        For interrupted sync tests we rely on the tap's internal
        date_to_resume bookmark rather than the generic start_date
        handling, so we indicate that streams do not obey start_date
        here. This prevents the base test from trying to combine
        bookmarks with an unset self.start_date.
        """
        if stream is None:
            return {name: False for name in self.expected_stream_names()}
        return False

    def manipulate_state(self):
        """Simulate an interrupted incremental sync state.

        To keep the test runtime reasonable, we follow the same
        approach as the bookmark tests and start from "yesterday"
        instead of far in the past. We mark one stream as
        currently syncing and provide bookmarks for all of the
        streams under test using the tap's native ``date_to_resume``
        field. This lets InterruptedSyncTest treat every other
        stream as "already synced" and only the interrupted stream
        as in-flight.
        """

        from datetime import datetime, timedelta

        # Pick one incremental stream as the interrupted stream.
        interrupted_stream = "accounts_table"

        # Use yesterday as the bookmark to avoid scanning too much
        # historical data, similar to FrontAppBookMarkTest. The
        # underlying tap stores this value in ``date_to_resume`` in
        # ``YYYY-MM-DD HH:MM:SS`` format; our BaseTest
        # ``get_bookmark_value`` normalizes that to ``YYYY-MM-DD``.
        bookmark_day = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Apply the same bookmark to all streams under test so that
        # they are all considered to have completed at least one
        # prior sync, except for the interrupted stream which is
        # flagged via ``currently_syncing``.
        bookmarks = {
            stream: {"date_to_resume": f"{bookmark_day} 00:00:00"}
            for stream in self.streams_to_test()
        }

        return {
            "currently_syncing": interrupted_stream,
            "bookmarks": bookmarks,
        }
