"""Test that interrupted sync resumes correctly from the saved bookmark."""
from base import FrontAppBaseTest, FULL_TABLE_STREAMS
from tap_tester.base_suite_tests.interrupted_sync_tests import InterruptedSyncTest


class FrontAppInterruptedSyncTest(InterruptedSyncTest, FrontAppBaseTest):
    """Test that if a sync is interrupted, the next sync resumes from the correct bookmark."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_interrupted_sync_test"

    def streams_to_test(self):
        # Only test INCREMENTAL streams (FULL_TABLE re-syncs fully anyway)
        streams_to_exclude = FULL_TABLE_STREAMS
        return self.expected_stream_names().difference(streams_to_exclude)
