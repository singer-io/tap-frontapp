"""Test that with no fields selected, automatic fields are still replicated."""
from base import FrontAppBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class FrontAppAutomaticFields(MinimumSelectionTest, FrontAppBaseTest):
    """Test that with no fields selected for a stream, automatic (primary key and
    replication key) fields are still replicated."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_automatic_fields_test"

    def streams_to_test(self):
        # channels_table and inboxes_table return 0 records as no entities are configured
        # in this FrontApp account
        streams_to_exclude = {"channels_table", "inboxes_table"}
        return self.expected_stream_names().difference(streams_to_exclude)
