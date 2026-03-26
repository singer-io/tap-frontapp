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
        # Exclude streams with known missing test data
        streams_to_exclude = set()
        return self.expected_stream_names().difference(streams_to_exclude)
