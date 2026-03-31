"""Test that all schema fields are replicated."""
from base import FrontAppBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

# Fields that exist in the schema but may not be returned by the FrontApp API
# in all test environments. Populate after a first real test run if needed.
KNOWN_MISSING_FIELDS = {
    # "<stream_name>": {"<field_name>"},
}


class FrontAppAllFields(AllFieldsTest, FrontAppBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = KNOWN_MISSING_FIELDS

    @staticmethod
    def name():
        return "tap_tester_frontapp_all_fields_test"

    def streams_to_test(self):
        # Exclude streams with no test data or no API access in the test environment
        # channels_table and inboxes_table return 0 records as no entities are configured
        # in this FrontApp account
        streams_to_exclude = {"channels_table", "inboxes_table"}
        return self.expected_stream_names().difference(streams_to_exclude)
