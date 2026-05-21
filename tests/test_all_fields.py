"""Test that all schema fields are replicated."""
from base import FrontAppBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

# Fields that exist in the schema but may not be returned by the FrontApp API
# in all test environments. The Front API only returns metrics that have data
# for a given entity, so metric fields are conditionally absent from records.
_METRIC_FIELDS = {
    "avg_first_response_time",
    "avg_handle_time",
    "avg_response_time",
    "avg_sla_breach_time",
    "avg_total_reply_time",
    "new_segments_count",
    "num_active_segments_full",
    "num_archived_segments",
    "num_archived_segments_with_reply",
    "num_csat_survey_response",
    "num_messages_received",
    "num_messages_sent",
    "num_sla_breach",
    "pct_csat_survey_satisfaction",
    "pct_tagged_conversations",
    "num_open_segments_start",
    "num_closed_segments",
    "num_open_segments_end",
    "num_workload_segments",
}

KNOWN_MISSING_FIELDS = {
    "accounts_table": _METRIC_FIELDS,
    "channels_table": _METRIC_FIELDS,
    "inboxes_table": _METRIC_FIELDS,
    "tags_table": _METRIC_FIELDS,
    "teammates_table": _METRIC_FIELDS,
    "teams_table": _METRIC_FIELDS,
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
