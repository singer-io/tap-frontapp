"""Test that all schema fields are replicated."""
from base import FrontAppBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

# Metric fields that the Front API returns conditionally — only when an entity
# has activity data for that metric. They may be present or absent in any record.
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

# All streams that contain conditional metric fields
_METRIC_STREAMS = {
    "accounts_table",
    "channels_table",
    "inboxes_table",
    "tags_table",
    "teammates_table",
    "teams_table",
}


class FrontAppAllFields(AllFieldsTest, FrontAppBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_all_fields_test"

    def streams_to_test(self):
        # Exclude streams with no test data or no API access in the test environment
        # channels_table and inboxes_table return 0 records as no entities are configured
        # in this FrontApp account
        streams_to_exclude = {"channels_table", "inboxes_table"}
        return self.expected_stream_names().difference(streams_to_exclude)

    def test_all_fields_for_streams_are_replicated(self):
        """Override to handle metric fields that the Front API returns conditionally.
        The Front API only includes a metric in the response when the entity has
        activity data for that metric, so metric fields may be present or absent
        in any given record. Metric fields are excluded from both sides of the
        assertion so the test validates all structural fields without being
        affected by the variable presence of metric data."""
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                conditional_fields = _METRIC_FIELDS if stream in _METRIC_STREAMS else set()

                expected_all_keys = self.selected_fields.get(stream, set()) - conditional_fields
                fields_replicated = self.actual_fields.get(stream, set()) - conditional_fields

                self.assertSetEqual(
                    fields_replicated,
                    expected_all_keys,
                )
