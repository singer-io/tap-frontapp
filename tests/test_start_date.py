"""Test that data is replicated from the configured start_date."""
from base import FrontAppBaseTest, FULL_TABLE_STREAMS
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class FrontAppStartDateTest(StartDateTest, FrontAppBaseTest):
    """Instantiate start date according to the desired data set and run the test."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_start_date_test"

    def streams_to_test(self):
        # Exclude FULL_TABLE streams (none currently) and streams with insufficient test data
        streams_to_exclude = FULL_TABLE_STREAMS
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2019-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2020-01-01T00:00:00Z"
