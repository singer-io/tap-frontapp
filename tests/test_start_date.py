"""Test that data is replicated from the configured start_date."""
from datetime import datetime, timedelta

from base import FrontAppBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class FrontAppStartDateTest(StartDateTest, FrontAppBaseTest):
    """Instantiate start date according to the desired data set and run the test."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_start_date_test"

    def streams_to_test(self):
       # channels_table and inboxes_table return 0 records as no entities are configured
        streams_to_exclude = {   # sandbox typically has < API_LIMIT entities
            "channels_table",
            "inboxes_table",
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def get_properties(self, original: bool = True):
        """Provide a start_date only for start-date tests.

        StartDateTest sets ``self.start_date`` before each connection
        is created (first to ``start_date_1``, then to ``start_date_2``).
        Expose that value as the tap's ``start_date`` so the tap
        respects the configured window, without impacting other test
        suites.
        """
        return {"start_date": self.start_date}

    @property
    def start_date_1(self):
        """First start_date: two days ago at midnight UTC.

        Using a relative date keeps the test fast by limiting how
        far back the tap needs to scan.
        """
        two_days_ago = datetime.utcnow() - timedelta(days=2)
        return f"{two_days_ago.date().isoformat()}T00:00:00Z"

    @property
    def start_date_2(self):
        """Second start_date: one day ago at midnight UTC."""
        one_day_ago = datetime.utcnow() - timedelta(days=1)
        return f"{one_day_ago.date().isoformat()}T00:00:00Z"
