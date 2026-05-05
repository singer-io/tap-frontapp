"""Test that the tap can replicate multiple pages of data."""
from base import FrontAppBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class FrontAppPaginationTest(PaginationTest, FrontAppBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use pagination."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_pagination_test"

    def streams_to_test(self):
        # FrontApp analytics streams return one daily report per entity.
        # Most test environments do not have enough entities to exceed one page.
        # Exclude all streams until a test environment with sufficient data is available.
        streams_to_exclude = {   # sandbox typically has < API_LIMIT entities
            "channels_table",
            "inboxes_table",
        }
        return self.expected_stream_names().difference(streams_to_exclude)
