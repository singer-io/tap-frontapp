"""Test tap discovery mode and metadata."""
from base import FrontAppBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class FrontAppDiscoveryTest(DiscoveryTest, FrontAppBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_frontapp_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
