import os

from tap_tester import menagerie
from tap_tester.base_suite_tests.base_case import BaseCase


STREAMS = [
    "accounts_table",
    "channels_table",
    "inboxes_table",
    "tags_table",
    "teammates_table",
    "teams_table",
]

INCREMENTAL_STREAMS = set(STREAMS)  # all streams are INCREMENTAL

FULL_TABLE_STREAMS = set()

PRIMARY_KEYS = ["analytics_date", "analytics_range", "report_id", "metric_id"]


class FrontAppBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-frontapp"

    @staticmethod
    def get_type():
        """The Stitch connection type slug."""
        return "platform.frontapp"

    def setUp(self):
        """Fail fast if required credentials env vars are missing."""
        missing = [v for v in ["TAP_FRONTAPP_TOKEN"] if not os.getenv(v)]
        if missing:
            raise Exception(f"Missing required environment variables: {missing}")

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            "start_date": self.start_date,
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account.
        Values are read from environment variables — never hardcode credentials.
        """
        return {
            "token": os.getenv("TAP_FRONTAPP_TOKEN"),
        }

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        pk_set = set(PRIMARY_KEYS)
        return {
            "accounts_table": {
                cls.PRIMARY_KEYS: pk_set,
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"analytics_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "channels_table": {
                cls.PRIMARY_KEYS: pk_set,
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"analytics_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "inboxes_table": {
                cls.PRIMARY_KEYS: pk_set,
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"analytics_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "tags_table": {
                cls.PRIMARY_KEYS: pk_set,
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"analytics_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "teammates_table": {
                cls.PRIMARY_KEYS: pk_set,
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"analytics_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "teams_table": {
                cls.PRIMARY_KEYS: pk_set,
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"analytics_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
        }
