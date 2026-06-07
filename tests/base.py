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

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-frontapp"

    @staticmethod
    def get_type():
        """The Stitch connection type slug."""
        return "platform.frontapp"

    def setUp(self, logging=True):
        """Fail fast if required credentials env vars are missing.

        The ``logging`` argument is accepted for compatibility with
        tap-tester's discovery test harness, which calls ``setUp``
        with this keyword. It is not used here.
        """
        missing = [v for v in ["TAP_FRONTAPP_TOKEN"] if not os.getenv(v)]
        if missing:
            raise Exception(f"Missing required environment variables: {missing}")

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return {}

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

    def expected_stream_names(self):
        """The expected stream names and exclude forbidden streams."""
        return {
            stream_name
            for stream_name, metadata in self.expected_metadata().items()
            if not metadata.get(self.IS_FORBIDDEN_STREAM, False)
        }

    def get_bookmark_value(self, state, stream):
        """Return the effective bookmark value for a stream.

        FrontApp stores bookmarks under ``date_to_resume`` rather than
        the replication key name (``analytics_date``). For tests, we
        normalize this to a simple ``YYYY-MM-DD`` string so
        tap-tester's ``parse_date`` helper can consume it.
        """
        stream_bookmark = (state or {}).get("bookmarks", {}).get(stream, {})

        # Prefer an explicit analytics_date bookmark if present.
        if "analytics_date" in stream_bookmark:
            return stream_bookmark["analytics_date"]

        # Fallback to the tap's native date_to_resume field.
        raw = stream_bookmark.get("date_to_resume")
        if not raw:
            return None

        # raw is typically "YYYY-MM-DD HH:MM:SS"; strip time portion
        # so it matches one of BaseCase.parse_date accepted formats.
        if isinstance(raw, str):
            return raw.split(" ", 1)[0]
        return raw
