"""Schema definitions and metadata handling for Frontapp streams."""
import os
import re
import singer
from singer import metadata, utils

LOGGER = singer.get_logger()

class IDS:  # pylint: disable=too-few-public-methods
    """Stream identifier constants."""
    ACCOUNTS_TABLE = "accounts_table"
    CHANNELS_TABLE = "channels_table"
    INBOXES_TABLE = "inboxes_table"
    TAGS_TABLE = "tags_table"
    TEAMMATES_TABLE = "teammates_table"
    TEAMS_TABLE = "teams_table"

STATIC_SCHEMA_STREAM_IDS = [
    IDS.ACCOUNTS_TABLE,
    IDS.CHANNELS_TABLE,
    IDS.INBOXES_TABLE,
    IDS.TAGS_TABLE,
    IDS.TEAMMATES_TABLE,
    IDS.TEAMS_TABLE,
]

PK_FIELDS = {
    IDS.ACCOUNTS_TABLE: ["analytics_date", "analytics_range", "report_id", "metric_id"],
    IDS.CHANNELS_TABLE: ["analytics_date", "analytics_range", "report_id", "metric_id"],
    IDS.INBOXES_TABLE: ["analytics_date", "analytics_range", "report_id", "metric_id"],
    IDS.TAGS_TABLE: ["analytics_date", "analytics_range", "report_id", "metric_id"],
    IDS.TEAMMATES_TABLE: ["analytics_date", "analytics_range", "report_id", "metric_id"],
    IDS.TEAMS_TABLE: ["analytics_date", "analytics_range", "report_id", "metric_id"],
}

def normalize_fieldname(fieldname):
    """Normalize field names to snake_case."""
    fieldname = fieldname.lower()
    fieldname = re.sub(r"[\s\-]", "_", fieldname)
    return re.sub(r"[^a-z0-9_]", "", fieldname)

def get_abs_path(path):
    """Get absolute path for schema files."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(tap_stream_id):
    """Load schema file for specified stream."""
    path = f"schemas/{tap_stream_id}.json"
    return utils.load_json(get_abs_path(path))

def load_and_write_schema(tap_stream_id):
    """Write schema to singer catalog."""
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])

def get_schemas():
    """Load all schemas and construct metadata using Singer standards."""
    schemas = {}
    metadata_map = {}

    for stream_id in STATIC_SCHEMA_STREAM_IDS:
        schema = load_schema(stream_id)
        mdata = metadata.new()

        # Stream-level metadata
        mdata = metadata.write(mdata, (), "inclusion", "available")
        mdata = metadata.write(mdata, (), "selected-by-default", True)
        mdata = metadata.write(mdata, (), "inclusion-reason", "automatic")
        mdata = metadata.write(mdata, (), "table-key-properties", PK_FIELDS[stream_id])

        # Field-level metadata
        for field_name in schema["properties"]:
            inclusion = "automatic" if field_name in PK_FIELDS[stream_id] else "available"
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", inclusion)
            mdata = metadata.write(mdata, ("properties", field_name), "selected-by-default", True)
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion-reason", "manual")

        schemas[stream_id] = schema
        metadata_map[stream_id] = mdata

    return schemas, metadata_map
