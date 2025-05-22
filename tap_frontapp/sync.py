"""Module for syncing selected FrontApp streams using Singer framework."""

import singer
from tap_frontapp.streams import sync_selected_streams
from tap_frontapp.schemas import load_and_write_schema, STATIC_SCHEMA_STREAM_IDS

LOGGER = singer.get_logger()


def update_currently_syncing(state, stream_name):
    """Update the currently syncing stream in the Singer state."""
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(atx):
    """Main sync method to process selected streams from FrontApp."""

    catalog = atx.catalog
    state = atx.state

    streams_to_sync = [s.tap_stream_id for s in catalog.get_selected_streams(state)]
    LOGGER.info("Selected streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("Last stream synced (if resuming): %s", last_stream)

    for stream_name in STATIC_SCHEMA_STREAM_IDS:
        load_and_write_schema(stream_name)

    LOGGER.info("Starting sync of selected streams.")
    sync_selected_streams(atx)
    LOGGER.info("All selected streams synced successfully.")
