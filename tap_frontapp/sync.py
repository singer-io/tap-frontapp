import singer
from typing import Dict
from singer.catalog import Catalog
from tap_frontapp.streams import sync_selected_streams
from tap_frontapp.schemas import load_and_write_schema, STATIC_SCHEMA_STREAM_IDS

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """Update currently_syncing in the Singer state."""
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config: Dict, catalog: Catalog, state: Dict) -> None:
    """Main sync method to process selected streams."""
    streams_to_sync = [s.tap_stream_id for s in catalog.get_selected_streams(state)]
    LOGGER.info(f"Selected streams: {streams_to_sync}")

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info(f"Last stream synced (if resuming): {last_stream}")

    # Load and emit schemas for all static streams
    for stream_name in STATIC_SCHEMA_STREAM_IDS:
        load_and_write_schema(stream_name)

    # Sync stream records
    LOGGER.info("Starting sync of selected streams")
    sync_selected_streams(client, streams_to_sync, catalog, state)
    LOGGER.info("All selected streams synced successfully")
