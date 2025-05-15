#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["token"]

LOGGER = singer.get_logger()

#def check_authorization(atx):
#    atx.client.get('/settings')


# Some taps do discovery dynamically where the catalog is read in from a
#  call to the api but with the odd frontapp structure, we won't do that
#  here we never use atx in here since the schema is from file but we
#  would use it if we pulled schema from the API def discover(atx):


def discover():
    catalog = Catalog([])
    
    # Build initial catalog from schema files
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        LOGGER.info("tap stream id=%s", tap_stream_id)
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id))
        metadata = []
        
        # Stream-level metadata select the stream
        metadata.append({
            "metadata": {
                "selected": True  # Make sure to select every stream
            },
            "breadcrumb": []
        })
        
        # Field level metadata with inclusion type
        for field_name in schema.properties.keys():
            if field_name in schemas.PK_FIELDS[tap_stream_id]:
                inclusion = "automatic"
            else:
                inclusion = "available"
            metadata.append({
                "metadata": {
                    "inclusion": inclusion
                },
                "breadcrumb": ['properties', field_name]
            })
        
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata
        ))
    
    # Creating a dict to change before converting
    catalog_dict = catalog.to_dict()
    
    required_streams = [
        "accounts_table",
        "channels_table",
        "inboxes_table",
        "tags_table",
        "teammates_table",
        "teams_table"
    ]
    
    # We verify this to ensure all mandatory streams are available even if schema files are missing
    present_streams = {stream['tap_stream_id'] for stream in catalog_dict['streams']}
    
    # Ensure all required streams are included even if schema is missing
    for stream_name in required_streams:
        if stream_name not in present_streams:
            LOGGER.info("Adding missing required stream: %s", stream_name)
            
            # Create a minimal stream entry that will be visible in the output
            catalog_dict['streams'].append({
                "stream": stream_name,
                "tap_stream_id": stream_name,
                "schema": {
                    "type": ['null', 'object'],
                    "properties": {},
                    "additionalProperties": False
                },
                "key_properties": [],
                "metadata": [
                    {
                        "metadata": {
                            "selected": True
                        },
                        "breadcrumb": []
                    }
                ]
            })
    
    # This ensure singer tools recognize all streams in the catalog dictionary
    for stream in catalog_dict['streams']:
        has_selection = False
        for metadata_item in stream.get('metadata', []):
            if metadata_item.get('breadcrumb') == [] and metadata_item.get('metadata', {}).get('selected') is True:
                has_selection = True
                break
                
        if not has_selection:
            LOGGER.info("Adding selection metadata to stream: %s", stream['tap_stream_id'])
            stream.setdefault('metadata', []).insert(0, {
                "metadata": {
                    "selected": True
                },
                "breadcrumb": []
            })
    
    # Convert back to a Catalog object
    modified_catalog = Catalog.from_dict(catalog_dict)

    
    return modified_catalog


def get_abs_path(path: str):
    """Returns absolute path for URL."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# this is already defined in schemas.py though w/o dependencies.  do we keep this for the sync?
def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def sync(atx):
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        schemas.load_and_write_schema(tap_stream_id)

    streams.sync_selected_streams(atx)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)
    if args.discover:
        # the schema is static from file so we don't need to pass in atx for connection info.
        catalog = discover()
        json.dump(catalog.to_dict(), sys.stdout)
    else:
        atx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover()
        sync(atx)

if __name__ == "__main__":
    main()
    