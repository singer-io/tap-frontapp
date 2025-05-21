#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog
from . import streams
from .context import Context
from .discover import discover
from .sync import sync
from . import schemas

REQUIRED_CONFIG_KEYS = ["token"]
LOGGER = singer.get_logger()


def get_abs_path(path: str):
    """Returns absolute path for a given relative path."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    """Loads schema from JSON file, resolving dependencies."""
    path = f"schemas/{tap_stream_id}.json"
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {sub_id: load_schema(sub_id) for sub_id in dependencies}
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)

    if args.discover:
        catalog = discover()
        json.dump(catalog.to_dict(), sys.stdout)
    else:
        atx.catalog = Catalog.from_dict(args.properties) if args.properties else discover()
        sync(atx)


if __name__ == "__main__":
    main()
