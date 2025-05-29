#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog
from .context import Context
from .discover import discover
from .sync import sync
from . import schemas

REQUIRED_CONFIG_KEYS = ["token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    """Returns absolute path for a given relative path."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    """Loads schema from JSON file, resolving dependencies."""
    path = f"schemas/{tap_stream_id}.json"
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {sub_stream_id: load_schema(sub_stream_id) for sub_stream_id in dependencies}
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema

def validate_credentials(token):
    """Validates the FrontApp token using a simple API call"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get("https://api2.frontapp.com/me", headers=headers, timeout=10)
        if response.status_code == 200:
            LOGGER.info("Frontapp credentials validated successfully.")
        else:
            LOGGER.critical("Invalid Frontapp credentials. Status code: %s", response.status_code)
            sys.exit(1)
    except requests.exceptions.RequestException as err:
        LOGGER.critical("Credential validation failed: %s", str(err))
        sys.exit(1)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    #Validate credentials early
    validate_credentials(args.config["token'])
                         
    atx = Context(args.config, args.state)

    if args.discover:
        catalog = discover()
        json.dump(catalog.to_dict(), sys.stdout)
    else:
        atx.catalog = Catalog.from_dict(args.properties) if args.properties else discover()
        sync(atx)


if __name__ == "__main__":
    main()
