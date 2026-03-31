#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog
from .context import Context
from . import schemas
from .discover import discover as _discover_impl, validate_credentials as _validate_credentials_impl
from .sync import sync as _sync_impl

REQUIRED_CONFIG_KEYS = ["token"]
LOGGER = singer.get_logger()


def discover(*args, **kwargs):
    """Public discover function re-exported from discover module.

    This wrapper exists so tests can patch ``tap_frontapp.discover``
    and so the CLI entrypoint can call a stable symbol regardless of
    where the implementation lives.
    """

    return _discover_impl(*args, **kwargs)


def validate_credentials(*args, **kwargs):
    """Public credential validation helper re-exported for tests."""

    return _validate_credentials_impl(*args, **kwargs)


def sync(*args, **kwargs):
    """Public sync function re-exported from the sync module.

    Tests patch ``tap_frontapp.sync`` and expect the entrypoint to call
    this symbol directly.
    """

    return _sync_impl(*args, **kwargs)


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


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        validate_credentials(args.config["token"])
        catalog = discover()
        json.dump(catalog.to_dict(), sys.stdout, indent=2)
    else:
        atx = Context(args.config, args.state)

        catalog_obj = args.properties or getattr(args, "catalog", None)
        if catalog_obj is None:
            catalog_obj = discover()
        elif isinstance(catalog_obj, dict):
            catalog_obj = Catalog.from_dict(catalog_obj)

        atx.catalog = catalog_obj
        sync(atx)


if __name__ == "__main__":
    main()
