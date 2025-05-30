"""Discovery module for the FrontApp tap."""

import sys
import requests
import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from .schemas import get_schemas

LOGGER = singer.get_logger()


def validate_credentials(token):
    """Validates the FrontApp token using a simple API call."""
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


def discover():
    """Run the discovery mode, prepare the catalog file and return the catalog."""
    schemas, field_metadata = get_schemas()
    LOGGER.info("Schemas loaded: %s", list(schemas.keys()))

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error("Error while processing stream '%s': %s", stream_name, err)
            raise err

        key_properties = mdata.get((), {}).get("table-key-properties", [])

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=metadata.to_list(mdata),
            )
        )

    return catalog
