"""Discovery module for the FrontApp tap."""

import sys
import requests
import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from .schemas import get_schemas, STATIC_SCHEMA_STREAM_IDS
from .http import FrontappForbiddenError
from .streams import METRIC_API_PATH

LOGGER = singer.get_logger()


def _check_stream_access(client, stream_name):
    """
    Probe a stream's API endpoint for read access.
    Returns True if accessible, False if a 403 Forbidden error is raised.
    """
    path = METRIC_API_PATH.get(stream_name)
    if path is None:
        return True

    try:
        url = client.url(path)
        client.request('get', url)
        return True
    except FrontappForbiddenError as exc:
        LOGGER.warning(
            "Stream '%s' does not have read permission, excluding from catalog. Detail: %s",
            stream_name,
            str(exc),
        )
        return False


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    from schemas and field_metadata in place.
    Raises FrontappForbiddenError if no streams are accessible.

    Note: No parent-child pruning is needed since tap-frontapp has no
    parent-child stream relationships — all streams are independent.
    """
    inaccessible_streams = [
        stream_name
        for stream_name in list(schemas.keys())
        if not _check_stream_access(client, stream_name)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    if not schemas:
        raise FrontappForbiddenError(
            "HTTP-error-code: 403, Error: Credentials lack read access to all supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "These streams have been excluded due to 403 Forbidden: %s",
            ", ".join(inaccessible_streams),
        )


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


def discover(client):
    """Run the discovery mode, prepare the catalog file and return the catalog.

    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    LOGGER.info("Schemas loaded: %s", list(schemas.keys()))

    _apply_access_checks(client, schemas, field_metadata)

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
