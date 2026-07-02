import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mambu.helpers.schema import get_schemas, STREAMS
from tap_mambu.helpers.client import (
    MambuUnauthorizedError,
    MambuForbiddenError,
    MambuNotFoundError,
    MambuMethodNotAllowedError,
    MambuNoAuditApikeyInConfig,
    MambuError,
    MambuApiLimitError,
)
from tap_mambu.helpers.constants import MINIMAL_POST_BODY, PROBE_PARAMS, STREAM_PROBE_CONFIG

LOGGER = singer.get_logger()


def check_stream_access(client, stream_name) -> bool:
    """
    Probes a stream endpoint to verify credential access.
    Returns False on auth/permission errors (401, 403, 404, 405, missing audit key);
    True otherwise. Only valid for streams with a direct probe config (no 'parent' key).
    """
    probe = STREAM_PROBE_CONFIG.get(stream_name)
    if not probe or "parent" in probe:
        raise ValueError(f"Stream '{stream_name}' does not have a direct probe configuration.")

    path = probe["path"]
    method = probe["method"]
    version = probe.get("version", "v2")
    apikey_type = probe.get("apikey_type")

    try:
        if method == "GET":
            client.request(method="GET", path=path, version=version,
                           apikey_type=apikey_type, params=PROBE_PARAMS,
                           endpoint=stream_name)
        else:
            client.request(method="POST", path=path, version=version,
                           apikey_type=apikey_type, json=MINIMAL_POST_BODY,
                           params=PROBE_PARAMS, endpoint=stream_name)
        return True
    except (MambuUnauthorizedError, MambuForbiddenError,
            MambuNotFoundError, MambuMethodNotAllowedError,
            MambuNoAuditApikeyInConfig):
        return False
    except (MambuError, MambuApiLimitError):
        return True


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Remove child streams from the catalog whose parent stream was excluded."""
    for stream_name, probe_cfg in list(STREAM_PROBE_CONFIG.items()):
        parent_name = probe_cfg.get("parent")
        if stream_name in schemas and parent_name and parent_name not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                parent_name,
            )
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Remove inaccessible top-level streams and dependent children in place."""
    inaccessible_streams = [
        stream_name
        for stream_name, probe_cfg in STREAM_PROBE_CONFIG.items()
        if stream_name in schemas and "parent" not in probe_cfg and client is not None
        and not check_stream_access(client, stream_name)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    accessible_streams = [s for s in STREAMS if s in schemas]

    if not accessible_streams:
        raise MambuForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have "
            "'read' access to any supported streams."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client=None) -> Catalog:
    """Run discovery and exclude streams the credentials cannot read."""
    schemas, field_metadata = get_schemas()
    missing_probe_cfg = set(schemas) - set(STREAM_PROBE_CONFIG)
    if missing_probe_cfg:
        raise ValueError(f"Missing STREAM_PROBE_CONFIG entries for streams: {sorted(missing_probe_cfg)}")
    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name in schemas:
        stream = STREAMS[stream_name]
        schema = Schema.from_dict(schemas[stream_name])
        mdata = metadata.to_map(field_metadata[stream_name])

        if stream.get("replication_method") == "INCREMENTAL":
            for field_name in stream.get("replication_keys", []):
                metadata.write(mdata, ("properties", field_name), "inclusion", "automatic")

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=stream["key_properties"],
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))

    return catalog
