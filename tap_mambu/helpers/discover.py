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


def discover(client=None) -> Catalog:
    """Run discovery mode, probing each stream endpoint to verify access.
    Streams that return an auth error are excluded from the catalog.
    Child streams (cards, loan_repayments) are included only if their
    parent stream is accessible.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])
    accessible_streams = set()

    # Two-pass: probe top-level streams first so parent accessibility is known
    # before child streams are evaluated.
    missing_probe_cfg = set(schemas) - set(STREAM_PROBE_CONFIG)
    if missing_probe_cfg:
        raise ValueError(f"Missing STREAM_PROBE_CONFIG entries for streams: {sorted(missing_probe_cfg)}")
    top_level = {name: cfg for name, cfg in STREAM_PROBE_CONFIG.items() if "parent" not in cfg}
    child = {name: cfg for name, cfg in STREAM_PROBE_CONFIG.items() if "parent" in cfg}

    for stream_name, probe_cfg in {**top_level, **child}.items():
        if stream_name not in schemas:
            continue

        if "parent" in probe_cfg:
            parent_name = probe_cfg["parent"]
            if parent_name not in accessible_streams:
                LOGGER.warning(
                    "Stream '%s' will be excluded from the catalog because its "
                    "parent stream '%s' is not accessible.",
                    stream_name,
                    parent_name,
                )
                continue
        elif client is not None and not check_stream_access(client, stream_name):
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )
            continue

        stream = STREAMS[stream_name]
        schema = Schema.from_dict(schemas[stream_name])
        mdata = metadata.to_map(field_metadata[stream_name])

        if stream.get("replication_method") == "INCREMENTAL":
            for field_name in stream.get("replication_keys", []):
                metadata.write(mdata, ("properties", field_name), "inclusion", "automatic")

        accessible_streams.add(stream_name)
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=stream["key_properties"],
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))

    if not catalog.streams:
        raise Exception(
            "The credentials do not have read access to any of the supported streams. "
            "Verify that the API credentials have the required permissions."
        )

    return catalog
