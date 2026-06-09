import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mambu.helpers.schema import get_schemas, STREAMS
from tap_mambu.helpers.client import (
    MambuUnauthorizedError,
    MambuForbiddenError,
    MambuNoAuditApikeyInConfig,
    MambuError,
)

LOGGER = singer.get_logger()

# Minimal probe config for each stream: method, API path, version, and optional apikey_type.
# Streams with 'parent' are child streams that inherit accessibility from their parent.
# Defaults: version='v2', apikey_type=None.
_MINIMAL_POST_BODY = {
    "sortingCriteria": {"field": "encodedKey", "order": "ASC"},
    "filterCriteria": [],
}
_PROBE_PARAMS = {"pageSize": 1, "paginationDetails": "OFF", "detailsLevel": "FULL"}

STREAM_PROBE_CONFIG = {
    "activities":                 {"method": "GET",  "path": "activities",                           "version": "v1"},
    "audit_trail":                {"method": "GET",  "path": "v1/events",                            "version": "v1", "apikey_type": "audit"},
    "branches":                   {"method": "GET",  "path": "branches"},
    "cards":                      {"parent": "deposit_accounts"},
    "centres":                    {"method": "GET",  "path": "centres"},
    "clients":                    {"method": "POST", "path": "clients:search"},
    "communications":             {"method": "POST", "path": "communications/messages:search"},
    "credit_arrangements":        {"method": "GET",  "path": "creditarrangements"},
    "custom_field_sets":          {"method": "GET",  "path": "customfieldsets"},
    "deposit_accounts":           {"method": "POST", "path": "deposits:search"},
    "deposit_products":           {"method": "GET",  "path": "depositproducts"},
    "deposit_transactions":       {"method": "POST", "path": "deposits/transactions:search"},
    "gl_accounts":                {"method": "GET",  "path": "glaccounts"},
    "gl_journal_entries":         {"method": "POST", "path": "gljournalentries:search"},
    "groups":                     {"method": "POST", "path": "groups:search"},
    "index_rate_sources":         {"method": "GET",  "path": "indexratesources"},
    "installments":               {"method": "GET",  "path": "installments"},
    "interest_accrual_breakdown": {"method": "POST", "path": "accounting/interestaccrual:search"},
    "loan_accounts":              {"method": "POST", "path": "loans:search"},
    "loan_products":              {"method": "GET",  "path": "loanproducts"},
    "loan_repayments":            {"parent": "loan_accounts"},
    "loan_transactions":          {"method": "POST", "path": "loans/transactions:search"},
    "tasks":                      {"method": "GET",  "path": "tasks"},
    "users":                      {"method": "GET",  "path": "users"},
}


def check_stream_access(client, stream_name) -> bool:
    """
    Probes a stream endpoint with minimal params to verify the credentials
    have access to that stream.
    Returns False on 401/403 (auth denied) or missing audit API key.
    Returns True on success or any other non-auth API error — a non-auth
    response means the server accepted the credentials.
    Should only be called for streams that have a direct probe config (no 'parent' key).
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
                           apikey_type=apikey_type, params=_PROBE_PARAMS,
                           endpoint=stream_name)
        else:
            client.request(method="POST", path=path, version=version,
                           apikey_type=apikey_type, json=_MINIMAL_POST_BODY,
                           params=_PROBE_PARAMS, endpoint=stream_name)
        return True
    except (MambuUnauthorizedError, MambuForbiddenError, MambuNoAuditApikeyInConfig):
        return False
    except MambuError:
        return True


def discover(client) -> Catalog:
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
        elif not check_stream_access(client, stream_name):
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
