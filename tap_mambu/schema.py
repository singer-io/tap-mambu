import os
import json
from singer import metadata

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata
STREAMS = {
    'branches': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'cards': {
        'key_properties': ['deposit_id', 'reference_token'],
        'replication_method': 'FULL_TABLE'
    },
    'communications': {
        'key_properties': ['encoded_key'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['creation_date']
    },
    'centres': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'clients': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'credit_arrangements': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'custom_field_sets': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'deposit_accounts': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'deposit_products': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'deposit_transactions': {
        'key_properties': ['encoded_key'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['creation_date']
    },
    'groups': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'loan_accounts': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'loan_products': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'loan_transactions': {
        'key_properties': ['encoded_key'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['creation_date']
    },
    'tasks': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'users': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
    },
    'gl_accounts': {
        'key_properties': ['gl_code'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['last_modified_date']
     },
     'gl_journal_entries': {
        'key_properties': ['entry_id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['booking_date']
     }
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
