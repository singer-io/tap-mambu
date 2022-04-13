from singer.utils import load_json
from tap_mambu.client import MambuClient
from schema_fetcher.constants import CLIENT_CONFIG_FILE_PATH, TAP_TYPES_AND_FORMATS, TAP_FORMATS


def get_mambu_client():
    config = load_json(CLIENT_CONFIG_FILE_PATH)
    return MambuClient(username=config.get('username'),
                       password=config.get('password'),
                       apikey='',
                       subdomain=config.get('subdomain'),
                       apikey_audit='',
                       page_size='',
                       user_agent='')


def get_data_type_and_format(field_type, field_format=None, obj_type_nullable=True):
    tap_field_type = TAP_TYPES_AND_FORMATS[field_type]
    if not obj_type_nullable and isinstance(tap_field_type, list):
        tap_field_type = tap_field_type[1]

    type_and_format = {'type': tap_field_type}
    if field_format and TAP_FORMATS[field_format]:
        type_and_format['format'] = TAP_FORMATS[field_format]
    if field_type == 'number':
        type_and_format['format'] = 'singer.decimal'
    return type_and_format
