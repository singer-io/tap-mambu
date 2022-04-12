import os
import shutil
import json
from schema_fetcher.constants import OUTPUT_DIR_PATH, RESOURCES_LINK, CUSTOM_FIELDS_FIELD
from schema_fetcher.converters import convert_pascal_to_snake, convert_snake_to_pascal
from schema_fetcher.helpers import get_mambu_client, get_data_type_and_format
from schema_fetcher.custom_exceptions import ResourceFileNotFound, StreamJsonObjectNotFound


class Streams:
    def __init__(self):
        self.__swaggered_streams = {
            'gl_accounts': 'general_ledger_accounts',
            'gl_journal_entries': 'journal_entries',
            'interest_accrual_breakdown': 'accounting_interest_accrual',
        }

        self.__tap_streams_singular_form = {
            'branches': 'branch',
            'communications': 'communication',
            'centres': 'centre',
            'clients': 'client',
            'credit_arrangements': 'credit_arrangement',
            'custom_field_sets': 'custom_field_set',
            'deposit_accounts': 'deposit_account',
            'deposit_products': 'deposit_product',
            'deposit_transactions': 'deposit_transaction',
            'groups': 'group',
            'loan_accounts': 'loan_account',
            'loan_products': 'loan_product',
            'loan_transactions': 'loan_transaction',
            'tasks': 'task',
            'users': 'user',
            'gl_accounts': 'gl_account',
            'gl_journal_entries': 'gl_journal_entry',
            'index_rate_sources': 'index_rate_source',
            'installments': 'installment',
            'interest_accrual_breakdown': 'interest_accrual_breakdown',
        }

        self.__stream_json_obj = {
            'communications': 'communication_message',
            'custom_field_sets': 'custom_field_meta',
        }

    def get_stream_json_obj_form(self, stream_name):
        stream_json_obj = dict(self.__tap_streams_singular_form, **self.__stream_json_obj)

        if stream_name in self.__swaggered_streams.values():
            for tap_stream, swaggered_stream in self.__swaggered_streams.items():
                if swaggered_stream == stream_name:
                    return stream_json_obj[tap_stream]
        return stream_json_obj[stream_name]

    def get_stream_names_swaggered(self):
        streams_singular_form = list(self.__tap_streams_singular_form.keys())
        for idx, val in enumerate(streams_singular_form):
            streams_singular_form[idx] = self.__swaggered_streams.get(val, val)
        return streams_singular_form

    def convert_swaggered_to_tap_stream(self, swaggered_stream):
        if swaggered_stream in self.__swaggered_streams.values():
            for tap_stream, swaggered in self.__swaggered_streams.items():
                if swaggered == swaggered_stream:
                    return tap_stream
        return swaggered_stream

    def get_tap_streams_count(self):
        return len(self.__tap_streams_singular_form)

    def get_tap_streams_name(self):
        return self.__tap_streams_singular_form.keys()


streams = Streams()
client = get_mambu_client()


def get_stream_resource_file_paths():
    available_resources = client.request(method='GET',
                                         path=RESOURCES_LINK,
                                         version='remove_accept_header')
    file_paths = []
    found_streams_swagger_format = []
    swagger_format_streams = []

    tap_streams_swaggered = streams.get_stream_names_swaggered()
    for file_data in available_resources['items']:
        stream_name = file_data['hashValue'].lower()
        swagger_format_streams.append(stream_name)
        if stream_name in tap_streams_swaggered:
            found_streams_swagger_format.append(stream_name)
            file_paths.append(file_data['jsonPath'])

    if len(found_streams_swagger_format) != len(streams.get_stream_names_swaggered()):
        raise ResourceFileNotFound(f'Resource file not found for the following streams: '
                                   f'{", ".join([stream for stream in streams.get_stream_names_swaggered() if stream not in found_streams_swagger_format])}\n'
                                   f'Available swagger streams: {", ".join(swagger_format_streams)}')

    return zip(found_streams_swagger_format, file_paths)


def generate_json_objs(field_properties, all_schema_obj, type_object_nullable=True):
    full_schema = {**get_data_type_and_format('object', obj_type_nullable=type_object_nullable),
                   'additionalProperties': False,
                   'properties': {}}

    for field_name, field_prop in field_properties['properties'].items():
        field_name = convert_pascal_to_snake(field_name)
        if 'items' in field_prop:
            full_schema['properties'][field_name] = {'anyOf': [{**get_data_type_and_format(field_prop['type'],
                                                                                           field_prop.get('format',
                                                                                                          None)),
                                                                'items': {}},
                                                               {'type': 'null'}]}
            if 'originalRef' in field_prop['items']:
                full_schema['properties'][field_name]['anyOf'][0]['items'] = generate_json_objs(
                    all_schema_obj[field_prop['items']['originalRef']],
                    all_schema_obj,
                    False)
            else:
                full_schema['properties'][field_name]['anyOf'][0]['items'] = get_data_type_and_format(
                    field_prop['items']['type'],
                    field_prop.get('format', None),
                    obj_type_nullable=False)
        else:
            if 'originalRef' in field_prop:
                full_schema['properties'][field_name] = generate_json_objs(all_schema_obj[field_prop['originalRef']],
                                                                           all_schema_obj)
            else:
                full_schema['properties'][field_name] = get_data_type_and_format(field_prop['type'],
                                                                                 field_prop.get('format', None))
    full_schema['properties'] = {k: v for k, v in sorted(full_schema['properties'].items())}
    return full_schema


def generate_json_schema(stream_name, file_path):
    stream_data = client.request(method='GET',
                                 path=f'swagger/{file_path}',
                                 version='remove_accept_header')

    stream_fields_w_refs = stream_data['definitions']
    json_stream_name_form = convert_snake_to_pascal(streams.get_stream_json_obj_form(stream_name))

    if json_stream_name_form not in stream_fields_w_refs:
        raise StreamJsonObjectNotFound(f'Json object for "{stream_name}" stream not found. '
                                       f'Available objects: {", ".join(stream_fields_w_refs.keys())}')

    stream_schema = generate_json_objs(stream_fields_w_refs[json_stream_name_form], stream_fields_w_refs, False)

    stream_schema['properties'].update(CUSTOM_FIELDS_FIELD)
    return stream_schema


def main():
    if os.path.exists(OUTPUT_DIR_PATH):
        shutil.rmtree(OUTPUT_DIR_PATH)
    os.mkdir(OUTPUT_DIR_PATH)

    for stream_name, file_path in get_stream_resource_file_paths():
        file_dir_path = os.path.join(OUTPUT_DIR_PATH, f'{streams.convert_swaggered_to_tap_stream(stream_name)}.json')
        with open(file_dir_path, 'w') as f:
            json.dump(generate_json_schema(stream_name, file_path), f, indent=2)

    created_files = [file_name for file_name in os.listdir(OUTPUT_DIR_PATH) if
                     os.path.isfile(os.path.join(OUTPUT_DIR_PATH, file_name))]

    if len(created_files) != streams.get_tap_streams_count():
        created_files_name_only = [created_file.replace('.json', '') for created_file in created_files]
        for tap_stream in streams.get_tap_streams_name():
            if tap_stream not in created_files_name_only:
                raise FileNotFoundError(f'Json Schema for "{tap_stream}" stream not found')


if __name__ == '__main__':
    main()
