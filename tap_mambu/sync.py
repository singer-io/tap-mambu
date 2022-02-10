from _datetime import timedelta
from datetime import datetime

import singer
from singer import Transformer, metadata, metrics, utils
from singer.utils import strftime, strptime_to_utc

from tap_mambu.tap_mambu_refactor import sync_endpoint_refactor
from tap_mambu.transform import transform_json, transform_activities

LOGGER = singer.get_logger()
LOOKBACK_DEFAULT = 14

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, sub_type, default):
    if (state is None) or ('bookmarks' not in state):
        return default

    if sub_type == 'self':
        return (state.get('bookmarks', {}).get(stream, default))
    else:
        return (state.get('bookmarks', {}).get(stream, {}).get(sub_type, default))


def write_bookmark(state, stream, sub_type, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    if stream not in state['bookmarks']:
        state['bookmarks'][stream] = {}
    if sub_type == 'self':
        state['bookmarks'][stream] = value
    else:
        if sub_type not in state['bookmarks'][stream]:
            state['bookmarks'][stream][sub_type] = {}
        state['bookmarks'][stream][sub_type] = value
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer() as transformer:
                transformed_record = transformer.transform(record,
                                               schema,
                                               stream_metadata)

                # Reset max_bookmark_value to new value if higher
                if bookmark_field and (bookmark_field in transformed_record):
                    bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
                    if max_bookmark_value:
                        max_bookmark_value_dttm = strptime_to_utc(max_bookmark_value)
                        if bookmark_dttm > max_bookmark_value_dttm:
                            max_bookmark_value = transformed_record[bookmark_field]
                    else:
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, time_extracted=time_extracted)
                            counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, len(records)


# Sync a specific parent or child endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  api_version,
                  api_method,
                  static_params,
                  sub_type,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  data_key=None,
                  body=None,
                  id_fields=None,
                  parent=None,
                  parent_id=None,
                  apikey_type=None):


    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    number_last_occurrence = 0
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, sub_type, 0)
        max_bookmark_value = last_integer
    else:
        if stream_name == 'audit_trail':
            audit_trail_bookmark = get_bookmark(state, stream_name, sub_type, [start_date, 0])
            last_datetime, number_last_occurrence = audit_trail_bookmark if type(audit_trail_bookmark) == list \
                else (audit_trail_bookmark, 0)
        else:
            last_datetime = get_bookmark(state, stream_name, sub_type, start_date)
        max_bookmark_value = last_datetime

    write_schema(catalog, stream_name)

    # pagination: loop thru all pages of data
    # Pagination reference: https://api.mambu.com/?http#pagination
    # Each page has an offset (starting value) and a limit (batch size, number of records)
    # Increase the "offset" by the "limit" for each batch.
    # Continue until the "record_count" returned < "limit" is null/zero or
    offset = 0 # Starting offset value for each batch API call
    limit = client.page_size # Batch size; Number of records per API call
    total_records = 0 # Initialize total
    record_count = limit # Initialize, reset for each API call

    # Initialize next_max_date and number_last_occurrence parameters used in the request for audit_trail
    next_max_date = static_params['occurred_at[gte]'] if stream_name == 'audit_trail' else None

    while record_count == limit: # break out of loop when record_count < limit (or not data returned)
        params = {
            'offset': offset,
            'limit': limit,
            **static_params # adds in endpoint specific, sort, filter params
        }

        if stream_name == 'audit_trail':
            del params['offset']
            del params['limit']
            params['from'] = number_last_occurrence
            params['size'] = limit
            params['occurred_at[gte]'] = next_max_date

        if bookmark_query_field:
            if bookmark_type == 'datetime':
                params[bookmark_query_field] = last_datetime
            elif bookmark_type == 'integer':
                params[bookmark_query_field] = last_integer

        LOGGER.info('Stream: {}, Type: {} - Sync start {}'.format(
            stream_name, sub_type,
            'since: {}, '.format(last_datetime) if bookmark_query_field else ''))

        # Squash params to query-string params
        querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        LOGGER.info('URL for {} ({}, {}): {}/{}?{}'\
            .format(stream_name, api_method, api_version, client.base_url, path, querystring))
        if body is not None:
            LOGGER.info('body = {}'.format(body))

        # API request data
        data = client.request(
            method=api_method,
            path=path,
            version=api_version,
            apikey_type=apikey_type,
            params=querystring,
            endpoint=stream_name,
            json=body)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        if not data or data is None or data == []:
            record_count = 0
            LOGGER.warning('Stream: {} - NO DATA RESULTS')
            break # NO DATA

        if stream_name == 'audit_trail':
            data = data['events']

        # Transform data with transform_json from transform.py
        #  This function converts camelCase to snake_case for fieldname keys.
        # The data_key may identify array/list of records below the <root> element
        # LOGGER.info('data = {}'.format(data)) # TESTING, comment out
        transformed_data = [] # initialize the record list
        data_list = []
        # If a single record dictionary, append to a list[]
        if isinstance(data, dict):
            data_list.append(data)
            data = data_list
        if data_key is None:
            transformed_data = transform_json(data, stream_name)
        elif data_key in data:
            transformed_data = transform_json(data, data_key)[data_key]

        if stream_name == 'activities':
            transformed_data = transform_activities(transformed_data)

        # LOGGER.info('transformed_data = {}'.format(transformed_data))  # TESTING, comment out
        if not transformed_data or transformed_data is None:
            record_count = 0
            LOGGER.warning('Stream: {} - NO TRANSFORMED DATA RESULTS')
            break # No data results

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value, record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            last_integer=last_integer,
            parent=parent,
            parent_id=parent_id)

        total_records = total_records + record_count

        # Loop thru parent batch records for each children objects (if should stream)
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                should_stream, last_stream_child = should_sync_stream(get_selected_streams(catalog),
                                                            None,
                                                            child_stream_name)
                if should_stream:
                    # For each parent record
                    for record in transformed_data:
                        i = 0
                        # Set parent_id
                        for id_field in id_fields:
                            if i == 0:
                                parent_id_field = id_field
                            if id_field == 'id':
                                parent_id_field = id_field
                            i = i + 1
                        parent_id = record.get(parent_id_field)

                        # sync_endpoint for child
                        LOGGER.info('Syncing: {}, parent_stream: {}, parent_id: {}'.format(
                            child_stream_name,
                            stream_name,
                            parent_id))
                        child_path = child_endpoint_config.get('path').format(str(parent_id))
                        child_total_records = sync_endpoint(
                            client=client,
                            catalog=catalog,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            api_version=child_endpoint_config.get('api_version', 'v2'),
                            api_method=child_endpoint_config.get('api_method', 'GET'),
                            static_params=child_endpoint_config.get('params', {}),
                            sub_type=sub_type,
                            bookmark_query_field=child_endpoint_config.get('bookmark_query_field'),
                            bookmark_field=child_endpoint_config.get('bookmark_field'),
                            bookmark_type=child_endpoint_config.get('bookmark_type'),
                            data_key=child_endpoint_config.get('data_key', None),
                            body=child_endpoint_config.get('body', None),
                            id_fields=child_endpoint_config.get('id_fields'),
                            parent=child_endpoint_config.get('parent'),
                            parent_id=parent_id)
                        LOGGER.info('Synced: {}, parent_id: {}, total_records: {}'.format(
                            child_stream_name,
                            parent_id,
                            child_total_records))


        # to_rec: to record; ending record for the batch
        to_rec = offset + limit
        if record_count < limit:
            to_rec = total_records

        if stream_name == 'audit_trail':
            next_max_date = transformed_data[-1]['occurred_at']
            index = -2
            number_last_occurrence = 1
            while index >= -len(transformed_data) and transformed_data[index]['occurred_at'] == next_max_date:
                number_last_occurrence += 1
                index -= 1

        LOGGER.info('{} - Synced records: {} to {}'.format(
            stream_name,
            offset,
            to_rec))
        # Pagination: increment the offset by the limit (batch-size)
        offset = offset + limit

        # End: while record_count == limit

    # Update the state with the max_bookmark_value for the stream
    if bookmark_field:
        write_bookmark(state,
                       stream_name,
                       sub_type,
                       max_bookmark_value if stream_name != 'audit_trail'
                       else (max_bookmark_value, number_last_occurrence))

    # Return total_records across all batches
    return total_records


# Review catalog and make a list of selected streams
def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# Review last_stream (last currently syncing stream), if any,
#  and continue where it left off in the selected streams.
# Or begin from the beginning, if no last_stream, and sync
#  all selected steams.
# Returns should_sync_stream (true/false) and last_stream.
def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream


def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']
    # LOGGER.info('start_date = {}'.format(start_date))

    # Get datetimes for endpoint parameters
    communications_dttm_str = get_bookmark(state, 'communications', 'self', start_date)
    communications_dt_str = transform_datetime(communications_dttm_str)[:10]
    # LOGGER.info('communications bookmark_date = {}'.format(communications_dt_str))

    deposit_transactions_dttm_str = get_bookmark(state, 'deposit_transactions', 'self', start_date)
    deposit_transactions_dt_str = transform_datetime(deposit_transactions_dttm_str)[:10]
    # LOGGER.info('deposit_transactions bookmark_date = {}'.format(deposit_transactions_dt_str))

    loan_transactions_dttm_str = get_bookmark(state, 'loan_transactions', 'self', start_date)
    loan_transactions_dt_str = transform_datetime(loan_transactions_dttm_str)[:10]
    loan_transactions_dttm = strptime_to_utc(loan_transactions_dt_str)

    clients_dttm_str = get_bookmark(state, 'clients', 'self', start_date)
    clients_dt_str = transform_datetime(clients_dttm_str)[:10]

    groups_dttm_str = get_bookmark(state, 'groups', 'self', start_date)
    groups_dt_str = transform_datetime(groups_dttm_str)[:10]

    loan_accounts_dttm_str = get_bookmark(state, 'loan_accounts', 'self', start_date)
    loan_accounts_dt_str = transform_datetime(loan_accounts_dttm_str)[:10]

    deposit_accounts_dttm_str = get_bookmark(state, 'deposit_accounts', 'self', start_date)
    deposit_accounts_dt_str = transform_datetime(deposit_accounts_dttm_str)[:10]

    lookback_days = int(config.get('lookback_window', LOOKBACK_DEFAULT))
    lookback_date = utils.now() - timedelta(lookback_days)
    if loan_transactions_dttm > lookback_date:
        loan_transactions_dt_str = transform_datetime(strftime(lookback_date))[:10]
    # LOGGER.info('loan_transactions bookmark_date = {}'.format(loan_transactions_dt_str))

    # endpoints: API URL endpoints to be called
    # properties:
    #   <root node>: Plural stream name for the endpoint
    #   path: API endpoint relative path, when added to the base URL, creates the full path
    #   api_version: v1 or v2 (default v2).
    #   api_method: GET or POST (default GET).
    #   params: Query, sort, and other endpoint specific parameters
    #   data_key: JSON element containing the records for the endpoint
    #   bookmark_query_field: Typically a date-time field used for filtering the query
    #   bookmark_field: Replication key field, typically a date-time, used for filtering the results
    #        and setting the state
    #   bookmark_type: Data type for bookmark, integer or datetime
    #   id_fields: Primary key (and other IDs) from the Parent stored when store_ids is true.
    #   children: A collection of child endpoints (where the endpoint path includes the parent id)
    #   parent: On each of the children, the singular stream name for parent element
    #   Details Level: https://api.mambu.com/?http#detail-level, FULL includes custom fields

    endpoints = {
        'branches': {
            'path': 'branches',
            'api_version': 'v2',
            'api_method': 'GET',
            'params': {
                'sortBy': 'lastModifiedDate:ASC',
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'communications': {
            'path': 'communications/messages:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL'
            },
            'body': [
                {
                    'field': 'state',
                    'operator': 'EQUALS',
                    'value': 'SENT'
                },
                {
                    'field': 'creationDate',
                    'operator': 'AFTER',
                    'value': communications_dt_str
                }
            ],
            'bookmark_field': 'creation_date',
            'bookmark_type': 'datetime',
            'id_fields': ['encoded_key']
        },
        'centres': {
            'path': 'centres',
            'api_version': 'v2',
            'api_method': 'GET',
            'params': {
                'sortBy': 'lastModifiedDate:ASC',
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'clients': {
            'path': 'clients:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL'
            },
            'body': {
                "sortingCriteria": {
                    "field": "lastModifiedDate",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "lastModifiedDate",
                        "operator": "AFTER",
                        "value": clients_dt_str
                    }
                ]
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'credit_arrangements': {
            'path': 'creditarrangements',
            'api_version': 'v2',
            'api_method': 'GET',
            'params': {
                'sortBy': 'creationDate:ASC',
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'custom_field_sets': {
            'path': 'customfieldsets',
            'api_version': 'v1',
            'api_method': 'GET',
            'params': {},
            'id_fields': ['id']
        },
        'deposit_accounts': {
            'path': 'deposits:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL'
            },
            'body': {
                "sortingCriteria": {
                    "field": "lastModifiedDate",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "lastModifiedDate",
                        "operator": "AFTER",
                        "value": deposit_accounts_dt_str
                    }
                ]
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id'],
            'store_ids': True,
            'children': {
                'cards': {
                    'path': 'deposits/{}/cards',
                    'api_version': 'v2',
                    'api_method': 'GET',
                    'params': {
                        'detailsLevel': 'FULL'
                    },
                    'id_fields': ['deposit_id', 'reference_token'],
                    'parent': 'deposit'
                }
            }
        },
        'deposit_products': {
            'path': 'savingsproducts',
            'api_version': 'v1',
            'api_method': 'GET',
            'params': {
                "fullDetails": True
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'deposit_transactions': {
            'path': 'deposits/transactions:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL'
            },
            'body': {
                "sortingCriteria": {
                    "field": "creationDate",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "creationDate",
                        "operator": "AFTER",
                        "value": deposit_transactions_dt_str
                    }
                ]
            },
            'bookmark_field': 'creation_date',
            'bookmark_type': 'datetime',
            'id_fields': ['encoded_key']
        },
        'groups': {
            'path': 'groups:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL'
            },
            'body': {
                "sortingCriteria": {
                    "field": "lastModifiedDate",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "lastModifiedDate",
                        "operator": "AFTER",
                        "value": groups_dt_str
                    }
                ]
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'loan_accounts': {
            'path': 'loans:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'body': {
                "sortingCriteria": {
                    "field": "lastModifiedDate",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "lastModifiedDate",
                        "operator": "AFTER",
                        "value": loan_accounts_dt_str
                    }
                ]
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id'],
            'children': {
                'loan_repayments': {
                    'path': 'loans/{}/repayments',
                    'api_version': 'v1',
                    'api_method': 'GET',
                    'params': {
                        'detailsLevel': 'FULL',
                        'paginationDetails': 'ON'
                    },
                    'id_fields': ['encoded_key'],
                    'parent': 'loan_accounts'
                }
            }
        },
        'loan_products': {
            'path': 'loanproducts',
            'api_version': 'v1',
            'api_method': 'GET',
            'params': {
                "fullDetails": True
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'loan_transactions': {
            'path': 'loans/transactions:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL'
            },
            'body': {
                "sortingCriteria": {
                    "field": "creationDate",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "creationDate",
                        "operator": "AFTER",
                        "value": loan_transactions_dt_str
                    }
                ]
            },
            'bookmark_field': 'creation_date',
            'bookmark_type': 'datetime',
            'id_fields': ['encoded_key']
        },
        'tasks': {
            'path': 'tasks',
            'api_version': 'v2',
            'api_method': 'GET',
            'params': {
                'sortBy': 'lastModifiedDate:ASC',
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'users': {
            'path': 'users',
            'api_version': 'v2',
            'api_method': 'GET',
            'params': {
                'sortBy': 'lastModifiedDate:ASC',
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'id_fields': ['id']
        },
        'gl_accounts': {
            'path': 'glaccounts',
            'api_version': 'v1',
            'api_method': 'GET',
            'params': {
                'type': '{sub_type}'
            },
            'id_fields': ['gl_code'],
            'bookmark_field': 'last_modified_date',
            'bookmark_type': 'datetime',
            'sub_types': ['ASSET', 'LIABILITY', 'EQUITY', 'INCOME', 'EXPENSE']
        },
        'gl_journal_entries': {
            'path': 'gljournalentries/search',
            'api_version': 'v1',
            'api_method': 'POST',
            'body': {
                "sortDetails": {
                    "sortingColumn": "ENTRY_ID",
                    "sortingOrder": "ASCENDING"
                },
                "filterConstraints": [
                    {
                        "filterSelection": "CREATION_DATE",
                        "filterElement": "BETWEEN",
                        "value": '{gl_journal_entries_from_dt_str}',
                        "secondValue": "{now_date_str}"
                    }
                ]
            },
            'id_fields': ['entry_id'],
            'bookmark_field': 'creation_date',
            'bookmark_type': 'datetime'
        },
        'activities': {
            'path': 'activities',
            'api_version': 'v1',
            'api_method': 'GET',
            'params' : {
                'from': '{activities_from_dt_str}',
                'to': '{now_date_str}'
            },
            'id_fields': ['encoded_key'],
            'bookmark_field': 'timestamp',
            'bookmark_type': 'datetime'
        },
        'index_rate_sources': {
            'path': 'indexratesources',
            'api_version': 'v2',
            'api_method': 'GET',
            'id_fields': ['encoded_key'],
            'params': {}
        },
        'installments': {
            'path': 'installments',
            'api_version': 'v2',
            'api_method': 'GET',
            'id_fields': ['encoded_key'],
            'params': {
                'dueFrom': '{installments_from_dt_str}',
                'dueTo': '{now_date_str}'
            },
            'bookmark_field': 'last_paid_date',
            'bookmark_type': 'datetime'
        },
        'audit_trail': {
            'path': 'v1/events',
            'api_version': 'v1',
            'api_method': 'GET',
            'id_fields': [],
            'apikey_type': 'audit',
            'params': {
                'sort_order': 'asc',
                'occurred_at[gte]': '{audit_trail_from_dt_str}',
                'occurred_at[lte]': '{audit_trail_to_dt_str}'
            },
            'bookmark_field': 'occurred_at',
            'bookmark_type': 'datetime'
        }
    }

    selected_streams = get_selected_streams(catalog)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))

    # Start syncing from last/currently syncing stream
    if last_stream in selected_streams:
        selected_streams = selected_streams[selected_streams.index(last_stream):] + selected_streams[:selected_streams.index(last_stream)]

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name in selected_streams:
        endpoint_config = endpoints.get(stream_name)
        if endpoint_config is None:
            # null endpoint_config signifies that this is a child stream
            continue
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)

        if should_stream:
            # loop through each sub type
            sub_types = endpoint_config.get('sub_types', ['self'])
            for sub_type in sub_types:
                LOGGER.info('START Syncing: {}, Type: {}'.format(stream_name, sub_type))

                # Now date
                if stream_name == 'gl_journal_entries':
                    now_date_str = strftime(utils.now())[:10]
                    gl_journal_entries_from_dttm_str = get_bookmark(
                        state, 'gl_journal_entries', sub_type, start_date)
                    gl_journal_entries_from_dt_str = transform_datetime(
                        gl_journal_entries_from_dttm_str)[:10]
                    gl_journal_entries_from_param = endpoint_config.get(
                        'body', {}).get('filterConstraints', {})[0].get('value')
                    if gl_journal_entries_from_param:
                        endpoint_config['body']['filterConstraints'][0]['value'] = gl_journal_entries_from_dt_str
                    gl_journal_entries_to_param = endpoint_config.get(
                        'body', {}).get('filterConstraints', {})[0].get('secondValue')
                    if gl_journal_entries_to_param:
                        endpoint_config['body']['filterConstraints'][0]['secondValue'] = now_date_str

                if stream_name == 'activities':
                    now_date_str = strftime(utils.now())[:10]
                    activities_from_dttm_str = get_bookmark(
                        state, 'activities', sub_type, start_date)
                    activities_from_dt_str = transform_datetime(
                        activities_from_dttm_str)[:10]
                    activities_from_param = endpoint_config.get(
                        'params', {}).get('from')
                    if activities_from_param:
                        endpoint_config['params']['from'] = activities_from_dt_str
                    activities_to_param = endpoint_config.get(
                        'params', {}).get('to')
                    if activities_to_param:
                        endpoint_config['params']['to'] = now_date_str

                if stream_name == 'installments':
                    now_date_str = strftime(utils.now())[:10]
                    installments_from_dttm_str = start_date
                    installments_from_dt_str = transform_datetime(
                        installments_from_dttm_str)[:10]
                    installments_from_param = endpoint_config.get(
                        'params', {}).get('dueFrom')
                    if installments_from_param:
                        endpoint_config['params']['dueFrom'] = installments_from_dt_str
                    installments_to_param = endpoint_config.get(
                        'params', {}).get('dueTo')
                    if installments_to_param:
                        endpoint_config['params']['dueTo'] = now_date_str

                if stream_name == 'audit_trail':
                    now_date_str = strftime(utils.now())
                    audit_trail_bookmark = get_bookmark(state, 'audit_trail', sub_type, start_date)
                    audit_trail_from_dttm_str = audit_trail_bookmark[0] if type(audit_trail_bookmark) == list \
                        else audit_trail_bookmark
                    audit_trail_from_dt_str = transform_datetime(
                        audit_trail_from_dttm_str)
                    audit_trail_from_param = endpoint_config.get(
                        'params', {}).get('occurred_at[gte]')
                    if audit_trail_from_param:
                        endpoint_config['params']['occurred_at[gte]'] = audit_trail_from_dt_str
                    audit_trail_to_dt_str = endpoint_config.get('params', {}).get('occurred_at[lte]')
                    if audit_trail_to_dt_str:
                        endpoint_config['params']['occurred_at[lte]'] = now_date_str

                update_currently_syncing(state, stream_name)
                path = endpoint_config.get('path')
                sub_type_param = endpoint_config.get('params', {}).get('type')
                if sub_type_param:
                    endpoint_config['params']['type'] = sub_type

                if stream_name in ["loan_accounts"]:
                    total_records = sync_endpoint_refactor(
                        client=client,
                        catalog=catalog,
                        state=state,
                        stream_name=stream_name,
                        sub_type=sub_type,
                        config=config
                    )
                else:
                    total_records = sync_endpoint(
                        client=client,
                        catalog=catalog,
                        state=state,
                        start_date=start_date,
                        stream_name=stream_name,
                        path=path,
                        endpoint_config=endpoint_config,
                        api_version=endpoint_config.get('api_version', 'v2'),
                        api_method=endpoint_config.get('api_method', 'GET'),
                        static_params=endpoint_config.get('params', {}),
                        sub_type=sub_type,
                        bookmark_query_field=endpoint_config.get('bookmark_query_field'),
                        bookmark_field=endpoint_config.get('bookmark_field'),
                        bookmark_type=endpoint_config.get('bookmark_type'),
                        data_key=endpoint_config.get('data_key', None),
                        body=endpoint_config.get('body', None),
                        id_fields=endpoint_config.get('id_fields'),
                        apikey_type=endpoint_config.get('apikey_type', None)
                    )

                update_currently_syncing(state, None)
                LOGGER.info('Synced: {}, total_records: {}'.format(
                                stream_name,
                                total_records))
                LOGGER.info('FINISHED Syncing: {}'.format(stream_name))
