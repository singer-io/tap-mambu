import re
import singer
from datetime import datetime, timezone
from singer import write_state, metadata
from singer.utils import strptime_to_utc


def get_bookmark(state, stream, sub_type, default):
    if (state is None) or ('bookmarks' not in state):
        return default

    if sub_type == 'self':
        return state.get('bookmarks', {}).get(stream, default)
    return state.get('bookmarks', {}).get(stream, {}).get(sub_type, default)


def write_bookmark(state, stream, sub_type, value):
    def _normalize_bookmark_value(bookmark_value):
        if isinstance(bookmark_value, datetime):
            return bookmark_value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if isinstance(bookmark_value, list):
            return [_normalize_bookmark_value(item) for item in bookmark_value]
        if isinstance(bookmark_value, dict):
            return {
                key: _normalize_bookmark_value(item_value)
                for key, item_value in bookmark_value.items()
            }
        return bookmark_value

    def _max_datetime_bookmark(existing_value, new_value):
        existing_value = _normalize_bookmark_value(existing_value)
        new_value = _normalize_bookmark_value(new_value)
        if existing_value is None:
            return new_value
        try:
            existing_dt = strptime_to_utc(existing_value)
            new_dt = strptime_to_utc(new_value)
            return existing_value if existing_dt >= new_dt else new_value
        except (TypeError, ValueError):
            return new_value

    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    if stream not in state['bookmarks']:
        state['bookmarks'][stream] = {}
    if sub_type == 'self':
        existing_value = state['bookmarks'].get(stream)
        state['bookmarks'][stream] = _max_datetime_bookmark(existing_value, value)
    else:
        if sub_type not in state['bookmarks'][stream]:
            state['bookmarks'][stream][sub_type] = {}
        state['bookmarks'][stream][sub_type] = _normalize_bookmark_value(value)
    write_state(state)


# Convert camelCase to snake_case
def convert(name):
    reg_sub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', reg_sub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    out = {}
    for key in this_json:
        new_key = convert(key)
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out


# Move Custom Fields objects under custom_fields key
def convert_custom_fields(this_json):
    for record in this_json:
        cust_field_sets = []
        for key, value in record.items():
            if key.startswith('_'):
                if isinstance(value, dict):
                    cust_field_sets.append({key: value})
                elif isinstance(value, list):
                    cust_field_sets.append({key: value})
        record['custom_fields'] = cust_field_sets
    return this_json


# Run all transforms: denests _embedded, removes _embedded/_links, and
#  convert camelCase to snake_case for fieldname keys.
def transform_json(this_json, path):
    new_json = convert_custom_fields(this_json)
    out = dict()
    out[path] = new_json
    transformed_json = convert_json(out)
    return transformed_json[path]


# def transform_activities(this_json):
#     for record in this_json:
#         for key, value in record['activity'].items():
#             record[key] = value
#         del record['activity']
#     return this_json


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
