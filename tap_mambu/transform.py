import re

# Convert camelCase to snake_case
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


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


def remove_custom_nodes(this_json):
    if not isinstance(this_json, (dict, list)):
        return this_json
    if isinstance(this_json, list):
        return [remove_custom_nodes(vv) for vv in this_json]
    return {kk: remove_custom_nodes(vv) for kk, vv in this_json.items() \
        if not kk[:1] == '_'}


def add_cust_field(key, record, cust_field_sets):
    for cf_key, cf_value in record.items():
        field = {
            'field_set_id' : key,
            'id' : cf_key,
            'value' : cf_value,
        }
        cust_field_sets.append(field)

# Convert custom fields and sets
# Generalize/Abstract custom fields to key/value pairs
def convert_custom_fields(this_json):
    for record in this_json:
        cust_field_sets = []
        for key, value in record.items():
            if key.startswith('_'):
                if isinstance(value, dict):
                    add_cust_field(key, value, cust_field_sets)
                elif isinstance(value, list):
                    for element in value:
                        add_cust_field(key, element, cust_field_sets)
        record['custom_fields'] = cust_field_sets
    return this_json


# Run all transforms: denests _embedded, removes _embedded/_links, and
#  converst camelCase to snake_case for fieldname keys.
def transform_json(this_json, path):
    new_json = remove_custom_nodes(convert_custom_fields(this_json))
    out = {}
    out[path] = new_json
    transformed_json = convert_json(out)
    return transformed_json[path]


def transform_activities(this_json):
    for record in this_json:
        for key, value in record['activity'].items():
            record[key] = value
        del record['activity']
    return this_json
