import os

WORKING_DIR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')

CLIENT_CONFIG_FILE_NAME = 'config.json'
CLIENT_CONFIG_FILE_PATH = os.path.join(WORKING_DIR_PATH, f'{CLIENT_CONFIG_FILE_NAME}')

RESOURCES_LINK = r'swagger/resources/'

OUTPUT_DIR_PATH = os.path.join(os.path.join(WORKING_DIR_PATH, 'tap_mambu'), 'generated_schemas')

TAP_TYPES_AND_FORMATS = {
    'integer': ['null', 'integer'],
    'number': ['null', 'string'],
    'string': ['null', 'string'],
    'boolean': ['null', 'boolean'],
    'object': ['null', 'object'],
    'array': 'array',
}

TAP_FORMATS = {
    'int32': None,
    'int64': None,
    'date': 'date',
    'date-time': 'date-time',
}

CUSTOM_FIELDS_FIELD = {
    "custom_fields": {
        "anyOf": [
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "field_set_id": {
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        "id": {
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        "value": {
                            "type": [
                                "null",
                                "string"
                            ]
                        }
                    }
                }
            },
            {
                "type": "null"
            }
        ]
    }}
