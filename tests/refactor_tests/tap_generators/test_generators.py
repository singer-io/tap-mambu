import inspect
import json
import os
from copy import deepcopy

import mock
from unittest.mock import MagicMock

from tap_mambu.tap_mambu_refactor.helpers import transform_json
from tap_mambu.tap_mambu_refactor.tap_generators.generator import TapGenerator
from ..constants import config_json

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/fixtures"


def test_generator_fetch_batch_flow():
    client_mock = MagicMock()
    client_mock.page_size = 5
    client_mock.request = MagicMock()
    client_mock.request.side_effect = [[{"id": nr + page*5} for nr in range(5)] for page in range(3)]
    generator = TapGenerator(stream_name="loan_accounts",
                             client=client_mock,
                             config=config_json,
                             state={"currently_syncing": "loan_accounts"},
                             sub_type="self")
    generator_endpoint_api_method = generator.endpoint_api_method
    generator_endpoint_path = generator.endpoint_path
    generator_endpoint_api_version = generator.endpoint_api_version
    generator_endpoint_api_key_type = generator.endpoint_api_key_type
    generator_endpoint_body = {"sortingCriteria": generator.endpoint_sorting_criteria,
                               "filterCriteria": generator.endpoint_filter_criteria}

    for batch_index in range(3):
        generator.prepare_batch()
        generator_endpoint_querystring = '&'.join([f'{key}={value}' for (key, value) in generator.params.items()])

        assert {"detailsLevel": "FULL", "limit": 5,
                "offset": 0 + batch_index * 5, "paginationDetails": "OFF"} == generator.params
        raw_batch = generator.fetch_batch()
        client_mock.request.assert_called_with(method=generator_endpoint_api_method,
                                               path=generator_endpoint_path,
                                               version=generator_endpoint_api_version,
                                               apikey_type=generator_endpoint_api_key_type,
                                               params=generator_endpoint_querystring,
                                               endpoint="loan_accounts",
                                               json=generator_endpoint_body)

        assert raw_batch == [{"id": nr + batch_index * 5} for nr in range(5)]

        generator.buffer = transform_json(raw_batch, "loan_accounts")
        assert generator.buffer == [{"custom_fields": [], "id": nr + batch_index * 5} for nr in range(5)]

        generator.last_batch_size = len(generator.buffer)

        # Simulate end of batch, so we can fetch a new one
        generator.buffer = []
        generator.offset += generator.limit


def test_generator_iterator_flow():
    client_mock = MagicMock()
    client_mock.page_size = 5
    client_data = [[{"id": nr + page*5} for nr in range(5)] for page in range(3)]
    generator = TapGenerator(stream_name="loan_accounts",
                             client=client_mock,
                             config=config_json,
                             state={"currently_syncing": "loan_accounts"},
                             sub_type="self")
    generator.prepare_batch = MagicMock()
    generator.fetch_batch = MagicMock(side_effect=client_data)

    expected_records = [{"custom_fields": [], "id": record["id"]} for record in deepcopy(sum(client_data, []))]
    record_count = 0
    for record in generator:
        assert record == expected_records[record_count]
        record_count += 1


def test_transform_batch_custom_field():
    from tap_mambu.tap_mambu_refactor.helpers import transform_json

    assert transform_json([
        {"encodedKey": "123", "creation_date": "asd",
         "_random_field": {"cevaId": "asd"},
         "_random_field_2": [{"a": "b", "c": "d"}],
         "_random_field_3": 0}
    ], "loan_accounts") == [
        {"encoded_key": "123", "creation_date": "asd", "custom_fields": [
            {"field_set_id": "_random_field", "id": "cevaId", "value": "asd"},
            {"field_set_id": "_random_field_2", "id": "a", "value": "b"},
            {"field_set_id": "_random_field_2", "id": "c", "value": "d"}
        ]}
    ]


def test_transform_batch_custom_field_auto_add():
    from tap_mambu.tap_mambu_refactor.helpers import transform_json

    assert transform_json([
        dict(encodedKey="123", creation_date="asd", _test=2)
    ], "loan_accounts") == [
        dict(encoded_key="123", creation_date="asd", custom_fields=list())
    ]


def test_transform_batch_larger_batch():
    from tap_mambu.tap_mambu_refactor.helpers import transform_json

    assert transform_json([
        dict(encodedKey="123", creation_date="asd", custom_fields=list()) for _ in range(10)
    ], "loan_accounts") == [
        dict(encoded_key="123", creation_date="asd", custom_fields=list()) for _ in range(10)
    ]
