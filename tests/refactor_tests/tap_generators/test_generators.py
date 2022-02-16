import inspect
import json
import os
import mock
from unittest.mock import MagicMock

from tap_mambu.tap_mambu_refactor.tap_generators.generator import TapGenerator
from ..constants import config_json

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/fixtures"


# def test_count_records():
#     client_mock = MagicMock()
#     client_mock.page_size = int(config_json.get("page_size", 500))
#     client_mock.request = MagicMock()
#     with open(f"{FIXTURES_PATH}/data_LoanAccountsLMGenerator.json", "r") as fd:
#         fixture_data = json.loads(fd.read())
#     client_mock.request.side_effect = fixture_data
#     generator = TapGenerator(stream_name="loan_accounts",
#                              client=client_mock,
#                              config=config_json,
#                              state={"currently_syncing": "loan_accounts"},
#                              sub_type="self")
#
#     record_count = 0
#     for _ in generator:
#         record_count += 1
#
#     assert record_count == sum([len(record_list) for record_list in fixture_data])


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
