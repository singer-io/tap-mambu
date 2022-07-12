import inspect
import logging
import os
from copy import deepcopy
from datetime import datetime

from unittest.mock import MagicMock

from pytz import timezone

from tap_mambu.helpers import transform_json, convert
from tap_mambu.helpers.generator_processor_pairs import get_available_streams
from tap_mambu.tap_generators.generator import TapGenerator
from tap_mambu.tap_generators.multithreaded_bookmark_generator import MultithreadedBookmarkDayByDayGenerator, \
    MultithreadedBookmarkGenerator
from . import setup_generator_base_test
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
    from tap_mambu.helpers import transform_json

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
    from tap_mambu.helpers import transform_json

    assert transform_json([
        dict(encodedKey="123", creation_date="asd", _test=2)
    ], "loan_accounts") == [
        dict(encoded_key="123", creation_date="asd", custom_fields=list())
    ]


def test_transform_batch_larger_batch():
    from tap_mambu.helpers import transform_json

    assert transform_json([
        dict(encodedKey="123", creation_date="asd", custom_fields=list()) for _ in range(10)
    ], "loan_accounts") == [
        dict(encoded_key="123", creation_date="asd", custom_fields=list()) for _ in range(10)
    ]


def test_generator_bookmark_flow():
    stream_bookmarks = {
        "activities": "timestamp",
        "branches": "lastModifiedDate",
        "centres": "lastModifiedDate",
        "clients": "lastModifiedDate",
        "communications": "creationDate",
        "credit_arrangements": "lastModifiedDate",
        "deposit_accounts": "lastModifiedDate",
        "deposit_products": "lastModifiedDate",
        "gl_accounts": "lastModifiedDate",
        "gl_journal_entries": "creationDate",
        "groups": "lastModifiedDate",
        "deposit_transactions": "creationDate",
        "installments": "lastPaidDate",
        "interest_accrual_breakdown": "creationDate",
        "loan_products": "lastModifiedDate",
        "loan_transactions": "creationDate",
        "tasks": "lastModifiedDate",
        "users": "lastModifiedDate",
    }
    stream_custom_date = {
        # Activities stream needs the 'activity' field, which contains the actual record
        # This stream is also iterated in DESC order, so the bookmark will result in the lowest date (6th, not 7th)
        "activities": [{
            "client": "N/A", "activity": {
                "id": index, "timestamp": f"2022-06-06T00:00:00.{index:06d}Z-07:00"}
        } for index in range(400)] + [{
            "client": "N/A", "activity": {
                "id": index, "timestamp": f"2022-06-07T00:00:00.{index:06d}Z-07:00"}
        } for index in range(400)],
        # "branches": ["lastModifiedDate"],
        # "centres": ["lastModifiedDate"],
        # "clients": ["lastModifiedDate"],
        # "communications": ["creationDate"],
        # "credit_arrangements": ["lastModifiedDate"],
        # "deposit_accounts": ["lastModifiedDate"],
        # "deposit_products": ["lastModifiedDate"],
        # "gl_accounts": ["lastModifiedDate"],
        # "gl_journal_entries": ["creationDate"],
        # "groups": ["lastModifiedDate"],
        # "deposit_transactions": ["creationDate"],
        # "installments": ["lastPaidDate"],
        # "interest_accrual_breakdown": ["creationDate"],
        # "loan_products": ["lastModifiedDate"],
        # "loan_transactions": ["creationDate"],
        # "tasks": ["lastModifiedDate"],
        # "users": ["lastModifiedDate"],
    }
    for stream_name in stream_bookmarks:
        generators = setup_generator_base_test(stream_name, with_data=True,
                                               bookmark_field=stream_bookmarks[stream_name],
                                               custom_data=stream_custom_date.get(stream_name, None))
        for generator in generators:
            max_bookmark = None
            for record in generator:
                compare_function = lambda a, b: a > b
                if hasattr(generator, "compare_bookmark_values"):
                    compare_function = generator.compare_bookmark_values
                record_date = record[convert(stream_bookmarks[stream_name])]
                if max_bookmark is None or compare_function(record_date, max_bookmark):
                    max_bookmark = record_date

            if isinstance(generator, MultithreadedBookmarkDayByDayGenerator):
                assert 400 == generator.endpoint_intermediary_bookmark_offset, generator
                assert datetime(year=2022, month=6, day=6,
                                hour=0, minute=0, second=0,
                                microsecond=0, tzinfo=timezone("UTC")
                                ) == generator.endpoint_intermediary_bookmark_value, generator
            elif isinstance(generator, MultithreadedBookmarkGenerator):
                assert 1 == generator.endpoint_intermediary_bookmark_offset, generator
                assert datetime(year=2022, month=6, day=6,
                                hour=7, minute=0, second=0,
                                microsecond=399, tzinfo=timezone("UTC")
                                ) == generator.endpoint_intermediary_bookmark_value, generator

            if stream_name == "activities":  # Activities stream has descending order
                assert '2022-06-06T00:00:00.000000Z-07:00' == max_bookmark
            else:
                assert '2022-06-06T00:00:00.000399Z-07:00' == max_bookmark

