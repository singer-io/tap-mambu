import datetime
import json

import pytest
from mock import MagicMock, call

import mock
import os.path
import inspect

from ..constants import config_json
from ..helpers import GeneratorMock, IsInstanceMatcher

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/Fixtures"


# @mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.TapProcessor.write_schema")
# @mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.TapProcessor.write_bookmark")
# def test_tap_processor(mock_write_bookmark, mock_write_schema, capsys):
#     from singer.catalog import Catalog
#     from tap_mambu.tap_mambu_refactor.tap_processors.processor import TapProcessor
#     catalog = Catalog.load(f"{FIXTURES_PATH}/catalog.json")
#
#     with open(f"{FIXTURES_PATH}/data_LoanAccountsLMGenerator.json", "r") as fd:
#         loan_accounts_lm_generator = GeneratorMock(json.loads(fd.read()))
#         loan_accounts_lm_generator.time_extracted = 0
#
#     with open(f"{FIXTURES_PATH}/data_LoanAccountsADGenerator.json", "r") as fd:
#         loan_accounts_ad_generator = GeneratorMock(json.loads(fd.read()))
#         loan_accounts_ad_generator.time_extracted = 0
#
#     client_mock = MagicMock()
#     processor = TapProcessor(catalog=catalog,
#                              stream_name="loan_accounts",
#                              client=client_mock,
#                              config=config_json,
#                              state={'currently_syncing': 'loan_accounts'},
#                              sub_type="self",
#                              generators=[loan_accounts_lm_generator, loan_accounts_ad_generator])
#     processor.endpoint_deduplication_key = "id"
#     processor.process_streams_from_generators()
#
#     captured = capsys.readouterr()
#
#     stdout_list = [json.loads(line) for line in captured.out.split("\n") if line]
#     with open(f"{FIXTURES_PATH}/expected_LoanAccountsProcessor.json", "r") as fd:
#         expected_list = [dict(type="RECORD", stream="loan_accounts", record=record) for record in json.loads(fd.read())]
#
#     for record in stdout_list:
#         assert record in expected_list
#
#     for expected_record in expected_list:
#         assert expected_record in stdout_list


@mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.parent_processor.get_selected_streams")
@mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.TapProcessor.write_schema")
@mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.TapProcessor.write_bookmark")
@mock.patch("tap_mambu.tap_mambu_refactor.sync_endpoint_refactor")
def test_tap_processor_process_child_records(mock_sync_endpoint_refactor,
                                             mock_write_bookmark,
                                             mock_write_schema,  # Mock write_schema so we don't pollute the output
                                             mock_get_selected_streams,
                                             capsys):
    from singer.catalog import Catalog
    from tap_mambu.tap_mambu_refactor.tap_processors.parent_processor import ParentProcessor
    fake_children_record_count = 4
    mock_get_selected_streams.return_value = ["child_1", "child_2"]
    mock_sync_endpoint_refactor.return_value = fake_children_record_count
    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")

    generator_data = [
        {
            "encoded_key": "12345678901234567890123456789012",
            "last_modified_date": "2022-01-01T00:00:00.000000Z",
            "id": "2"
        }, {
            "encoded_key": "12345678901234567890123456789013",
            "last_modified_date": "2022-01-01T00:00:00.000000Z",
            "id": "3"
        }, {
            "encoded_key": "12345678901234567890123456789014",
            "last_modified_date": "2022-01-01T00:00:00.000000Z",
            "id": "4"
        }, {
            "encoded_key": "12345678901234567890123456789015",
            "last_modified_date": "2022-01-01T00:00:00.000000Z",
            "id": "5"
        }
    ]
    generator = GeneratorMock(list(generator_data))
    generator.time_extracted = 0

    client_mock = MagicMock()
    processor = ParentProcessor(catalog=catalog,
                                stream_name="loan_accounts",
                                client=client_mock,
                                config=config_json,
                                state={'currently_syncing': 'loan_accounts'},
                                sub_type="self",
                                generators=[generator])
    processor.endpoint_child_streams = ["child_1", "child_2"]
    actual_records_count = processor.process_streams_from_generators()
    # sync_endpoint_refactor called for every record (len(generator_data)) for every child_stream + once for parent
    assert actual_records_count == \
           len(generator_data) * (fake_children_record_count * len(processor.endpoint_child_streams) + 1), \
        "Record count mismatch when adding child records"

    mock_sync_endpoint_refactor.assert_called_with(client=client_mock,
                                                   catalog=processor.catalog,
                                                   state={'currently_syncing': 'loan_accounts'},
                                                   stream_name=processor.endpoint_child_streams[-1],
                                                   sub_type="self",
                                                   config=config_json,
                                                   parent_id="5")

    captured = capsys.readouterr()
    stdout_list = [json.loads(line) for line in captured.out.split("\n") if line]
    # noinspection PyTypeChecker
    assert stdout_list == [
        {"type": "RECORD", "stream": "loan_accounts", "record": record} for record in generator_data
    ], "Output should contain mocked records"


@mock.patch("tap_mambu.tap_mambu_refactor.helpers.write_state")
def test_bookmarks(mock_write_state):
    from singer.catalog import Catalog
    from tap_mambu.tap_mambu_refactor.tap_processors.processor import TapProcessor

    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")
    client_mock = MagicMock()
    processor = TapProcessor(catalog=catalog,
                             stream_name="loan_accounts",
                             client=client_mock,
                             config=config_json,
                             state={'currently_syncing': 'loan_accounts'},
                             sub_type="self",
                             generators=[GeneratorMock([])])

    processor.write_bookmark()

    expected_state = {'currently_syncing': 'loan_accounts', 'bookmarks': {
        'loan_accounts': '2021-06-01T00:00:00Z'}}
    mock_write_state.assert_called_once_with(expected_state)


@mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.write_schema")
def test_write_schema(mock_write_schema):
    from singer.catalog import Catalog
    from tap_mambu.tap_mambu_refactor.tap_processors.processor import TapProcessor

    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")
    client_mock = MagicMock()
    processor = TapProcessor(catalog=catalog,
                             stream_name="loan_accounts",
                             client=client_mock,
                             config=config_json,
                             state={'currently_syncing': 'loan_accounts'},
                             sub_type="self",
                             generators=[GeneratorMock([])])
    processor.write_schema()

    schema = None
    stream_key_properties = None
    with open(f"{FIXTURES_PATH}/processor_catalog.json", "r") as fd:
        schema_json = json.loads(fd.read())
        for stream in schema_json["streams"]:
            if stream["tap_stream_id"] == "loan_accounts":
                schema = stream["schema"]
                stream_key_properties = stream["key_properties"]
                break
    assert schema is not None
    mock_write_schema.assert_called_with("loan_accounts", schema, stream_key_properties)


@mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.write_record")
@mock.patch("tap_mambu.tap_mambu_refactor.tap_processors.processor.write_schema")
def test_write_exceptions(mock_write_schema, mock_write_record):
    from singer.catalog import Catalog
    from tap_mambu.tap_mambu_refactor.tap_processors.processor import TapProcessor
    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")

    mock_write_record.side_effect = [None, OSError("Mock Record Exception")]
    mock_write_schema.side_effect = [None, OSError("Mock Schema Exception")]

    client_mock = MagicMock()
    processor = TapProcessor(catalog=catalog,
                             stream_name="loan_accounts",
                             client=client_mock,
                             config=config_json,
                             state={'currently_syncing': 'loan_accounts'},
                             sub_type="self",
                             generators=[GeneratorMock([
                                 {"id": "1", "last_modified_date": "2022-01-01T00:00:00+03:00"},
                                 {"id": "2", "last_modified_date": "2022-01-01T01:00:00+03:00"},
                                 {"id": "3", "last_modified_date": "2022-01-01T02:00:00+03:00"}
                             ])])
    processor.endpoint_deduplication_key = "id"

    with pytest.raises(OSError) as err:
        processor.process_streams_from_generators()
    assert err.value.args[0] == "Mock Record Exception"

    mock_write_record.assert_has_calls([
        call("loan_accounts", IsInstanceMatcher(dict), time_extracted=IsInstanceMatcher(datetime.datetime)),
        call("loan_accounts", IsInstanceMatcher(dict), time_extracted=IsInstanceMatcher(datetime.datetime))
    ])

    with pytest.raises(OSError) as err:
        processor.process_streams_from_generators()
    assert err.value.args[0] == "Mock Schema Exception"

    mock_write_schema.assert_has_calls([
        call("loan_accounts", IsInstanceMatcher(dict), IsInstanceMatcher(list)),
        call("loan_accounts", IsInstanceMatcher(dict), IsInstanceMatcher(list))
    ])
