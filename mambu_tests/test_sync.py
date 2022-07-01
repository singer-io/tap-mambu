import pytest
import mock
from mock import PropertyMock, call

from tap_mambu.helpers.generator_processor_pairs import get_generator_processor_for_stream, \
    get_generator_processor_pairs, get_stream_subtypes
from tap_mambu.sync import sync_endpoint, sync_all_streams
from tap_mambu.tap_generators.child_generator import ChildGenerator
from .helpers import IsInstanceMockMatcher
from .constants import config_json


@mock.patch("tap_mambu.helpers.generator_processor_pairs.LoanAccountsLMGenerator")
@mock.patch("tap_mambu.helpers.generator_processor_pairs.LoanAccountsADGenerator")
@mock.patch("tap_mambu.helpers.generator_processor_pairs.LoanAccountsProcessor")
def test_sync_endpoint(mock_loan_accounts_processor, mock_loan_accounts_ad_generator,
                       mock_loan_accounts_lm_generator):
    type(mock_loan_accounts_lm_generator.return_value).type = PropertyMock(return_value="LoanAccountsLMGenerator")
    type(mock_loan_accounts_ad_generator.return_value).type = PropertyMock(return_value="LoanAccountsADGenerator")
    client = "client"
    catalog = "catalog"
    state = "state"
    stream_name = "loan_accounts"
    sub_type = "sub_type"
    config = "config"
    sync_endpoint(client=client, catalog=catalog, state=state, stream_name=stream_name,
                  sub_type=sub_type, config=config)
    mock_loan_accounts_lm_generator.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                            sub_type=sub_type, config=config)
    mock_loan_accounts_ad_generator.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                            sub_type=sub_type, config=config)
    mock_loan_accounts_processor.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                         sub_type=sub_type, config=config, catalog=catalog,
                                                         generators=[IsInstanceMockMatcher("LoanAccountsLMGenerator"),
                                                                     IsInstanceMockMatcher("LoanAccountsADGenerator")])


@mock.patch("tap_mambu.sync.sync_endpoint")
@mock.patch("tap_mambu.sync.update_currently_syncing")
@mock.patch("tap_mambu.sync.singer.get_currently_syncing")
@mock.patch("tap_mambu.sync.get_generator_processor_for_stream")
@mock.patch("tap_mambu.sync.get_selected_streams")
@mock.patch("tap_mambu.sync.get_timezone_info")
def test_sync_all_streams_flow(mock_get_timezone_info,
                               mock_get_selected_streams,
                               mock_get_generator_processor_for_stream,
                               mock_singer_get_currently_syncing,
                               mock_update_currently_syncing,
                               mock_sync_endpoint):
    available_stream_pairs = get_generator_processor_pairs()
    selected_streams = list(available_stream_pairs.keys())
    child_streams = [stream_name for stream_name, pairs in available_stream_pairs.items()
                     for generator in pairs[0]
                     if issubclass(generator, ChildGenerator)]

    client = "client",
    catalog = "catalog",
    state = {'currently_syncing': 'activities'}

    mock_get_selected_streams.return_value = selected_streams
    mock_get_generator_processor_for_stream.side_effect = lambda x: get_generator_processor_for_stream(x)
    mock_singer_get_currently_syncing.return_value = 'activities'

    sync_all_streams(client, config_json, catalog, state)

    mock_get_selected_streams.assert_called_with(catalog)
    mock_singer_get_currently_syncing.assert_called_with(state)
    mock_get_generator_processor_for_stream.assert_has_calls([call(stream_name) for stream_name in selected_streams])
    assert mock_get_generator_processor_for_stream.call_count == len(selected_streams)

    selected_streams_count = 0
    for stream_name in selected_streams:
        # did in this way because gl_accounts counts as 5 streams due to its subtypes
        selected_streams_count += len(get_stream_subtypes(stream_name))
        if stream_name not in child_streams:
            state['currently_syncing'] = stream_name
            mock_update_currently_syncing.assert_any_call(state, stream_name)
            mock_update_currently_syncing.assert_any_call(state, None)

            for sub_type in get_stream_subtypes(stream_name):
                mock_sync_endpoint.assert_has_calls([call(client=client,
                                                          catalog=catalog,
                                                          state=state,
                                                          stream_name=stream_name,
                                                          sub_type=sub_type,
                                                          config=config_json)])

    assert mock_update_currently_syncing.call_count == (selected_streams_count - len(child_streams)) * 2
    assert mock_sync_endpoint.call_count == selected_streams_count - len(child_streams)


@mock.patch("tap_mambu.sync.singer.get_currently_syncing")
@mock.patch("tap_mambu.sync.get_selected_streams")
@mock.patch("tap_mambu.sync.get_timezone_info")
def test_sync_all_streams_no_selected_streams(mock_get_timezone_info,
                                              mock_get_selected_streams,
                                              mock_singer_get_currently_syncing):
    mock_get_selected_streams.return_value = set()
    sync_all_streams('client', {}, 'catalog', 'state')

    mock_get_selected_streams.assert_called_with('catalog')
    mock_singer_get_currently_syncing.assert_not_called()


@mock.patch("tap_mambu.sync.should_sync_stream")
@mock.patch("tap_mambu.sync.get_selected_streams")
@mock.patch("tap_mambu.sync.get_timezone_info")
def test_sync_all_streams_flow_child_stream(mock_get_timezone_info,
                                            mock_get_selected_streams,
                                            mock_should_sync_stream):
    streams_without_child = []
    child_streams = []

    for stream_name, pairs in get_generator_processor_pairs().items():
        for generator in pairs[0]:
            if issubclass(generator, ChildGenerator):
                child_streams.append(stream_name)
            else:
                streams_without_child.append(stream_name)

    all_streams = streams_without_child + child_streams
    mock_get_selected_streams.return_value = all_streams
    mock_should_sync_stream.return_value = (None, None)

    sync_all_streams('client', {}, 'catalog', {})

    for stream_name in streams_without_child:
        mock_should_sync_stream.assert_any_call(all_streams,
                                                None,
                                                stream_name)

    for stream_name in child_streams:
        with pytest.raises(AssertionError):
            mock_should_sync_stream.assert_called_with(all_streams,
                                                       None,
                                                       stream_name)
