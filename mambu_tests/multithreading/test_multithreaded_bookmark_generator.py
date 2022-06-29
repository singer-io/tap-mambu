import json
import threading
import pytest
from datetime import datetime
from mock import Mock, patch, call

from mambu_tests.helpers import ClientMock, MultithreadedBookmarkGeneratorFake
from tap_mambu.tap_generators.multithreaded_bookmark_generator import MultithreadedBookmarkGenerator


def test_set_intermediary_bookmark():
    mock_record_bookmark_value = '2020-01-01T00:00:00.000000Z'

    generator = MultithreadedBookmarkGeneratorFake()

    # test if the initial values aren't modified
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # check if the endpoint intermediary bookmark value and offset are set for a "correct" bookmark value
    generator.set_intermediary_bookmark(mock_record_bookmark_value)
    assert generator.endpoint_intermediary_bookmark_value == mock_record_bookmark_value
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value isn't set with a value lower than the already saved one
    generator.set_intermediary_bookmark('2005-01-01T00:00:00.000000Z')
    assert generator.endpoint_intermediary_bookmark_value == mock_record_bookmark_value
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value and offset correctly set when multiple records have
    # the same bookmark field value
    for _ in range(3):
        generator.set_intermediary_bookmark(mock_record_bookmark_value)
    assert generator.endpoint_intermediary_bookmark_value == mock_record_bookmark_value
    assert generator.endpoint_intermediary_bookmark_offset == 4

    # check if the endpoint intermediary bookmark value is properly set and the offset is reset
    mock_record_bookmark_value = '2021-11-11T00:00:00.000000Z'
    generator.set_intermediary_bookmark(mock_record_bookmark_value)
    assert generator.endpoint_intermediary_bookmark_value == mock_record_bookmark_value
    assert generator.endpoint_intermediary_bookmark_offset == 1


@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator._all_fetch_batch_steps")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.time.sleep")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.MultithreadedBookmarkGenerator.prepare_batch_params")
def test_fetch_batch_continuously_first_call(mock_prepare_batch_params, mock_time_sleep, mock_all_fetch_batch_steps):
    mock_all_fetch_batch_steps.return_value = False

    generator = MultithreadedBookmarkGeneratorFake()
    assert generator.end_of_file is False
    assert len(generator.buffer) == 0

    # test if the first run of the method does call only the _all_fetch_batch_steps method
    generator.fetch_batch_continuously()
    mock_prepare_batch_params.assert_not_called()
    mock_all_fetch_batch_steps.assert_called_once()
    mock_time_sleep.assert_not_called()
    assert generator.end_of_file is True


@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator._all_fetch_batch_steps")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.MultithreadedBookmarkGenerator.prepare_batch_params")
def test_fetch_batch_continuously_multiple_calls(mock_prepare_batch_params, mock_all_fetch_batch_steps):
    mock_all_fetch_batch_steps.side_effect = [True, True, False]

    generator = MultithreadedBookmarkGeneratorFake()
    generator.fetch_batch_continuously()

    # test the flow if multiple while iterations are needed
    mock_prepare_batch_params.assert_called()
    assert mock_prepare_batch_params.call_count == 2
    mock_all_fetch_batch_steps.assert_called()
    assert mock_all_fetch_batch_steps.call_count == 3
    assert generator.end_of_file is True


@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator._all_fetch_batch_steps")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.time.sleep")
def test_fetch_batch_continuously_sleep_branch(mock_time_sleep, mock_all_fetch_batch_steps):
    mock_all_fetch_batch_steps.return_value = False

    generator = MultithreadedBookmarkGeneratorFake()
    generator.buffer = [{'encoded_key': f'0-{record_no}-test'} for record_no in range(0, 10)]
    generator.batch_limit = 5

    fetch_batch_thread = threading.Thread(target=generator.fetch_batch_continuously)
    fetch_batch_thread.start()

    while len(generator.buffer) >= generator.batch_limit:
        generator.buffer.pop()
    fetch_batch_thread.join()

    assert mock_time_sleep.call_count > generator.batch_limit


@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.transform_json")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.time.sleep")
def test_collect_batches_thread_not_done(mock_time_sleep, mock_transform_json):
    def create_mock_future_pending():
        mock_future_pending = Mock()
        mock_future_pending.done = Mock()
        mock_future_pending.done.side_effect = [False, False, True]
        return mock_future_pending

    mock_future_done = Mock()
    mock_future_done.done = Mock()
    mock_future_done.done.return_value = True

    generator = MultithreadedBookmarkGeneratorFake()

    mock_futures = [mock_future_done for _ in
                    range(0, generator.batch_limit - generator.artificial_limit,
                          generator.artificial_limit)] + \
                   [create_mock_future_pending()]

    generator.collect_batches(mock_futures)

    # test if the sleep function is called when the request isn't finished
    assert mock_time_sleep.call_count == 2

    # just a check to make sure the algorithm passed over the while loop
    mock_transform_json.assert_called()
