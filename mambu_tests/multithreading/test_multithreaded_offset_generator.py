import json
import threading
import pytest
from mock import Mock, patch, call

from mambu_tests.helpers import ClientMock, MultithreadedOffsetGeneratorFake
from tap_mambu.tap_generators.multithreaded_offset_generator import MultithreadedOffsetGenerator


def test_init_variable_params():
    mock_client = ClientMock()
    mock_config = Mock()
    mock_config.start_date = "2019-11-25T00:00:00.000000Z"

    generator = MultithreadedOffsetGeneratorFake(config=mock_config)

    assert generator.artificial_limit == mock_client.page_size
    assert generator.limit == mock_client.page_size + generator.overlap_window
    assert generator.batch_limit == generator.max_threads * generator.client.page_size + generator.overlap_window


def test_check_and_get_set_reunion():
    limit = 100
    overlap_window = 10
    total_records_no = limit + overlap_window

    generator = MultithreadedOffsetGeneratorFake()
    generator.overlap_window = limit
    generator.artificial_limit = overlap_window

    # test when a and b have the same values on the overlap window indexes
    a = [json.dumps({'encoded_key': f'0-{record_no}-test'}) for record_no in range(0, total_records_no)]
    b = a[-overlap_window:] + [json.dumps({'encoded_key': f'1-{record_no}-test'}) for record_no in range(0, limit)]
    assert generator.check_and_get_set_reunion(set(a), set(b), overlap_window) == set(a).union(b)

    # test when a and b don't overlap 100% but the error it's manageable
    a = [json.dumps({'encoded_key': f'0-{record_no}-test'}) for record_no in range(0, total_records_no)]
    b = a[-(overlap_window - 5):] + [json.dumps({'encoded_key': f'1-{record_no}-test'})
                                     for record_no in range(0, limit)]
    assert generator.check_and_get_set_reunion(set(a), set(b), overlap_window) == set(a).union(b)

    # test when a and b don't overlap at all
    with pytest.raises(RuntimeError):
        a = {json.dumps({'encoded_key': f'0-{record_no}-test'}) for record_no in range(0, total_records_no)}
        b = {json.dumps({'encoded_key': f'1-{record_no}-test'}) for record_no in range(0, total_records_no)}
        generator.check_and_get_set_reunion(a, b, overlap_window)


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.stop_all_request_threads")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.check_and_get_set_reunion",
       wraps=MultithreadedOffsetGenerator.check_and_get_set_reunion)
def test_error_check_and_fix(mock_check_and_get_set_reunion, mock_stop_all_request_threads):
    generator = MultithreadedOffsetGeneratorFake()

    mock_final_buffer = {f'0-{record_no}'
                         for record_no in range(0, generator.client.page_size + generator.overlap_window)}
    mock_temp_buffer = {f'1-{record_no}'
                        for record_no in range(0, generator.client.page_size)}

    # test if the method raises RuntimeError if it can't correct the batches
    with pytest.raises(RuntimeError):
        generator.error_check_and_fix(mock_final_buffer, mock_temp_buffer, [])

    mock_check_and_get_set_reunion.assert_called_with(mock_final_buffer, mock_temp_buffer, generator.client.page_size)
    mock_stop_all_request_threads.assert_called_with([])


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.fetch_batch_continuously")
def test_iter_flow(mock_fetch_batch_continuously):
    generator = MultithreadedOffsetGeneratorFake()

    mock_fetch_batch_continuously.assert_not_called()
    assert generator.fetch_batch_thread is None

    iter(generator)
    mock_fetch_batch_continuously.assert_called_once()
    assert generator.fetch_batch_thread is not None


def test_next_flow():
    test_records = [{"encoded_key": "1", "creation_date": '2020-01-01T00:00:000000Z'},
                    {"encoded_key": "2", "creation_date": '2020-01-01T00:00:000000Z'},
                    {"encoded_key": "3", "creation_date": '2020-01-01T00:00:000000Z'}, ]
    generator = MultithreadedOffsetGeneratorFake()
    generator.buffer = list(test_records)

    for idx, record in enumerate(test_records):
        assert next(generator) == test_records[idx]

    with pytest.raises(StopIteration):
        generator.end_of_file = True
        next(generator)


@patch("tap_mambu.tap_generators.multithreaded_offset_generator.time.sleep")
def test_stop_all_request_threads(mock_time_sleep):
    def create_future(side_effect=None):
        future = Mock()
        future.cancel = Mock()
        future.done = Mock()
        if side_effect:
            future.done.side_effect = side_effect
        else:
            future.done.return_value = True
        return future

    finished_futures = [create_future() for _ in range(0, 100)]
    not_finished_futures = [create_future(side_effect=[False, True]) for _ in range(0, 5)]

    MultithreadedOffsetGeneratorFake.stop_all_request_threads(finished_futures + not_finished_futures)

    assert mock_time_sleep.call_count == len(not_finished_futures)

    # test if the cancel and done methods are called the correct amount of times
    for finished_future in finished_futures:
        assert finished_future.cancel.call_count == 1
        assert finished_future.done.call_count == 1

    # test if the cancel and done methods are called the correct amount of times
    for not_finished_future in not_finished_futures:
        assert not_finished_future.cancel.call_count == 1
        assert not_finished_future.done.call_count == 2


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator._all_fetch_batch_steps")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator.time.sleep")
def test_fetch_batch_continuously_first_call(mock_time_sleep, mock_all_fetch_batch_steps):
    mock_all_fetch_batch_steps.return_value = False

    generator = MultithreadedOffsetGeneratorFake()
    assert generator.end_of_file is False
    assert len(generator.buffer) == 0

    # test if the first run of the method does call only the _all_fetch_batch_steps method
    generator.fetch_batch_continuously()
    mock_all_fetch_batch_steps.assert_called_once()
    mock_time_sleep.assert_called_once()
    assert generator.end_of_file is True


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator._all_fetch_batch_steps")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator.time.sleep")
def test_fetch_batch_continuously_multiple_calls(mock_time_sleep, mock_all_fetch_batch_steps):
    mock_all_fetch_batch_steps.side_effect = [True, True, False]

    generator = MultithreadedOffsetGeneratorFake()
    generator.fetch_batch_continuously()

    # test the flow if multiple while iterations are needed
    mock_all_fetch_batch_steps.assert_called()
    assert mock_all_fetch_batch_steps.call_count == 3
    assert mock_time_sleep.call_count == 3
    assert generator.end_of_file is True
