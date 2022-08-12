import json
import pytest
from mock import Mock, patch, call

from mambu_tests.helpers import ClientMock, MultithreadedOffsetGeneratorFake
from tap_mambu.helpers.hashable_dict import HashableDict
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
       side_effect=RuntimeError)
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

    for record in test_records:
        assert next(generator) == record

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


def test_preprocess_record_one_record():
    mock_record = {'encoded_key': 'test', 'test_field': 'value'}

    generator = MultithreadedOffsetGeneratorFake()
    assert generator.buffer == []

    record = generator.preprocess_record(mock_record)
    assert record == mock_record
    assert generator.buffer == [mock_record]


def test_preprocess_record_multiple_records():
    mock_records = [{'encoded_key': 'test', 'test_field': f'value_{no}'} for no in range(10)]

    generator = MultithreadedOffsetGeneratorFake()
    assert generator.buffer == []

    for idx, mock_record in enumerate(mock_records):
        record = generator.preprocess_record(mock_record)
        assert record == mock_records[idx]
    assert generator.buffer == mock_records


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.preprocess_record")
def test_preprocess_batches_flow(mock_preprocess_record):
    mock_records = [{'encoded_key': 'test', 'test_field': f'value_{no}'} for no in range(100)]
    mock_last_batch_set = set(f'{{"encoded_key":"test_value{no}"}}' for no in range(50))

    generator = MultithreadedOffsetGeneratorFake()
    assert generator.last_batch_set == set()

    generator.last_batch_set = mock_last_batch_set
    generator.preprocess_batches(mock_records)

    mock_preprocess_record.assert_has_calls([call(record) for record in mock_records])
    assert mock_preprocess_record.call_count == len(mock_records)
    assert generator.last_batch_size == len(mock_last_batch_set)


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.preprocess_batches")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.collect_batches")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.queue_batches")
def test_all_fetch_batch_steps_flow(mock_queue_batches, mock_collect_batches, mock_preprocess_batches):
    generator = MultithreadedOffsetGeneratorFake()

    # test simple flow
    mock_collect_batches.return_value = (set('test_value'), False)
    fetch_output = generator._all_fetch_batch_steps()
    assert fetch_output is True

    # test when stop_iteration is True
    mock_collect_batches.return_value = (set('test_value'), True)
    fetch_output = generator._all_fetch_batch_steps()
    assert fetch_output is False

    # test when final_buffer is empty
    mock_collect_batches.return_value = (set(), False)
    fetch_output = generator._all_fetch_batch_steps()
    assert fetch_output is False

    # test when stop_iteration is True and final_buffer is empty
    mock_collect_batches.return_value = (set(), True)
    fetch_output = generator._all_fetch_batch_steps()
    assert fetch_output is False

    assert mock_queue_batches.call_count == 4
    assert mock_collect_batches.call_count == 4
    assert mock_preprocess_batches.call_count == 4


@patch.object(MultithreadedOffsetGenerator, 'prepare_batch',
              side_effect=MultithreadedOffsetGenerator.prepare_batch,
              autospec=True)
@patch("tap_mambu.tap_generators.multithreaded_offset_generator.MultithreadedRequestsPool.queue_request")
def test_queue_batches(mock_queue_request,
                       mock_prepare_batch):
    mock_client = ClientMock()
    mock_endpoint_path = 'test_endpoint_path'
    mock_endpoint_api_method = 'POST'
    mock_endpoint_api_version = 'v2'
    mock_endpoint_api_key_type = 'test_endpoint_api_key_type'
    mock_endpoint_body = {'filterCriteria': {"field": "creationDate",
                                             "operator": "BETWEEN",
                                             "value": '2020-01-01T12:00:00.000000Z',
                                             "secondValue": '2020-02-01T03:01:20.000000Z'},
                          'sortingCriteria': {"field": "encodedKey",
                                              "order": "ASC"}
                          },
    mock_params = {'paginationDetails': 'OFF',
                   'detailsLevel': 'FULL'}
    mock_overlap_window = 20
    mock_batch_limit = 4000
    mock_artificial_limit = mock_client.page_size

    mock_queue_request.side_effect = [Mock() for _ in range(0, mock_batch_limit, mock_artificial_limit)]

    generator = MultithreadedOffsetGeneratorFake(client=mock_client)
    generator.overlap_window = mock_overlap_window
    generator.batch_limit = mock_batch_limit
    generator.endpoint_path = mock_endpoint_path
    generator.endpoint_api_method = mock_endpoint_api_method
    generator.endpoint_api_version = mock_endpoint_api_version
    generator.endpoint_api_key_type = mock_endpoint_api_key_type
    generator.endpoint_body = mock_endpoint_body
    generator.params = mock_params
    generator.limit = mock_client.page_size + mock_overlap_window

    # +1 because the while loop condition is <=
    while_upper_limit = mock_batch_limit // (mock_client.page_size + mock_overlap_window) + 1

    assert generator.offset == 0
    features = generator.queue_batches()

    assert len(features) == while_upper_limit

    mock_params['offset'] = 0
    mock_params['limit'] = mock_artificial_limit + mock_overlap_window
    calls = []
    for offset in range(0, while_upper_limit):
        calls.append(call(mock_client, 'test_stream', mock_endpoint_path, mock_endpoint_api_method,
                          mock_endpoint_api_version, mock_endpoint_api_key_type, mock_endpoint_body, dict(mock_params)))
        mock_params['offset'] += mock_artificial_limit

    # test if the queue_request method is called using the correct offset
    mock_queue_request.assert_has_calls(calls)

    # test if the methods are called the correct amount of times
    assert mock_prepare_batch.call_count == while_upper_limit
    assert mock_queue_request.call_count == while_upper_limit
    # used only artificial_limit because the offset is increased only by the artificial_limit value (without overlap)
    assert generator.offset == while_upper_limit * mock_artificial_limit


@patch("tap_mambu.tap_generators.multithreaded_offset_generator.transform_json")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator.time.sleep")
def test_collect_batches_thread_not_done(mock_time_sleep, mock_transform_json):
    def create_mock_future_pending():
        mock_future_pending = Mock()
        mock_future_pending.done = Mock()
        mock_future_pending.done.side_effect = [False, False, True]
        return mock_future_pending

    mock_future_done = Mock()
    mock_future_done.done = Mock()
    mock_future_done.done.return_value = True

    generator = MultithreadedOffsetGeneratorFake()

    mock_futures = [mock_future_done for _ in
                    range(0, generator.batch_limit - generator.artificial_limit,
                          generator.artificial_limit)] + \
                   [create_mock_future_pending()]

    generator.collect_batches(mock_futures)

    # test if the sleep function is called when the request isn't finished
    assert mock_time_sleep.call_count == 2

    # just a check to make sure the algorithm passed over the while loop
    mock_transform_json.assert_called()


@patch.object(MultithreadedOffsetGenerator, 'error_check_and_fix',
              side_effect=MultithreadedOffsetGenerator.error_check_and_fix,
              autospec=True)
@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.stop_all_request_threads")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator.MultithreadedOffsetGenerator.transform_batch")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator.transform_json")
def test_collect_batches(mock_transform_json,
                         mock_transform_batch,
                         mock_stop_all_request_threads,
                         mock_error_check_and_fix):
    mock_batch_limit = 4000
    mock_artificial_limit = 200
    mock_overlap_window = 20
    mock_client = ClientMock(page_size=200)

    generator = MultithreadedOffsetGeneratorFake(client=mock_client)
    generator.artificial_limit = mock_artificial_limit
    generator.batch_limit = mock_batch_limit
    generator.overlap_window = mock_overlap_window

    # generate fake data as if they were extracted from the API
    mock_records = [[HashableDict({'encoded_key': f'0-{record_no}'})
                     for record_no in range(0, mock_client.page_size + mock_overlap_window)], ]
    for batch_no in range(1, (mock_batch_limit // mock_artificial_limit) - 1):
        mock_batch = mock_records[batch_no - 1][-mock_overlap_window:]
        for record_no in range(0, mock_client.page_size):
            mock_batch.append(HashableDict({'encoded_key': f'{batch_no}-{record_no}'}))
        mock_records.append(mock_batch)
    mock_records.append([])

    mock_transform_batch.side_effect = mock_records

    mock_features = [Mock() for _ in range(0, mock_batch_limit, mock_artificial_limit)]
    buffer, stop_iteration = generator.collect_batches(mock_features)

    assert stop_iteration is True
    assert all(record in buffer for batch in mock_records[:-1] for record in batch)

    assert mock_transform_json.call_count == mock_batch_limit / mock_artificial_limit
    assert mock_transform_batch.call_count == mock_batch_limit / mock_artificial_limit
    # -2 because the first call is skipped and the last future value is an empty list
    assert mock_error_check_and_fix.call_count == (mock_batch_limit / mock_artificial_limit) - 2
    mock_stop_all_request_threads.assert_called_once()
