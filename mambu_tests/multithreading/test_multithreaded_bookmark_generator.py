import json
import threading
import time
from datetime import datetime

from mock import Mock, patch, call
from pytz import timezone

from mambu_tests.helpers import ClientMock, MultithreadedBookmarkGeneratorFake
from tap_mambu.helpers.hashable_dict import HashableDict
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
    assert mock_prepare_batch_params.call_count == 2
    assert mock_all_fetch_batch_steps.call_count == 3
    assert generator.end_of_file is True


@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator._all_fetch_batch_steps")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.time.sleep")
def test_fetch_batch_continuously_sleep_branch(mock_time_sleep, mock_all_fetch_batch_steps):
    mock_all_fetch_batch_steps.return_value = False

    generator = MultithreadedBookmarkGeneratorFake()
    generator.buffer = [{'encoded_key': f'0-{record_no}-test'} for record_no in range(10)]
    generator.batch_limit = 5

    fetch_batch_thread = threading.Thread(target=generator.fetch_batch_continuously)
    fetch_batch_thread.start()

    while len(generator.buffer) >= generator.batch_limit:
        time.sleep(0.1)
        generator.buffer.pop()
    fetch_batch_thread.join()

    assert mock_time_sleep.call_count > generator.batch_limit


@patch.object(MultithreadedBookmarkGenerator, 'prepare_batch',
              side_effect=MultithreadedBookmarkGenerator.prepare_batch,
              autospec=True)
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.MultithreadedRequestsPool.queue_request")
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

    generator = MultithreadedBookmarkGeneratorFake(client=mock_client)
    generator.overlap_window = mock_overlap_window
    generator.batch_limit = mock_batch_limit
    generator.endpoint_path = mock_endpoint_path
    generator.endpoint_api_method = mock_endpoint_api_method
    generator.endpoint_api_version = mock_endpoint_api_version
    generator.endpoint_api_key_type = mock_endpoint_api_key_type
    generator.endpoint_body = mock_endpoint_body
    generator.params = mock_params
    generator.limit = mock_client.page_size + mock_overlap_window

    assert generator.offset == 0
    features = generator.queue_batches()

    assert len(features) == mock_batch_limit // mock_artificial_limit

    mock_params['offset'] = 0
    mock_params['limit'] = mock_artificial_limit + mock_overlap_window
    calls = []
    for offset in range(0, mock_batch_limit, mock_artificial_limit):
        calls.append(call(mock_client, 'test_stream', mock_endpoint_path, mock_endpoint_api_method,
                          mock_endpoint_api_version, mock_endpoint_api_key_type, mock_endpoint_body, dict(mock_params)))
        mock_params['offset'] += mock_artificial_limit

    # test if the queue_request method is called using the correct offset
    mock_queue_request.assert_has_calls(calls)

    # test if the methods are called the correct amount of times
    assert mock_prepare_batch.call_count == mock_batch_limit / mock_client.page_size
    assert mock_queue_request.call_count == mock_batch_limit / mock_client.page_size
    assert generator.offset == mock_batch_limit - mock_client.page_size


@patch.object(MultithreadedBookmarkGenerator, 'error_check_and_fix',
              side_effect=MultithreadedBookmarkGenerator.error_check_and_fix,
              autospec=True)
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator.stop_all_request_threads")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.MultithreadedBookmarkGenerator.transform_batch")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.transform_json")
def test_collect_batches(mock_transform_json,
                         mock_transform_batch,
                         mock_stop_all_request_threads,
                         mock_error_check_and_fix):
    mock_batch_limit = 4000
    mock_artificial_limit = 200
    mock_overlap_window = 20
    mock_client = ClientMock(page_size=200)

    generator = MultithreadedBookmarkGeneratorFake(client=mock_client)
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


@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.datetime_to_tz")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.str_to_localized_datetime")
@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.preprocess_record")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator.set_intermediary_bookmark")
def test_preprocess_record(mock_set_intermediary_bookmark, mock_offset_preprocess_record,
                           mock_str_to_localized_datetime, mock_datetime_to_tz):
    mock_bookmark_field = 'test_field'
    generator = MultithreadedBookmarkGeneratorFake()
    generator.endpoint_bookmark_field = mock_bookmark_field

    # test the behaviour with a record that doesn't contain the bookmarked field
    mock_record = {'encoded_key': 'test'}
    mock_offset_preprocess_record.return_value = mock_record
    mock_datetime_to_tz.return_value = mock_record
    mock_str_to_localized_datetime.return_value = mock_record

    generator.preprocess_record(mock_record)
    mock_str_to_localized_datetime.assert_not_called()
    mock_datetime_to_tz.assert_not_called()
    mock_set_intermediary_bookmark.assert_not_called()

    # test the behaviour with a record that does contain the bookmarked field
    mock_record = {'encoded_key': 'test',
                   'test_field': '2022-01-01T00:00:00Z+03:00'}
    datetime_in_local_tz = datetime.now(timezone("Europe/Bucharest")).replace(year=2022, month=1, day=1, hour=3,
                                                                              minute=0, second=0, microsecond=0)
    datetime_in_utc_tz = datetime.utcnow().replace(year=2022, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    mock_offset_preprocess_record.return_value = mock_record
    mock_str_to_localized_datetime.return_value = datetime_in_local_tz
    mock_datetime_to_tz.return_value = datetime_in_utc_tz

    generator.preprocess_record(mock_record)
    mock_str_to_localized_datetime.assert_called_with(mock_record[mock_bookmark_field])
    mock_datetime_to_tz.assert_called_with(datetime_in_local_tz, "UTC")
    mock_set_intermediary_bookmark.assert_called_with(datetime_in_utc_tz)


@patch("tap_mambu.tap_generators.multithreaded_offset_generator."
       "MultithreadedOffsetGenerator.preprocess_batches")
def test_preprocess_batches(mock_offset_preprocess_batches):
    mock_buffer = [{'encoded_key': f'value_{no}'} for no in range(100)]
    mock_offset_preprocess_batches.return_value = mock_buffer

    generator = MultithreadedBookmarkGeneratorFake()
    generator.preprocess_batches(mock_buffer)
    assert generator.last_batch_size == len(mock_buffer)
