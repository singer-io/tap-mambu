import pytest
from mock import Mock, patch, call

from mambu_tests.helpers import ClientMock, MultithreadedBookmarkGeneratorFake
from tap_mambu.tap_generators.multithreaded_bookmark_generator import MultithreadedBookmarkGenerator


def test_init_variable_params():
    mock_client = ClientMock()
    mock_config = Mock()
    mock_config.start_date = "2019-11-25T00:00:00.000000Z"

    generator = MultithreadedBookmarkGeneratorFake(config=mock_config)

    assert generator.artificial_limit == mock_client.page_size
    assert generator.limit == mock_client.page_size + generator.overlap_window


def test_error_check_and_fix():
    generator = MultithreadedBookmarkGeneratorFake()
    generator.overlap_window = 2
    generator.artificial_limit = 3

    # test when a and b have the same values on the overlap window indexes
    a = {1, 2, 3, 4, 5}
    b = {4, 5, 6, 7, 8}
    assert generator.error_check_and_fix(a, b) == {1, 2, 3, 4, 5, 6, 7, 8}

    # test when a and b don't overlap 100% but the error it's manageable
    a = {1, 2, 3, 4, 5}
    b = {5, 6, 7, 8, 9, 10}
    assert generator.error_check_and_fix(a, b) == {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

    # test when a and b don't overlap at all
    with pytest.raises(RuntimeError):
        a = {1, 2, 3, 4, 5}
        b = {6, 7, 8}
        assert generator.error_check_and_fix(a, b) == {1, 2, 3, 4, 5, 6, 7, 8}


def test_set_intermediary_bookmark():
    bookmark_field_name = 'testField'
    record = {'test_field': '2020-01-01T00:00:00.000000Z',
              'dummy': 'data',
              'number': 23}

    generator = MultithreadedBookmarkGeneratorFake()
    generator.endpoint_bookmark_field = bookmark_field_name

    # test if the initial values aren't modified
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # test if the method exits if the bookmark isn't found in the record
    # also check if there isn't any variables set by mistake
    no_bookmark_field_record = {'test': '2020-01-01T00:00:00.000Z'}
    assert generator.set_intermediary_bookmark(no_bookmark_field_record) is None
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # check if the endpoint intermediary bookmark value and offset are set for a "correct" record
    generator.set_intermediary_bookmark(record)
    assert generator.endpoint_intermediary_bookmark_value == record['test_field']
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value and offset correctly set when multiple records have
    # the same bookmark field value
    for _ in range(3):
        generator.set_intermediary_bookmark(record)
    assert generator.endpoint_intermediary_bookmark_value == record['test_field']
    assert generator.endpoint_intermediary_bookmark_offset == 4

    # check if the endpoint intermediary bookmark value is properly set and the offset is reset
    record['test_field'] = '2021-11-11T00:00:00.000000Z'
    generator.set_intermediary_bookmark(record)
    assert generator.endpoint_intermediary_bookmark_value == record['test_field']
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
       "MultithreadedBookmarkGenerator.fetch_batch_continuously")
def test_iter_flow(mock_fetch_batch_continuously):
    generator = MultithreadedBookmarkGeneratorFake()

    mock_fetch_batch_continuously.assert_not_called()
    assert generator.fetch_batch_thread is None

    iter(generator)
    mock_fetch_batch_continuously.assert_called_once()
    assert generator.fetch_batch_thread is not None


def test_next_flow():
    test_records = [{"encoded_key": "1", "creation_date": '2020-01-01T00:00:000000Z'},
                    {"encoded_key": "2", "creation_date": '2020-01-01T00:00:000000Z'},
                    {"encoded_key": "3", "creation_date": '2020-01-01T00:00:000000Z'}, ]
    generator = MultithreadedBookmarkGeneratorFake()
    generator.buffer = list(test_records)

    for idx, record in enumerate(test_records):
        assert next(generator) == test_records[idx]

    with pytest.raises(StopIteration):
        generator.end_of_file = True
        next(generator)


@patch.object(MultithreadedBookmarkGenerator, 'stop_all_request_threads',
              side_effect=MultithreadedBookmarkGenerator.stop_all_request_threads,
              autospec=True)
@patch.object(MultithreadedBookmarkGenerator, 'error_check_and_fix',
              side_effect=MultithreadedBookmarkGenerator.error_check_and_fix,
              autospec=True)
@patch.object(MultithreadedBookmarkGenerator, 'prepare_batch',
              side_effect=MultithreadedBookmarkGenerator.prepare_batch,
              autospec=True)
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator."
       "MultithreadedBookmarkGenerator.set_intermediary_bookmark")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.MultithreadedBookmarkGenerator.transform_batch")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.transform_json")
@patch("tap_mambu.tap_generators.multithreaded_bookmark_generator.MultithreadedRequestsPool.queue_request")
def test_all_fetch_batch_steps_flow(mock_queue_request,
                                    mock_transform_json,
                                    mock_transform_batch,
                                    mock_set_intermediary_bookmark,
                                    mock_prepare_batch,
                                    mock_error_check_and_fix,
                                    mock_stop_all_request_threads):
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
    mock_overlap_window = 30
    mock_batch_limit = 2500
    mock_artificial_limit = mock_client.page_size

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

    # generate fake data as if they were extracted from the API
    mock_records = [[{'encoded_key': f'0-{record_no}'}
                     for record_no in range(0, mock_client.page_size + mock_overlap_window)], ]
    for batch_no in range(1, mock_batch_limit // mock_artificial_limit):
        mock_batch = mock_records[batch_no - 1][-mock_overlap_window:]
        for record_no in range(0, mock_client.page_size):
            mock_batch.append({'encoded_key': f'{batch_no}-{record_no}'})
        mock_records.append(mock_batch)

    mock_transform_batch.side_effect = mock_records

    assert generator.offset == 0
    all_fetch_batch_steps_output = generator._all_fetch_batch_steps()

    assert all_fetch_batch_steps_output is True

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
    assert mock_transform_json.call_count == mock_batch_limit / mock_artificial_limit
    assert mock_transform_batch.call_count == mock_batch_limit / mock_artificial_limit
    assert mock_error_check_and_fix.call_count == \
           (mock_batch_limit / mock_artificial_limit) - 1  # -1 because the first call is skipped
    mock_stop_all_request_threads.assert_not_called()

    assert all(record in generator.buffer for batch in mock_records for record in batch)

    # the left assert branch explained: removed the overlap window from each batch in order to remove
    # the duplicated records
    assert mock_set_intermediary_bookmark.call_count == \
           sum(len(batch) - mock_overlap_window for batch in mock_records) + mock_overlap_window
