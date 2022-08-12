from mock import Mock, patch
from time import sleep
from copy import deepcopy

from tap_mambu.helpers.multithreaded_requests import MultithreadedRequestsPool, PerformanceMetrics


@patch("tap_mambu.helpers.multithreaded_requests.ThreadPoolExecutor.submit")
def test_queue_request(mock_thread_pool_exec_submit):
    MultithreadedRequestsPool.queue_request('client', 'stream_name', 'stream_path', 'GET', 'v2', '', {}, {})

    # test if the submit params are the same as queue_request params
    mock_thread_pool_exec_submit.assert_called_once_with(MultithreadedRequestsPool.run, 'client', 'stream_name',
                                                         'stream_path', 'GET', 'v2', '', {}, {})


@patch("tap_mambu.helpers.client.MambuClient.request")
@patch("tap_mambu.helpers.multithreaded_requests.PerformanceMetrics", wraps=PerformanceMetrics)
@patch("tap_mambu.helpers.multithreaded_requests.MultithreadedRequestsPool.run", wraps=MultithreadedRequestsPool.run)
def test_run(mock_multithreaded_request_pool_run, mock_performance_metrics, mock_client_request):
    request_result = [{'status_code': 200,
                       'content': {'test': 'result'}},
                      {'status_code': 400,
                       'content': {'test': ''}},
                      {'status_code': 500,
                       'content': {}},
                      ]
    mock_client = Mock()
    mock_client.request = mock_client_request
    mock_client.request.side_effect = deepcopy(request_result)

    client = mock_client
    stream_names = ['test_stream_name1', 'test_stream_name2', '']
    endpoint_paths = ['test_stream_path1', 'test_stream_path2', '']
    api_methods = ['GET', 'POST', '']
    endpoint_api_version = 'v2'
    endpoint_api_key_type = ''
    endpoint_body = [{'filterCriteria': {"field": "lastModifiedDate",
                                         "operator": "AFTER",
                                         "value": ''}
                      },
                     {'filterCriteria': {"field": "creationDate",
                                         "operator": "BETWEEN",
                                         "value": '2020-01-01T12:00:00.000000Z',
                                         "secondValue": '2020-02-01T03:01:20.000000Z'},
                      'sortingCriteria': {"field": "encodedKey",
                                          "order": "ASC"}
                      },
                     {'filterCriteria': {"field": "",
                                         "operator": "",
                                         "value": ''}
                      }
                     ]
    endpoint_params = [{'offset': 500, 'limit': 510},
                       {'offset': 0, 'limit': 0},
                       {'offset': '', 'limit': ''}]

    futures = []
    for idx, stream_name in enumerate(stream_names):
        futures.append(
            MultithreadedRequestsPool.queue_request(client, stream_name, endpoint_paths[idx], api_methods[idx],
                                                    endpoint_api_version, endpoint_api_key_type,
                                                    endpoint_body, endpoint_params[idx]))
    for idx, future in enumerate(futures):
        while not future.done():
            sleep(0.001)

        # test that the request response isn't modified
        assert future.result() == request_result[idx]

    for idx, stream_name in enumerate(stream_names):
        # test if the run method is called with the correct params
        mock_multithreaded_request_pool_run.assert_any_call(client, stream_name,
                                                            endpoint_paths[idx], api_methods[idx],
                                                            endpoint_api_version, endpoint_api_key_type,
                                                            endpoint_body, endpoint_params[idx])
        # test if the client request method is called with the correct params
        mock_client_request.assert_any_call(method=api_methods[idx],
                                            path=endpoint_paths[idx],
                                            version=endpoint_api_version,
                                            apikey_type=endpoint_api_key_type,
                                            params=f'offset={endpoint_params[idx]["offset"]}&limit={endpoint_params[idx]["limit"]}',
                                            endpoint=stream_name,
                                            json=endpoint_body)

    # test if the performance metrics collector is called
    mock_performance_metrics.assert_called_with(metric_name='generator')
