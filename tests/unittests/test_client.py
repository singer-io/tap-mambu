import requests
import pytest
import mock
from mock import Mock

from tap_mambu import MambuClient, DEFAULT_PAGE_SIZE
from tap_mambu.helpers.client import MambuError, MambuNoCredInConfig, MambuNoSubdomainInConfig, \
    MambuNoAuditApikeyInConfig, MambuInternalServiceError, raise_for_error, ERROR_CODE_EXCEPTION_MAPPING


config_json = {'username': 'unit',
               'password': 'test',
               'apikey': '',
               'subdomain': 'unit.test',
               'test_full_url': 'http://127.0.0.1:1080/test',
               'start_date': '2021-06-01T00:00:00Z',
               'lookback_window': 30,
               'user_agent': '',
               'page_size': '200',
               'apikey_audit': 'apikey_audit_test'}


@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_entry_check_access(mock_check_access):
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey='',
                         apikey_audit='')

    mock_check_access.assert_not_called()
    with client:
        mock_check_access.assert_called_once()


@mock.patch("tap_mambu.helpers.client.requests.Session.get")
def test_client_check_access_api_key(mock_requests_session_get):
    response = Mock()
    response.status_code = 200
    mock_requests_session_get.return_value = response

    client = MambuClient(username=config_json.get('username'),
                         password=None,
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent='User_Agent_Test',
                         apikey='apikey_test',
                         apikey_audit='')
    base_url = "https://unit.test.mambu.com/api"
    assert client.check_access()
    assert client.base_url == base_url
    assert client.page_size == DEFAULT_PAGE_SIZE

    mock_requests_session_get.assert_called_once_with(url=f'{base_url}/setup/organization',
                                                      headers={'Accept': 'application/vnd.mambu.v2+json',
                                                               'User-Agent': 'MambuTap-User_Agent_Test'})


@mock.patch("tap_mambu.helpers.client.requests.Session.get")
def test_client_check_access_user_pass(mock_requests_session_get):
    response = Mock()
    response.status_code = 200
    mock_requests_session_get.return_value = response

    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain='test_subdomain',
                         page_size=100,
                         user_agent='',
                         apikey=None,
                         apikey_audit='')

    base_url = 'https://test_subdomain.mambu.com/api'
    assert client.check_access()
    assert client.base_url == base_url
    assert client.page_size == 100

    mock_requests_session_get.assert_called_once_with(url=f'{base_url}/setup/organization',
                                                      headers={'Accept': 'application/vnd.mambu.v2+json',
                                                               'User-Agent': 'MambuTap'})


@mock.patch("tap_mambu.helpers.client.raise_for_error")
@mock.patch("tap_mambu.helpers.client.requests.Session.get")
def test_client_check_status_code_fail(mock_requests_session_get, mock_raise_for_error):
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey=None,
                         apikey_audit='')
    for status_code in [*range(0, 200), *range(201, 600)]:
        response = Mock()
        response.status_code = status_code
        mock_requests_session_get.return_value = response

        client.check_access()
        mock_raise_for_error.assert_called_with(response)


def test_client_check_access_no_auth_cred():
    client = MambuClient(username=None,
                         password=None,
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey=None,
                         apikey_audit='')

    with pytest.raises(MambuNoCredInConfig):
        client.check_access()


def test_client_check_access_no_subdomain():
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=None,
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey='',
                         apikey_audit='')

    with pytest.raises(MambuNoSubdomainInConfig):
        client.check_access()


@mock.patch("tap_mambu.helpers.client.metrics.http_request_timer")
@mock.patch("tap_mambu.helpers.client.requests.Session.request")
@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_request_get_flow(mock_check_access, mock_requests_session_request, mock_metrics_http_request_timer):
    response = Mock()
    response.status_code = 200
    response.json.return_value = {'test_record': {'data1': 1, 'data2': 2}}
    mock_requests_session_request.return_value = response

    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain='test_subdomain',
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent='test_user_agent',
                         apikey='',
                         apikey_audit='test_apikey_audit')

    req_response = client.request(method='GET', path='test_path', apikey_type='audit')
    mock_metrics_http_request_timer.assert_called_once_with(None)
    mock_requests_session_request.assert_called_once_with(method='GET',
                                                          url='https://test_subdomain.mambu.com/api/test_path',
                                                          json=None,
                                                          headers={'Accept': 'application/vnd.mambu.v2+json',
                                                                   'apikey': 'test_apikey_audit',
                                                                   'User-Agent': 'MambuTap-test_user_agent'})
    assert req_response == {'test_record': {'data1': 1, 'data2': 2}}

    client.request(method='POST', path='test_path', apikey_type='audit')
    mock_requests_session_request.assert_called_with(method='POST',
                                                     url='https://test_subdomain.mambu.com/api/test_path',
                                                     json=None,
                                                     headers={'Accept': 'application/vnd.mambu.v2+json',
                                                              'apikey': 'test_apikey_audit',
                                                              'User-Agent': 'MambuTap-test_user_agent',
                                                              'Content-Type': 'application/json'})
    mock_metrics_http_request_timer.assert_called_with(None)
    mock_check_access.assert_called_once()


@mock.patch("tap_mambu.helpers.client.metrics.http_request_timer")
@mock.patch("tap_mambu.helpers.client.requests.Session.request")
@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_request_post_flow(mock_check_access, mock_requests_session_request, mock_metrics_http_request_timer):
    response = Mock()
    response.status_code = 200
    response.json.return_value = {}
    mock_requests_session_request.return_value = response

    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent='',
                         apikey=None,
                         apikey_audit='')

    client.request(method='POST',
                   url='www.api.test_url',
                   json={'test': 'filter'},
                   version='v_test',
                   headers={'Accept': 'test_accept_header',
                            'User-Agent': 'test_user_agent',
                            'Content-Type': 'test_content_type'},
                   endpoint='test_endpoint_first')
    mock_metrics_http_request_timer.assert_called_once_with('test_endpoint_first')
    mock_requests_session_request.assert_called_once_with(method='POST',
                                                          url='www.api.test_url',
                                                          json={'test': 'filter'},
                                                          headers={'Accept': 'test_accept_header',
                                                                   'User-Agent': 'MambuTap',
                                                                   'Content-Type': 'application/json'})

    client.request(method='GET',
                   url='www.api.test_url',
                   json={'test': 'filter'},
                   version='v_test',
                   headers={'Accept': 'test_accept_header',
                            'User-Agent': 'test_user_agent',
                            'Content-Type': 'test_content_type'},
                   endpoint='test_endpoint_second')
    mock_requests_session_request.assert_called_with(method='GET',
                                                     url='www.api.test_url',
                                                     json={'test': 'filter'},
                                                     headers={'Accept': 'test_accept_header',
                                                              'User-Agent': 'MambuTap',
                                                              'Content-Type': 'test_content_type'})
    mock_metrics_http_request_timer.assert_called_with('test_endpoint_second')
    mock_check_access.assert_called_once()


@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_request_no_audit_apikey(mock_check_access):
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey='',
                         apikey_audit=None)

    with pytest.raises(MambuNoAuditApikeyInConfig):
        client.request(method='GET', apikey_type='audit')


@mock.patch("tap_mambu.helpers.client.requests.models.Response.raise_for_status")
def test_client_request_500_error(mock_requests_raise_for_status):
    mock_requests_raise_for_status.side_effect = Mock(side_effect=requests.HTTPError('test'))

    for status_code in range(500, 600):
        content = {'error': status_code,
                   'status': status_code,
                   'message': 'test'}
        response = Mock()
        response.content = content
        response.json.return_value = content
        response.status_code = status_code
        response.raise_for_status = mock_requests_raise_for_status
        with pytest.raises(MambuInternalServiceError):
            raise_for_error(response)
        response.json.assert_called_once()


@mock.patch("tap_mambu.helpers.client.raise_for_error")
@mock.patch("tap_mambu.helpers.client.metrics.http_request_timer")
@mock.patch("tap_mambu.helpers.client.requests.Session.request")
@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_request_raise_for_error(mock_check_access,
                                        mock_requests_session_request,
                                        mock_metrics_http_request_timer,
                                        mock_raise_for_error):
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey='',
                         apikey_audit='')

    for status_code in [*range(0, 200), *range(201, 500)]:
        response = Mock()
        response.status_code = status_code
        mock_requests_session_request.return_value = response

        client.request(method='GET')
        mock_raise_for_error.assert_called_with(response)


@mock.patch("tap_mambu.helpers.client.requests.models.Response.raise_for_status")
def test_raise_for_error_call(mock_requests_raise_for_status):
    response = Mock()
    response.raise_for_status = mock_requests_raise_for_status

    raise_for_error(response)

    # tested if the raise_for_status function was called
    mock_requests_raise_for_status.assert_called_once()


@mock.patch("tap_mambu.helpers.client.requests.models.Response.raise_for_status")
def test_raise_for_error_no_content(mock_requests_raise_for_status):
    mock_requests_raise_for_status.side_effect = Mock(side_effect=requests.HTTPError('test'))
    response = Mock()
    response.content = []
    response.raise_for_status = mock_requests_raise_for_status

    # test case: when something from the try/except HTTPError block throws ValueError, TypeError ->
    # it should raise MambuError
    response.content = None
    with pytest.raises(MambuError):
        raise_for_error(response)
    response.json.assert_not_called()

    # test case: when the response content isn't empty but it also doesn't contain "error" or "errorCode" ->
    # it should raise MambuError
    response.content = {'error': 404,
                        'status': 404,
                        'message': 'not found'}
    response.json.return_value = {}
    with pytest.raises(MambuError):
        raise_for_error(response)


@mock.patch("tap_mambu.helpers.client.requests.models.Response.raise_for_status")
def test_raise_for_error_known_error_codes(mock_requests_raise_for_status):
    mock_requests_raise_for_status.side_effect = Mock(side_effect=requests.HTTPError('test'))

    for status_code in range(300, 501):
        content = {'error': status_code,
                   'status': status_code,
                   'message': 'test'}
        response = Mock()
        response.content = content
        response.json.return_value = content
        response.status_code = status_code
        response.raise_for_status = mock_requests_raise_for_status

        # test if the exceptions are handled correctly, meaning ->
        # - throw the custom exceptions for the defined ones
        # - throw MambuError for others
        if status_code not in ERROR_CODE_EXCEPTION_MAPPING:
            with pytest.raises(MambuError):
                raise_for_error(response)
        else:
            with pytest.raises(ERROR_CODE_EXCEPTION_MAPPING[status_code]):
                raise_for_error(response)
        response.json.assert_called_once()


@pytest.mark.parametrize("status_code, verify_message, expected_exception", [
    (400, "400: Unable to process request",ERROR_CODE_EXCEPTION_MAPPING[400]),
    (401, "401: Invalid credentials provided",ERROR_CODE_EXCEPTION_MAPPING[401]),
    (402, "402: Unable to process request",ERROR_CODE_EXCEPTION_MAPPING[402]),
    (403, "403: Insufficient permission to access resource",ERROR_CODE_EXCEPTION_MAPPING[403]),
    (404, "404: Resource not found",ERROR_CODE_EXCEPTION_MAPPING[404]),
    (405, "405: Method Not Allowed",ERROR_CODE_EXCEPTION_MAPPING[405]),
    (409, "409: Conflict",ERROR_CODE_EXCEPTION_MAPPING[409]),
    (422, "422: Unable to process request",ERROR_CODE_EXCEPTION_MAPPING[422]),
    (500, "500: Server Fault, Unable to process request",ERROR_CODE_EXCEPTION_MAPPING[500]),
    (503, "503: Server Fault, Unable to process request",ERROR_CODE_EXCEPTION_MAPPING[500]),
    (504, "504: Server Fault, Unable to process request",ERROR_CODE_EXCEPTION_MAPPING[500])
    ])
@mock.patch("tap_mambu.helpers.client.requests.models.Response.raise_for_status")
def test_error_messages(mock_requests_raise_for_status, status_code, verify_message, expected_exception):
    mock_requests_raise_for_status.side_effect = Mock(side_effect=requests.HTTPError())

    response = Mock()
    response.status_code = status_code
    response.raise_for_status = mock_requests_raise_for_status

    with pytest.raises(expected_exception):
        try: 
            raise_for_error(response)
        except expected_exception as err:
            assert str(err) == verify_message
            raise err
