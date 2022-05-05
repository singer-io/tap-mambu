import pytest
import mock
from mock import Mock

from tap_mambu import MambuClient
from tap_mambu.helpers.client import MambuNoCredInConfig, MambuNoSubdomainInConfig, MambuNoAuditApikeyInConfig, \
    Server5xxError
from .constants import config_json


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
    assert client.page_size == 500

    mock_requests_session_get.assert_called_once_with(url=f'{base_url}/settings/organization',
                                                      headers={'Accept': 'application/vnd.mambu.v1+json',
                                                               'User-Agent': 'User_Agent_Test'})


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

    mock_requests_session_get.assert_called_once_with(url=f'{base_url}/settings/organization',
                                                      headers={'Accept': 'application/vnd.mambu.v1+json'})


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
                                                                   'User-Agent': 'test_user_agent'})
    assert req_response == {'test_record': {'data1': 1, 'data2': 2}}

    client.request(method='POST', path='test_path', apikey_type='audit')
    mock_requests_session_request.assert_called_with(method='POST',
                                                     url='https://test_subdomain.mambu.com/api/test_path',
                                                     json=None,
                                                     headers={'Accept': 'application/vnd.mambu.v2+json',
                                                              'apikey': 'test_apikey_audit',
                                                              'User-Agent': 'test_user_agent',
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
                         user_agent=config_json['user_agent'],
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
                                                                   'User-Agent': 'test_user_agent',
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
                                                              'User-Agent': 'test_user_agent',
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


@mock.patch("tap_mambu.helpers.client.backoff._decorator._sync.time.sleep")
@mock.patch("tap_mambu.helpers.client.metrics.http_request_timer")
@mock.patch("tap_mambu.helpers.client.requests.Session.request")
@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_request_500_error(mock_check_access,
                                  mock_requests_session_request,
                                  mock_metrics_http_request_timer,
                                  mock_backoff_on_exception, ):
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey='',
                         apikey_audit='')

    for status_code in range(500, 600):
        response = Mock()
        response.status_code = status_code
        mock_requests_session_request.return_value = response

        with pytest.raises(Server5xxError):
            client.request(method='GET')


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
