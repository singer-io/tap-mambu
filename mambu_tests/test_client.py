from unittest import mock
from unittest.mock import Mock

from tap_mambu import MambuClient
from tap_mambu.helpers.client import MambuNoCredInConfig, MambuNoSubdomainInConfig
import pytest
from .constants import config_json


@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_no_entry_check_access(mock_check_access):
    _ = MambuClient(username=config_json.get('username'),
                    password=config_json.get('password'),
                    subdomain=config_json['subdomain'],
                    page_size=int(config_json.get('page_size', 500)),
                    user_agent=config_json['user_agent'],
                    apikey='',
                    apikey_audit='')
    mock_check_access.assert_not_called()


@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
def test_client_entry_check_access(mock_check_access):
    client = MambuClient(username=config_json.get('username'),
                         password=config_json.get('password'),
                         subdomain=config_json['subdomain'],
                         page_size=int(config_json.get('page_size', 500)),
                         user_agent=config_json['user_agent'],
                         apikey='',
                         apikey_audit='')
    with client:
        mock_check_access.assert_called()


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

    mock_requests_session_get.assert_called_with(url=f'{base_url}/settings/organization',
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

    mock_requests_session_get.assert_called_with(url=f'{base_url}/settings/organization',
                                                 headers={'Accept': 'application/vnd.mambu.v1+json'})


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
