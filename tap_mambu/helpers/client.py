import backoff
import requests
import requests.adapters
from requests.exceptions import ConnectionError
import singer
from singer import metrics
from .exceptions import (raise_for_error, MambuInternalServiceError, MambuNoCredInConfig, MambuError,
                         MambuNoSubdomainInConfig, MambuNoAuditApikeyInConfig, MambuApiLimitError, ERROR_CODE_EXCEPTION_MAPPING)

LOGGER = singer.get_logger()

class MambuClient(object):
    def __init__(self,
                 username,
                 password,
                 apikey,
                 subdomain,
                 apikey_audit,
                 page_size,
                 user_agent=''):
        self.__username = username
        self.__password = password
        self.__subdomain = subdomain
        base_url = "https://{}.mambu.com/api".format(subdomain)
        self.base_url = base_url
        self.page_size = page_size
        self.__user_agent = f'MambuTap-{user_agent}' if user_agent else 'MambuTap'
        self.__apikey = apikey
        self.__session = requests.Session()
        self.__adapter = requests.adapters.HTTPAdapter(pool_maxsize=100)
        self.__session.mount("https://", self.__adapter)
        self.__verified = False
        self.__apikey_audit = apikey_audit

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          MambuInternalServiceError,
                          max_tries=5,
                          factor=2)
    def check_access(self):
        use_apikey = False
        if self.__username is None or self.__password is None:
            if self.__apikey is None:
                raise MambuNoCredInConfig(
                    'Error: Missing username or password, or apikey in config.json.'
                )
            else:
                use_apikey = True
        if self.__subdomain is None:
            raise MambuNoSubdomainInConfig('Error: Missing subdomain in config.json.')
        headers = {}
        # Endpoint: simple API call to return a single record (org settings) to test access
        # https://support.mambu.com/docs/organisational-settings-api#get-organisational-settings
        endpoint = 'settings/organization'
        url = '{}/{}'.format(self.base_url, endpoint)
        headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/vnd.mambu.v1+json'
        if use_apikey:
            # Api Key API Consumer Authentication: https://support.mambu.com/docs/api-consumers
            self.__session.headers['apikey'] = self.__apikey
        else:
            # Basic Authentication: https://api.mambu.com/?http#authentication
            self.__session.auth = (self.__username, self.__password)
        response = self.__session.get(
            url=url,
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            return True

    @backoff.on_exception(backoff.expo,
                          (MambuInternalServiceError, ConnectionError, MambuApiLimitError),
                          max_tries=7,
                          factor=3)
    def request(self, method, path=None, url=None, json=None, version=None, apikey_type=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if not version:
            version = 'v2'

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        # Version represents API version (e.g. v2): https://api.mambu.com/?http#versioning
        if version == 'v2':
            kwargs['headers']['Accept'] = 'application/vnd.mambu.{}+json'.format(version)

        if apikey_type == 'audit':
            if self.__apikey_audit is None:
                raise MambuNoAuditApikeyInConfig(
                    'Error: Missing apikey_audit in config.json.'
                )
            else:
                kwargs['headers']['apikey'] = self.__apikey_audit

        kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method=method,
                url=url,
                json=json,
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code


        if response.status_code != 200:
            raise_for_error(response)

        return response.json()
