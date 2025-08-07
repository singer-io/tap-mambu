import backoff
import requests
import requests.adapters
from requests.exceptions import ConnectionError, ChunkedEncodingError
from singer import metrics, get_logger

from urllib3.exceptions import ProtocolError
from tap_mambu.helpers.constants import DEFAULT_DATE_WINDOW_SIZE

LOGGER = get_logger()
class ClientError(Exception):
    """class representing Generic Http error."""

    message = None

    def __init__(self, message=None, response=None):
        super().__init__(message or self.message)
        self.response = response

class MambuError(ClientError):

    message = "Unable to process request"

class MambuBadRequestError(MambuError):

    message = "400: Unable to process request"

class MambuUnauthorizedError(MambuError):

    message = "401: Invalid credentials provided"

class MambuRequestFailedError(MambuError):

    message = "402: Unable to process request"

class MambuNotFoundError(MambuError):

    message = "404: Resource not found"

class MambuMethodNotAllowedError(MambuError):

    message = "405: Method Not Allowed"

class MambuConflictError(MambuError):

    message = "409: Conflict"

class MambuForbiddenError(MambuError):

    message = "403: Insufficient permission to access resource"

class MambuUnprocessableEntityError(MambuError):

    message = "422: Unable to process request"

class MambuApiLimitError(ClientError):

    message = "429: The API limit exceeded"

class MambuInternalServiceError(MambuError):

    message = "Server Fault, Unable to process request"

class MambuNoCredInConfig(MambuError):

    message = "Creds Not Provided"

class MambuNoSubdomainInConfig(MambuError):

    message = "Subdomain not Configured"

class MambuNoAuditApikeyInConfig(MambuError):

    message = "API Key not provided"

ERROR_CODE_EXCEPTION_MAPPING = {
    400: MambuBadRequestError,
    401: MambuUnauthorizedError,
    402: MambuRequestFailedError,
    403: MambuForbiddenError,
    404: MambuNotFoundError,
    405: MambuMethodNotAllowedError,
    409: MambuConflictError,
    422: MambuUnprocessableEntityError,
    429: MambuApiLimitError,
    500: MambuInternalServiceError}

def raise_for_error(response):
    """
    Raises the associated response exception.
    :param resp: requests.Response object
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = response.status_code
            if response.status_code >= 500:
                exc = MambuInternalServiceError
                message = f"{response.status_code}: {MambuInternalServiceError.message}"
            else:
                exc, message = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, MambuError), None
            try:
                if len(response.content) != 0:
                    response = response.json()
                    if ('error' in response) or ('errorCode' in response):
                        message = '%s: %s' % (response.get('error', str(error)),
                                            response.get('message', 'Unknown Error'))
            except (ValueError, TypeError):
                pass
            raise exc(message, response) from None
        except (ValueError, TypeError):
            raise MambuError(error) from error

class MambuClient(object):
    def __init__(self,
                 username,
                 password,
                 apikey,
                 subdomain,
                 apikey_audit,
                 page_size,
                 user_agent='',
                 window_size=DEFAULT_DATE_WINDOW_SIZE):
        self.__username = username
        self.__password = password
        self.__subdomain = subdomain
        base_url = "https://{}.mambu.com/api".format(subdomain)
        self.base_url = base_url
        self.page_size = page_size
        try:
            self.window_size = int(float(window_size)) if window_size else DEFAULT_DATE_WINDOW_SIZE
            if self.window_size <= 0:
                raise ValueError()
        except ValueError:
            raise Exception("The entered window size '{}' is invalid; it should be a valid non-zero integer.".format(window_size))

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
        endpoint = '/setup/organization'
        url = '{}/{}'.format(self.base_url, endpoint)
        headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/vnd.mambu.v2+json'
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
                          (MambuInternalServiceError, ConnectionError,
                           ChunkedEncodingError, MambuApiLimitError, ProtocolError),
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
