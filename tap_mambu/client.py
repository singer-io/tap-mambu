import backoff
import requests
from requests.exceptions import ConnectionError
from singer import metrics
import singer

LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class MambuError(Exception):
    pass


class MambuBadRequestError(MambuError):
    pass


class MambuUnauthorizedError(MambuError):
    pass


class MambuRequestFailedError(MambuError):
    pass


class MambuNotFoundError(MambuError):
    pass


class MambuMethodNotAllowedError(MambuError):
    pass


class MambuConflictError(MambuError):
    pass


class MambuForbiddenError(MambuError):
    pass


class MambuUnprocessableEntityError(MambuError):
    pass


class MambuInternalServiceError(MambuError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: MambuBadRequestError,
    401: MambuUnauthorizedError,
    402: MambuRequestFailedError,
    403: MambuForbiddenError,
    404: MambuNotFoundError,
    405: MambuMethodNotAllowedError,
    409: MambuConflictError,
    422: MambuUnprocessableEntityError,
    500: MambuInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, MambuError)

def raise_for_error(response):
    LOGGER.error('ERROR {}: {}, REASON: {}'.format(response.status_code,\
        response.text, response.reason))
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Mambu has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
                error_code = response.get('status')
                ex = get_exception_for_error_code(error_code)
                raise ex(message)
            else:
                raise MambuError(error)
        except (ValueError, TypeError):
            raise MambuError(error)


class MambuClient(object):
    def __init__(self,
                 username,
                 password,
                 subdomain,
                 user_agent=None):
        self.__username = username
        self.__password = password
        self.__subdomain = subdomain
        base_url = "https://{}.mambu.com/api".format(subdomain)
        self.base_url = base_url
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__verified = False

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def check_access(self):
        if self.__username is None or self.__password is None:
            raise Exception('Error: Missing username or password in config.json.')
        if self.__subdomain is None:
            raise Exception('Error: Missing subdomain in cofig.json.')
        headers = {}
        # Endpoint: simple API call to return a single record (org settings) to test access
        # https://support.mambu.com/docs/organisational-settings-api#get-organisational-settings
        endpoint = 'settings/organization'
        url = '{}/{}'.format(self.base_url, endpoint)
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/vnd.mambu.v1+json'
        response = self.__session.get(
            url=url,
            headers=headers,
            # Basic Authentication: https://api.mambu.com/?http#authentication
            auth=(self.__username, self.__password))
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            return True


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    def request(self, method, path=None, url=None, json=None, version=None, **kwargs):
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
        kwargs['headers']['Accept'] = 'application/vnd.mambu.{}+json'.format(version)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method=method,
                url=url,
                # Basic Authentication: https://api.mambu.com/?http#authentication
                auth=(self.__username, self.__password),
                json=json,
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()
