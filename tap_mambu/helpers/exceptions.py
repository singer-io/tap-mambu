import requests
import requests.adapters
from requests.exceptions import ConnectionError

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

class NoDeduplicationCapabilityException(Exception):
    pass

class NoDeduplicationKeyException(Exception):
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
        except (ValueError, TypeError) as exap:
            raise MambuError(error) from None