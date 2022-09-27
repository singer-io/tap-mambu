_threads_original_shutdown = None


def pytest_collection(session):
    import sys
    from mock import MagicMock

    def mock_on_exception(*args, **kwargs):
        def retry(func):
            return func

        return retry

    # this assignment will replace the backoff lib for all tests from this file, but this is acceptable because
    # there isn't any need of its functionality
    sys.modules['backoff'] = MagicMock()
    sys.modules['backoff'].on_exception = mock_on_exception

    from tap_mambu.helpers.multithreaded_requests import MultithreadedRequestsPool

    global _threads_original_shutdown
    _threads_original_shutdown = MultithreadedRequestsPool.shutdown
    MultithreadedRequestsPool.shutdown = MagicMock()


    from pytz import timezone
    from tap_mambu.helpers import datetime_utils
    datetime_utils._timezone = timezone("US/Pacific")


def pytest_sessionfinish(session, exitstatus):
    _threads_original_shutdown()
