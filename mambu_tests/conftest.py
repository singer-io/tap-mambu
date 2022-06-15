from tap_mambu.helpers.multithreaded_requests import MultithreadedRequestsPool


_original_shutdown = MultithreadedRequestsPool.shutdown


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

    MultithreadedRequestsPool.shutdown = MagicMock()


def pytest_sessionfinish(session, exitstatus):
    _original_shutdown()
