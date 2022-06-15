from tap_mambu.helpers.multithreaded_requests import MultithreadedRequestsPool


_original_shutdown = MultithreadedRequestsPool.shutdown


def pytest_collection(session):
    from mock import MagicMock
    MultithreadedRequestsPool.shutdown = MagicMock()


def pytest_sessionfinish(session, exitstatus):
    _original_shutdown()
