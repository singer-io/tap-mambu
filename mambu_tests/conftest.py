# TODO: find a better solution
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
