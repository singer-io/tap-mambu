import datetime
import pytz

from mock import MagicMock, Mock

from tap_mambu.tap_generators.multithreaded_bookmark_generator import MultithreadedOffsetGenerator, \
    MultithreadedBookmarkGenerator, MultithreadedBookmarkDayByDayGenerator


class IsInstanceMatcher:
    def __init__(self, expected_type):
        self.expected_type = expected_type

    def __eq__(self, other):
        return isinstance(other, self.expected_type)


class IsInstanceMockMatcher:
    def __init__(self, expected_type):
        self.expected_type = expected_type

    def __eq__(self, other):
        return isinstance(other, MagicMock) and other.type == str(self.expected_type)


class GeneratorMock:
    def __init__(self, data):
        self.data = data
        self.time_extracted = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        self.endpoint_bookmark_field = "last_modified_date"
        self.endpoint_sorting_criteria = {'field': 'id'}
        self.client = "mock_client"
        self.state = "mock_state"
        self.config = "mock_config"

    def __iter__(self):
        return self

    def __next__(self):
        if not self.data:
            raise StopIteration
        return self.data.pop(0)

    def write_bookmark(self):
        pass


class ClientMock:
    def __init__(self, page_size=100):
        self.page_size = page_size


class MultithreadedOffsetGeneratorFake(MultithreadedOffsetGenerator):
    def __init__(self, stream_name='test_stream', client=ClientMock(), config=Mock(), state='', sub_type=''):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type
        super().__init__(stream_name, client, config, state, sub_type)


class MultithreadedBookmarkGeneratorFake(MultithreadedBookmarkGenerator):
    def __init__(self, stream_name='test_stream', client=ClientMock(), config=Mock(), state='', sub_type=''):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type
        super().__init__(stream_name, client, config, state, sub_type)


class MultithreadedBookmarkDayByDayGeneratorFake(MultithreadedBookmarkDayByDayGenerator):
    def __init__(self, stream_name='test_stream', client=ClientMock(), config=Mock(), state='', sub_type=''):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type
        super().__init__(stream_name, client, config, state, sub_type)
