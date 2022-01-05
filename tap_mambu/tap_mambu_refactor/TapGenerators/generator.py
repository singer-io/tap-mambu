from abc import ABC
from typing import List
from singer import utils

from ..Helpers import write_bookmark, transform_json, get_bookmark


class TapGenerator(ABC):
    def __init__(self, stream_name, client, config, state, sub_type, endpoint_config=None):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type
        self.endpoint_config = endpoint_config
        self._init_config()
        self._init_endpoint_config()
        self._init_buffers()
        self._init_bookmarks()
        self._init_params()

    def _init_config(self):
        self.start_date = self.config.get('start_date')

    def _init_endpoint_config(self):
        if self.endpoint_config is None:
            self.endpoint_config = {}

    def _init_buffers(self):
        self.buffer: List = list()

    def _init_bookmarks(self):
        self.bookmark_query_field = self.endpoint_config.get('bookmark_query_field')
        self.bookmark_type = self.endpoint_config.get('bookmark_type')
        self.bookmark_field = self.endpoint_config.get('bookmark_field')
        if self.bookmark_type == "integer":
            self.last_bookmark_value = get_bookmark(self.state, self.stream_name, self.sub_type, 0)
        else:
            self.last_bookmark_value = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)
        self.max_bookmark_value = self.last_bookmark_value

    def _init_params(self):
        self.time_extracted = None
        self.static_params = self.endpoint_config.get('params', {})
        self.offset = 0
        self.limit = self.client.page_size
        self.params = self.static_params

    def __all_fetch_batch_steps(self):
        self.prepare_batch()
        raw_batch = self.fetch_batch()
        self.buffer = self.transform_batch(raw_batch)
        self.last_batch_size = len(self.buffer)

    def __iter__(self):
        self.__all_fetch_batch_steps()
        return self

    def __next__(self):
        if not self.buffer:
            if self.last_batch_size < self.limit:
                raise StopIteration()
            self.offset += self.limit
            # self.write_bookmark()
            self.__all_fetch_batch_steps()
            if not self.buffer:
                raise StopIteration()
        return self.buffer.pop(0)

    def write_bookmark(self):
        if self.bookmark_field:
            write_bookmark(self.state,
                           self.stream_name,
                           self.sub_type,
                           self.max_bookmark_value)

    def prepare_batch(self):
        self.params = {
            "offset": self.offset,
            "limit": self.limit,
            **self.static_params
        }

    def fetch_batch(self):
        if self.bookmark_query_field:
            self.params[self.bookmark_query_field] = self.last_bookmark_value
        response = self.client.request(
            method=self.endpoint_config.get('api_method', 'GET'),
            path=self.endpoint_config.get('path'),
            version=self.endpoint_config.get('api_version', 'v2'),
            apikey_type=self.endpoint_config.get('apikey_type', None),
            params='&'.join([f'{key}={value}' for (key, value) in self.params.items()]),
            endpoint=self.stream_name,
            json=self.endpoint_config.get('body', None))
        self.time_extracted = utils.now()
        if isinstance(response, dict):
            return [response]
        return response

    def transform_batch(self, batch):
        data_key = self.endpoint_config.get('data_key', None)
        transformed_batch = list()
        if data_key is None:
            transformed_batch = transform_json(batch, self.stream_name)
        elif data_key in batch:
            transformed_batch = transform_json(batch, data_key)[data_key]
        return transformed_batch
