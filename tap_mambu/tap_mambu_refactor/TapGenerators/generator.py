import time
from typing import List

import requests


class TapGenerator:
    buffer: List

    def __init__(self, stream_name, client, config, endpoint_config):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.endpoint_config = endpoint_config
        self.__init_buffers()
        self.__init_bookmarks()
        self.__init_params()

    def __init_buffers(self):
        self.buffer = list()

    def __init_bookmarks(self):
        self.bookmark_query_field = self.endpoint_config.get('bookmark_query_field')
        self.bookmark_type = self.endpoint_config.get('bookmark_type')
        self.last_bookmark_value = 0 if self.bookmark_type == "integer" else self.start_date

    def __init_params(self):
        self.start_date = self.config.get('start_date')
        self.static_params = self.endpoint_config.get('params', {})
        self.offset = 0
        self.limit = self.client.page_size
        self.params = self.static_params

    def __iter__(self):
        self.buffer = self.fetch_batch()
        return self

    def __next__(self):
        if not self.buffer:
            self.buffer = self.fetch_batch()
        if not self.buffer:
            raise StopIteration()
        return self.buffer.pop(0)

    def fetch_batch(self):
        response = self.client.request(
            method=self.endpoint_config.get('api_method', 'GET'),
            path=self.endpoint_config.get('path'),
            version=self.endpoint_config.get('api_version', 'v2'),
            apikey_type=self.endpoint_config.get('apikey_type', None),
            params=self.params,
            endpoint=self.stream_name,
            json=self.endpoint_config.get('body', None))
        return response
