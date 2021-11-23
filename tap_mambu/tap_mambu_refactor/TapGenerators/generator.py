import time
import requests

from typing import List
from singer import utils

from tap_mambu.transform import transform_json


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
        self.time_extracted = None
        self.static_params = self.endpoint_config.get('params', {})
        self.offset = 0
        self.limit = self.client.page_size
        self.params = self.static_params

    def __iter__(self):
        raw_batch = self.fetch_batch()
        self.buffer = self.transform_batch(raw_batch)
        return self

    def __next__(self):
        if not self.buffer:
            raw_batch = self.fetch_batch()
            self.buffer = self.transform_batch(raw_batch)
        if not self.buffer:
            raise StopIteration()
        return self.buffer.pop(0)

    def fetch_batch(self):
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

