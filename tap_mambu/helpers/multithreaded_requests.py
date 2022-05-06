from concurrent.futures import Future, ThreadPoolExecutor
from threading import Thread
from typing import List

from .perf_metrics import PerformanceMetrics


class MultithreadedRequestsPool:
    _dispatcher = ThreadPoolExecutor(max_workers=100)

    @classmethod
    def shutdown(cls):
        cls._dispatcher.shutdown(wait=True, cancel_futures=True)

    @staticmethod
    def run(client, stream_name,
            endpoint_path, endpoint_api_method, endpoint_api_version,
            endpoint_api_key_type, endpoint_body, endpoint_params) -> List[dict]:
        endpoint_querystring = '&'.join([f'{key}={value}' for (key, value) in endpoint_params.items()])

        # LOGGER.info(f'(generator) Stream {self.stream_name} - URL for {self.stream_name} ({self.endpoint_api_method}, '
        #             f'{self.endpoint_api_version}): {self.client.base_url}/{self.endpoint_path}?{endpoint_querystring}')
        # LOGGER.info(f'(generator) Stream {self.stream_name} - body = {self.endpoint_body}')

        with PerformanceMetrics(metric_name="generator"):
            response = client.request(
                method=endpoint_api_method,
                path=endpoint_path,
                version=endpoint_api_version,
                apikey_type=endpoint_api_key_type,
                params=endpoint_querystring,
                endpoint=stream_name,
                json=endpoint_body
            )

        # LOGGER.info(f'(generator) Stream {self.stream_name} - extracted records: {len(response)}')
        return response

    @classmethod
    def queue_request(cls, client, stream_name,
                      endpoint_path, endpoint_api_method, endpoint_api_version,
                      endpoint_api_key_type, endpoint_body, endpoint_params) -> Future:
        return cls._dispatcher.submit(cls.run, client, stream_name, endpoint_path, endpoint_api_method,
                                      endpoint_api_version, endpoint_api_key_type, endpoint_body, endpoint_params)
