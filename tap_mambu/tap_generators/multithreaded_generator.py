import concurrent.futures
import time
from threading import Thread
from singer import get_logger

from .generator import TapGenerator
from ..helpers import transform_json
from ..helpers.perf_metrics import PerformanceMetrics

LOGGER = get_logger()


class MultithreadedGenerator(TapGenerator):
    max_threads = 100
    max_buffer_size = 20000

    def _init_buffers(self):
        super(MultithreadedGenerator, self)._init_buffers()
        self.fetch_thread = None
        self.end_of_file = False

    def fetch_batch_chunk(self, offset):
        my_params = dict(self.params)
        my_params["offset"] = offset
        my_params["limit"] = self.limit
        endpoint_querystring = '&'.join([f'{key}={value}' for (key, value) in my_params.items()])

        if self.end_of_file:
            raise EOFError("No more data to be extracted!")

        LOGGER.info(f'(generator) Stream {self.stream_name} - URL for {self.stream_name} ({self.endpoint_api_method}, '
                    f'{self.endpoint_api_version}): {self.client.base_url}/{self.endpoint_path}?{endpoint_querystring}')
        LOGGER.info(f'(generator) Stream {self.stream_name} - body = {self.endpoint_body}')
        with PerformanceMetrics(metric_name="generator"):
            response = self.client.request(
                method=self.endpoint_api_method,
                path=self.endpoint_path,
                version=self.endpoint_api_version,
                apikey_type=self.endpoint_api_key_type,
                params=endpoint_querystring,
                endpoint=self.stream_name,
                json=self.endpoint_body
            )

        LOGGER.info(f'(generator) Stream {self.stream_name} - extracted records: {len(response)}')

        if not response:
            raise EOFError("No more data to be extracted!")
        return response

    def fetch_batch_continuously(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = list()
            running_threads = 0
            while not self.end_of_file:
                if len(self.buffer) > self.max_buffer_size:
                    with PerformanceMetrics(metric_name="generator_wait"):
                        while len(self.buffer) > self.max_buffer_size:
                            time.sleep(0.01)

                futures_to_delete = list()
                for future in futures:
                    if not future.done():
                        break
                    try:
                        raw_batch = future.result()
                    except TimeoutError:
                        break
                    except EOFError:
                        self.end_of_file = True
                        break

                    futures_to_delete.append(future)
                    running_threads -= 1

                    # for record in raw_batch:
                    #     for key, value in record['activity'].items():
                    #         record[key] = value
                    #     del record['activity']
                    for record in transform_json(raw_batch, self.stream_name):
                        self.buffer.append(record)

                for future_to_delete in futures_to_delete:
                    futures.remove(future_to_delete)

                while running_threads < self.max_threads and len(self.buffer) <= self.max_buffer_size:
                    futures.append(executor.submit(MultithreadedGenerator.fetch_batch_chunk, self, self.offset))
                    self.offset += self.limit
                    running_threads += 1

    def next(self):
        if not self.buffer and not self.end_of_file:
            with PerformanceMetrics(metric_name="processor_wait"):
                while not self.buffer and not self.end_of_file:
                    time.sleep(0.01)
        if not self.buffer and self.end_of_file:
            raise StopIteration()
        return self.buffer.pop(0)

    def _all_fetch_batch_steps(self):
        if self.fetch_thread is None:
            self.fetch_thread = Thread(target=MultithreadedGenerator.fetch_batch_continuously,
                                       args=(self,), daemon=True, name="FetchThread")
            self.fetch_thread.start()
