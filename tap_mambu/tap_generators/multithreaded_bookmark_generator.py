import time
from copy import deepcopy
from typing import List

import backoff
from singer import utils, get_logger

from .generator import TapGenerator
from ..helpers import transform_json
from ..helpers.multithreaded_requests import MultithreadedRequestsPool
from ..helpers.perf_metrics import PerformanceMetrics

LOGGER = get_logger()


class MultithreadedBookmarkGenerator(TapGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type

        # Define parameters inside init
        self.params = dict()
        self.time_extracted = 0
        self.offset = 0

        # Initialize parameters
        self._init_config()
        self._init_endpoint_config()
        self._init_endpoint_body()
        self._init_buffers()
        self._init_params()

    def _init_config(self):
        self.start_date = self.config.get('start_date')

    def _init_endpoint_config(self):
        self.endpoint_path = ""
        self.endpoint_api_version = "v2"
        self.endpoint_api_method = "POST"
        self.endpoint_params = {
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_sorting_criteria = {
            "field": "encoded_key",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = []
        self.endpoint_api_key_type = None
        self.endpoint_bookmark_field = ""
        self.endpoint_intermediary_bookmark_value = 0

    def _init_endpoint_body(self):
        self.endpoint_body = {"sortingCriteria": self.endpoint_sorting_criteria,
                              "filterCriteria": self.endpoint_filter_criteria}

    def _init_buffers(self):
        self.buffer: List = list()

    def _init_params(self):
        self.time_extracted = None
        self.static_params = dict(self.endpoint_params)
        self.offset = 0
        self.limit = self.client.page_size
        self.batch_limit = 20000
        self.params = self.static_params

    def prepare_batch_params(self):
        self.offset = 0
        # here we change the date to the new one,
        # in order to paginate through the data using date, resetting offset to 0

    @staticmethod
    def error_check_and_fix(a, b, overlap_window=20):
        reunion = a[-overlap_window:]
        for record in b[:overlap_window]:
            if record not in reunion:
                reunion.append(record)
        if len(reunion) > 2*overlap_window:
            raise RuntimeError("Failed to error correct, aborting job.")
        if 2*overlap_window > len(reunion) > overlap_window:
            return reunion + [record for record in b[:overlap_window] if record not in reunion]
        return reunion + b[overlap_window:]

    @staticmethod
    def stop_all_request_threads(futures):
        for future in futures:
            future.cancel()

        for future in futures:
            while not future.done():  # Both finished and cancelled futures return 'done'
                time.sleep(0.1)

    @backoff.on_exception(backoff.expo, RuntimeError)
    def _all_fetch_batch_steps(self):
        # prepare batches (with self.limit for each of them until we reach batch_limit)
        self.prepare_batch_params()
        # send batches to multithreaded_requests_pool
        futures = list()
        for offset in [offset for offset in range(0, 20000, self.limit)]:
            self.offset = offset
            self.prepare_batch()
            futures.append(MultithreadedRequestsPool.queue_request(self.client, self.stream_name,
                                                                   self.endpoint_path, self.endpoint_api_method,
                                                                   self.endpoint_api_version,
                                                                   self.endpoint_api_key_type,
                                                                   deepcopy(self.endpoint_body),
                                                                   deepcopy(self.params)))
        # wait for responses, and check them for errors
        final_buffer = list()
        stop_iteration = False
        for future in futures:
            while not future.done():
                time.sleep(0.1)

            temp_buffer = future.result()
            if not final_buffer:
                final_buffer += temp_buffer
                continue

            if not temp_buffer:  # We finished the data to extract, time to stop
                self.stop_all_request_threads(futures)
                stop_iteration = True

            try:
                final_buffer += self.error_check_and_fix(final_buffer, temp_buffer)
            except RuntimeError:  # if errors are found
                LOGGER.exception("Discrepancies found in extracted data, and errors couldn't be corrected."
                                 "Cleaning up...")

                # wait all threads to finish/cancel all threads
                self.stop_all_request_threads(futures)
                LOGGER.info("Cleanup complete! Retrying extraction from last bookmark...")
                # retry the whole process (using backoff decorator, so we need to propagate the exception)
                # effectively rerunning this function with the same parameters
                raise

            if stop_iteration:
                break

        # if no errors found:
        # dump data into buffer
        for record in final_buffer:
            self.buffer.append(record)
            record_bookmark_value = record.get(self.endpoint_bookmark_field)
            # increment bookmark
            if record_bookmark_value is not None and record_bookmark_value > self.endpoint_intermediary_bookmark_value:
                self.endpoint_intermediary_bookmark_value = record_bookmark_value
        self.last_batch_size = len(final_buffer)

    #     self.prepare_batch()
    #     raw_batch = self.fetch_batch()
    #     self.buffer = transform_json(raw_batch, self.stream_name)
    #     if not self.buffer:
    #         LOGGER.warning(f'(generator) Stream {self.stream_name} - NO TRANSFORMED DATA RESULTS')
    #     self.last_batch_size = len(self.buffer)

    def __iter__(self):
        self._all_fetch_batch_steps()
        return self

    def next(self):
        if not self.buffer and not self.end_of_file:
            with PerformanceMetrics(metric_name="processor_wait"):
                while not self.buffer and not self.end_of_file:
                    time.sleep(0.01)
        if not self.buffer and self.end_of_file:
            raise StopIteration()
        return self.buffer.pop(0)

    def prepare_batch(self):
        self.params = {
            "offset": self.offset,
            "limit": self.limit,
            **self.static_params
        }

    def fetch_batch(self):
        raise NotImplementedError()


# def check_and_correct_errors(a, b, overlap_window):
#     error_set = set(a[-overlap_window:]).union(set(b[:overlap_window]))
#     if len(error_set) > 2*overlap_window:
#         raise Exception("you're fucked")
#     elif overlap_window < len(error_set) < 2*overlap_window:
#         b = list(set(b).union(error_set))
#     return b
