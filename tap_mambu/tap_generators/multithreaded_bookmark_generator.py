import time
from copy import deepcopy
from threading import Thread
from typing import List

import backoff
from singer import utils, get_logger

from .generator import TapGenerator
from ..helpers import transform_json
from ..helpers.multithreaded_requests import MultithreadedRequestsPool
from ..helpers.perf_metrics import PerformanceMetrics

LOGGER = get_logger()


class MultithreadedBookmarkGenerator(TapGenerator):
    def _init_params(self):
        self.time_extracted = None
        self.static_params = dict(self.endpoint_params)
        self.offset = 0
        self.limit = self.client.page_size
        self.batch_limit = 20000
        self.params = self.static_params

    def _init_config(self):
        super(MultithreadedBookmarkGenerator, self)._init_config()
        self.end_of_file = False
        self.fetch_batch_thread = None

    def _init_endpoint_config(self):
        super(MultithreadedBookmarkGenerator, self)._init_endpoint_config()
        self.endpoint_intermediary_bookmark_value = None

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

    def fetch_batch_continuously(self):
        while not self.end_of_file:
            while len(self.buffer) > 20000:
                time.sleep(0.1)
            self.prepare_batch_params()
            if not self._all_fetch_batch_steps():
                self.end_of_file = True
            self.end_of_file = True

    @backoff.on_exception(backoff.expo, RuntimeError)
    def _all_fetch_batch_steps(self):
        # prepare batches (with self.limit for each of them until we reach batch_limit)
        futures = list()
        for offset in [offset for offset in range(0, 20000, self.limit)]:
            self.offset = offset
            self.prepare_batch()
            # send batches to multithreaded_requests_pool
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

            temp_buffer = self._transform_batch(future.result())
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
            if record_bookmark_value is not None:
                if self.endpoint_intermediary_bookmark_value is None or \
                        record_bookmark_value > self.endpoint_intermediary_bookmark_value:
                    self.endpoint_intermediary_bookmark_value = record_bookmark_value
        self.last_batch_size = len(final_buffer)
        if not final_buffer:
            return False
        return True

    def __iter__(self):
        if self.fetch_batch_thread is None:
            self.fetch_batch_thread = Thread(target=self.fetch_batch_continuously, name="FetchContinuouslyThread")
            self.fetch_batch_thread.start()
        return self

    def next(self):
        if not self.buffer and not self.end_of_file:
            with PerformanceMetrics(metric_name="processor_wait"):
                while not self.buffer and not self.end_of_file:
                    time.sleep(0.01)
        if not self.buffer and self.end_of_file:
            raise StopIteration()
        return self.buffer.pop(0)

    def fetch_batch(self):
        raise DeprecationWarning("Function is being deprecated, and not implemented in this subclass!")
