import time
from copy import deepcopy
from threading import Thread

import backoff
from singer import get_logger

from .generator import TapGenerator
from ..helpers import transform_json
from ..helpers.multithreaded_requests import MultithreadedRequestsPool
from ..helpers.perf_metrics import PerformanceMetrics

LOGGER = get_logger()


class MultithreadedOffsetGenerator(TapGenerator):
    def _init_params(self):
        self.time_extracted = None
        self.static_params = dict(self.endpoint_params)
        self.offset = 0
        self.overlap_window = 20
        self.artificial_limit = self.client.page_size
        self.limit = self.client.page_size + self.overlap_window
        self.batch_limit = self.max_threads * self.client.page_size + self.overlap_window
        self.params = self.static_params

    def _init_config(self):
        super(MultithreadedOffsetGenerator, self)._init_config()
        self.end_of_file = False
        self.fetch_batch_thread = None
        self.last_batch_set = set()
        self.max_threads = 20

    @staticmethod
    def check_and_get_set_reunion(a: set, b: set, lower_limit: int):
        reunion = a | b
        if len(reunion) == len(a) + len(b):
            # Raise a runtime error to be caught at the top level function _all_fetch_batch_steps,
            # in order to retry extraction or stop trying after a few retries
            raise RuntimeError("Retrying extraction for last multithreaded batches as a discrepancy has been detected "
                               "between two consecutive batches. (Records have shifted due to insertions/changes/deletions)")
        if len(a) + lower_limit < len(reunion) < len(a) + len(b):
            LOGGER.warning("Discrepancies detected for last multithreaded batches extraction, but they are correctable."
                           " (Records have shifted due to insertions/changes/deletions)")
        return reunion

    @staticmethod
    def stop_all_request_threads(futures):
        for future in futures:
            future.cancel()

        for future in futures:
            while not future.done():  # Both finished and cancelled futures return 'done'
                time.sleep(0.1)

    def fetch_batch_continuously(self):
        while not self.end_of_file:
            if not self._all_fetch_batch_steps():
                self.end_of_file = True
            time.sleep(0.1)

    def queue_batches(self):
        # prepare batches (with self.limit for each of them until we reach batch_limit)
        futures = list()
        while len(self.buffer) + len(futures) * self.limit <= self.batch_limit:
            self.prepare_batch()
            # send batches to multithreaded_requests_pool
            futures.append(MultithreadedRequestsPool.queue_request(self.client, self.stream_name,
                                                                   self.endpoint_path, self.endpoint_api_method,
                                                                   self.endpoint_api_version,
                                                                   self.endpoint_api_key_type,
                                                                   deepcopy(self.endpoint_body),
                                                                   deepcopy(self.params)))
            self.offset += self.artificial_limit
        return futures

    def collect_batches(self, futures):
        # wait for responses, and check them for errors
        last_batch = set()
        final_buffer = self.last_batch_set
        stop_iteration = False
        for future in futures:
            while not future.done():
                time.sleep(0.1)
            result = future.result()
            transformed_batch = self.transform_batch(transform_json(result, self.stream_name))
            temp_buffer = set(transformed_batch)

            if not temp_buffer:  # We finished the data to extract, time to stop
                self.stop_all_request_threads(futures)
                stop_iteration = True
                break

            last_batch = temp_buffer

            if not final_buffer:
                final_buffer = final_buffer | temp_buffer
                continue

            final_buffer = self.error_check_and_fix(final_buffer, temp_buffer, futures)

            if stop_iteration:
                break

        final_buffer -= self.last_batch_set
        self.last_batch_set = last_batch

        return final_buffer, stop_iteration

    def preprocess_record(self, raw_record):
        self.buffer.append(raw_record)
        return raw_record

    def preprocess_batches(self, final_buffer):
        # if no errors found:
        # dump data into buffer
        for raw_record in final_buffer:
            self.preprocess_record(raw_record)
        self.last_batch_size = len(self.last_batch_set)

    @backoff.on_exception(backoff.expo, RuntimeError, max_tries=5)
    def _all_fetch_batch_steps(self):
        futures = self.queue_batches()
        final_buffer, stop_iteration = self.collect_batches(futures)
        self.preprocess_batches(final_buffer)
        if not final_buffer or stop_iteration:
            return False
        return True

    def error_check_and_fix(self, final_buffer: set, temp_buffer: set, futures: list):
        try:
            final_buffer = self.check_and_get_set_reunion(final_buffer, temp_buffer, self.artificial_limit)
        except RuntimeError:  # if errors are found
            LOGGER.exception("Discrepancies found in extracted data, and errors couldn't be corrected."
                             "Shutting down all remaining threads for this batch's extraction to retry it.")

            # wait all threads to finish/cancel all threads
            self.stop_all_request_threads(futures)
            LOGGER.info("Thread shutdown complete! Retrying extraction from last bookmark...")
            # retry the whole process (using backoff decorator, so we need to propagate the exception)
            # effectively rerunning this function with the same parameters
            raise
        return final_buffer

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
