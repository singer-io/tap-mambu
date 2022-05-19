import json
import time
from copy import deepcopy
from threading import Thread

import backoff
from singer import get_logger

from .generator import TapGenerator
from ..helpers import transform_json, convert
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime
from ..helpers.multithreaded_requests import MultithreadedRequestsPool
from ..helpers.perf_metrics import PerformanceMetrics

LOGGER = get_logger()


class MultithreadedBookmarkGenerator(TapGenerator):
    def _init_params(self):
        self.time_extracted = None
        self.static_params = dict(self.endpoint_params)
        self.offset = 0
        self.overlap_window = 20
        self.artificial_limit = self.client.page_size
        self.limit = self.client.page_size + self.overlap_window
        self.batch_limit = 2000
        self.params = self.static_params

    def _init_config(self):
        super(MultithreadedBookmarkGenerator, self)._init_config()
        self.end_of_file = False
        self.fetch_batch_thread = None

    def _init_endpoint_config(self):
        super(MultithreadedBookmarkGenerator, self)._init_endpoint_config()
        self.endpoint_intermediary_bookmark_value = None
        self.endpoint_intermediary_bookmark_offset = 0

    def prepare_batch_params(self):
        self.offset = self.endpoint_intermediary_bookmark_offset
        # here we change the date to the new one,
        # in order to paginate through the data using date, resetting offset to 0

    def error_check_and_fix(self, a, b):
        reunion = a | b
        if len(reunion) == len(a) + len(b):
            raise RuntimeError("Failed to error correct, aborting job.")
        if len(a) + self.artificial_limit < len(reunion) < len(a) + len(b):
            LOGGER.warning("Error checking returned errors, but they will be corrected!")
        return reunion

    @staticmethod
    def stop_all_request_threads(futures):
        for future in futures:
            future.cancel()

        for future in futures:
            while not future.done():  # Both finished and cancelled futures return 'done'
                time.sleep(0.1)

    def fetch_batch_continuously(self):
        first_run = True
        while not self.end_of_file:
            while len(self.buffer) > self.batch_limit:
                time.sleep(0.1)
            if not first_run:
                self.prepare_batch_params()
            if not self._all_fetch_batch_steps():
                self.end_of_file = True
            first_run = False

    @backoff.on_exception(backoff.expo, RuntimeError, max_tries=5)
    def _all_fetch_batch_steps(self):
        # prepare batches (with self.limit for each of them until we reach batch_limit)
        futures = list()
        original_offset = self.offset
        for offset in [offset for offset in range(0, self.batch_limit, self.artificial_limit)]:
            self.offset = original_offset + offset
            self.prepare_batch()
            # send batches to multithreaded_requests_pool
            futures.append(MultithreadedRequestsPool.queue_request(self.client, self.stream_name,
                                                                   self.endpoint_path, self.endpoint_api_method,
                                                                   self.endpoint_api_version,
                                                                   self.endpoint_api_key_type,
                                                                   deepcopy(self.endpoint_body),
                                                                   deepcopy(self.params)))
        # wait for responses, and check them for errors
        final_buffer = set()
        stop_iteration = False
        for future in futures:
            while not future.done():
                time.sleep(0.1)

            temp_buffer = set([json.dumps(record, ensure_ascii=False).encode("utf8") for record in
                               self.transform_batch(transform_json(future.result(), self.stream_name))])
            if not final_buffer:
                final_buffer = final_buffer | temp_buffer
                continue

            if not temp_buffer:  # We finished the data to extract, time to stop
                self.stop_all_request_threads(futures)
                stop_iteration = True
                break

            try:
                final_buffer = self.error_check_and_fix(final_buffer, temp_buffer)
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
        for raw_record in final_buffer:
            record = json.loads(raw_record.decode("utf8"))
            self.buffer.append(record)
            # increment bookmark
            self.set_intermediary_bookmark(record)

        self.last_batch_size = len(final_buffer)
        if not final_buffer or stop_iteration:
            return False
        return True

    def set_intermediary_bookmark(self, record):
        record_bookmark_value = record.get(convert(self.endpoint_bookmark_field))
        if record_bookmark_value is None:
            return

        record_bookmark_value = str_to_localized_datetime(record_bookmark_value)

        if self.endpoint_intermediary_bookmark_value is None or \
                self.compare_bookmark_values(datetime_to_utc_str(record_bookmark_value),
                                             self.endpoint_intermediary_bookmark_value):
            self.endpoint_intermediary_bookmark_value = datetime_to_utc_str(record_bookmark_value)
            self.endpoint_intermediary_bookmark_offset = 1
            return

        if datetime_to_utc_str(record_bookmark_value)[:10] == self.endpoint_intermediary_bookmark_value:
            self.endpoint_intermediary_bookmark_offset += 1
            return

    def compare_bookmark_values(self, a, b):
        return a > b

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


class MultithreadedBookmarkDayByDayGenerator(MultithreadedBookmarkGenerator):
    def set_intermediary_bookmark(self, record):
        record_bookmark_value = str_to_localized_datetime(record.get(convert(self.endpoint_bookmark_field)))
        if record_bookmark_value is None:
            return

        if self.endpoint_intermediary_bookmark_value is None or \
                self.compare_bookmark_values(datetime_to_utc_str(record_bookmark_value)[:10],
                                             self.endpoint_intermediary_bookmark_value):
            self.endpoint_intermediary_bookmark_value = datetime_to_utc_str(record_bookmark_value)[:10]
            self.endpoint_intermediary_bookmark_offset = 1
            return

        if datetime_to_utc_str(record_bookmark_value)[:10] == self.endpoint_intermediary_bookmark_value:
            self.endpoint_intermediary_bookmark_offset += 1
            return
