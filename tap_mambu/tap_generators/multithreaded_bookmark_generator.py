import datetime
import time
import backoff

from copy import deepcopy
from singer import get_logger
from datetime import datetime, timedelta

from .multithreaded_offset_generator import MultithreadedOffsetGenerator
from ..helpers import transform_json, convert
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_tz
from ..helpers.multithreaded_requests import MultithreadedRequestsPool

LOGGER = get_logger()


class MultithreadedBookmarkGenerator(MultithreadedOffsetGenerator):
    def _init_endpoint_config(self):
        super(MultithreadedBookmarkGenerator, self)._init_endpoint_config()
        self.endpoint_intermediary_bookmark_value = None
        self.endpoint_intermediary_bookmark_offset = 0

    def prepare_batch_params(self):
        self.offset = self.endpoint_intermediary_bookmark_offset
        # This function should be extended upon in case you wish to change the date to a new one,
        # in order to paginate through the data using date, resetting offset to 0, or the specific number of records
        # the next batch needs to skip if there are multiple records with the same date.

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

    def queue_batches(self):
        # prepare batches (with self.limit for each of them until we reach batch_limit)
        futures = list()
        original_offset = self.offset
        for offset in range(0, self.batch_limit, self.artificial_limit):
            self.offset = original_offset + offset
            self.prepare_batch()
            # send batches to multithreaded_requests_pool
            futures.append(MultithreadedRequestsPool.queue_request(self.client, self.stream_name,
                                                                   self.endpoint_path, self.endpoint_api_method,
                                                                   self.endpoint_api_version,
                                                                   self.endpoint_api_key_type,
                                                                   deepcopy(self.endpoint_body),
                                                                   deepcopy(self.params)))
        return futures

    @backoff.on_exception(backoff.expo, RuntimeError, max_tries=5)
    def _all_fetch_batch_steps(self):
        if self.date_windowing:
            start = datetime.strptime(self.params["from"], '%Y-%m-%d').date()
            end = datetime.strptime(self.params["to"], '%Y-%m-%d').date()
            temp = start + timedelta(days=self.date_window_size)
            stop_iteration = True
            while temp < end:
                if stop_iteration:
                    self.offset = 0
                self.static_params["from"] = datetime.strftime(start, '%Y-%m-%d')
                self.static_params["to"] = datetime.strftime(temp, '%Y-%m-%d')
                final_buffer, stop_iteration = self.collect_batches(self.queue_batches())
                self.preprocess_batches(final_buffer)
                if not final_buffer or stop_iteration:
                    start = temp
                    temp = start + timedelta(days=self.date_window_size)
            self.offset = 0
            self.static_params["from"] = datetime.strftime(start, '%Y-%m-%d')
            self.static_params["to"] = datetime.strftime(end, '%Y-%m-%d')
        final_buffer, stop_iteration = self.collect_batches(self.queue_batches())
        self.preprocess_batches(final_buffer)
        if not final_buffer or stop_iteration:
            return False
        return True

    def collect_batches(self, futures):
        # wait for responses, and check them for errors
        final_buffer = set()
        stop_iteration = False
        for future in futures:
            while not future.done():
                time.sleep(0.1)
            result = future.result()
            transformed_batch = self.transform_batch(transform_json(result, self.stream_name))
            temp_buffer = set(transformed_batch)

            if not final_buffer:
                final_buffer = final_buffer | temp_buffer
                continue

            if not temp_buffer:  # We finished the data to extract, time to stop
                self.stop_all_request_threads(futures)
                stop_iteration = True
                break

            final_buffer = self.error_check_and_fix(final_buffer, temp_buffer, futures)

            if stop_iteration:
                break
        return final_buffer, stop_iteration

    def preprocess_record(self, raw_record):
        record = super().preprocess_record(raw_record)
        # increment bookmark
        record_bookmark_value = record.get(convert(self.endpoint_bookmark_field))
        if record_bookmark_value is not None:
            self.set_intermediary_bookmark(datetime_to_tz(str_to_localized_datetime(record_bookmark_value), "UTC"))
        return record

    def preprocess_batches(self, final_buffer):
        super().preprocess_batches(final_buffer)
        self.last_batch_size = len(final_buffer)

    def set_intermediary_bookmark(self, record_bookmark_value):
        if self.endpoint_intermediary_bookmark_value is None or \
                self.compare_bookmark_values(record_bookmark_value,
                                             self.endpoint_intermediary_bookmark_value):
            self.endpoint_intermediary_bookmark_value = record_bookmark_value
            self.endpoint_intermediary_bookmark_offset = 1
            return

        if record_bookmark_value == self.endpoint_intermediary_bookmark_value:
            self.endpoint_intermediary_bookmark_offset += 1
            return

    def compare_bookmark_values(self, a, b):
        return a > b


class MultithreadedBookmarkDayByDayGenerator(MultithreadedBookmarkGenerator):
    def set_intermediary_bookmark(self, record_bookmark_value: datetime):
        record_bookmark_value_without_time = record_bookmark_value.replace(hour=0, minute=0, second=0, microsecond=0)
        super().set_intermediary_bookmark(record_bookmark_value_without_time)
