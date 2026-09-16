from concurrent import futures

from .processor import TapProcessor, LOGGER
from ..helpers import get_bookmark, get_selected_streams, write_bookmark
from ..helpers.datetime_utils import str_to_datetime
from ..helpers.multithreaded_requests import MultithreadedRequestsPool


class MultithreadedParentProcessor(TapProcessor):
    def _init_config(self):
        super(MultithreadedParentProcessor, self)._init_config()
        self.futures = list()
        self.child_bookmark_values = dict()

    def _init_endpoint_config(self):
        super(MultithreadedParentProcessor, self)._init_endpoint_config()
        self.child_bookmark_values = dict()

    def process_records(self):
        for child_stream_name in self.endpoint_child_streams:
            self.child_bookmark_values.setdefault(
                child_stream_name,
                get_bookmark(self.state, child_stream_name, self.sub_type, None))

        record_count = super(MultithreadedParentProcessor, self).process_records()

        for future in futures.as_completed(self.futures):
            record_count += future.result()

        for child_stream_name in self.endpoint_child_streams:
            if child_stream_name in get_selected_streams(self.catalog):
                child_bookmark = self.child_bookmark_values.get(child_stream_name)
                if child_bookmark is None:
                    continue
                write_bookmark(self.state, child_stream_name, self.sub_type,
                               child_bookmark)

        for generator in self.generators:
            generator.set_last_sync_completed(self.generators[0].start_windows_datetime_str)
            generator.remove_sub_stream_bookmark()
        return record_count

    def _process_child_records(self, record):
        from ..sync import sync_endpoint

        super(MultithreadedParentProcessor, self)._process_child_records(record)
        for child_stream_name in self.endpoint_child_streams:
            if child_stream_name in get_selected_streams(self.catalog):
                parent_id = record[self.endpoint_id_field]
                parent_bookmark = record.get('last_modified_date')
                if parent_bookmark:
                    current_bookmark = self.child_bookmark_values.get(child_stream_name)
                    if current_bookmark is None or str_to_datetime(parent_bookmark) > \
                            str_to_datetime(current_bookmark):
                        self.child_bookmark_values[child_stream_name] = parent_bookmark
                LOGGER.info(f'(processor) Syncing: {child_stream_name}, '
                            f'parent_stream: {self.stream_name}, '
                            f'parent_id: {parent_id}')

                future = MultithreadedRequestsPool.queue_function(
                    sync_endpoint,
                    client=self.client,
                    catalog=self.catalog,
                    state=self.state,
                    stream_name=child_stream_name,
                    sub_type=self.sub_type,
                    config=self.config,
                    parent_id=parent_id,
                    parent_replication_value=record.get('last_modified_date'))
                self.futures.append(future)
