from concurrent import futures

from .processor import TapProcessor, LOGGER
from ..helpers import get_selected_streams
from ..helpers.multithreaded_requests import MultithreadedRequestsPool


class MultithreadedParentProcessor(TapProcessor):
    def _init_config(self):
        super(MultithreadedParentProcessor, self)._init_config()
        self.futures = list()

    def process_records(self):
        record_count = super(MultithreadedParentProcessor, self).process_records()

        for future in futures.as_completed(self.futures):
            record_count += future.result()
        return record_count

    def _process_child_records(self, record):
        from ..sync import sync_endpoint

        super(MultithreadedParentProcessor, self)._process_child_records(record)
        for child_stream_name in self.endpoint_child_streams:
            if child_stream_name in get_selected_streams(self.catalog):
                parent_id = record[self.endpoint_id_field]
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
                    parent_id=parent_id)
                self.futures.append(future)
