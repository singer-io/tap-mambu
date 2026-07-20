from concurrent import futures

from .processor import TapProcessor, LOGGER
from ..helpers import get_selected_streams
from ..helpers.multithreaded_requests import MultithreadedRequestsPool
from ..helpers.schema import STREAMS
from ..helpers import convert


def _snake_to_camel(name):
    parts = name.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class MultithreadedParentProcessor(TapProcessor):
    def _init_config(self):
        super(MultithreadedParentProcessor, self)._init_config()
        self.futures = list()

    def process_records(self):
        record_count = super(MultithreadedParentProcessor, self).process_records()

        for future in futures.as_completed(self.futures):
            record_count += future.result()

        for generator in self.generators:
            # Do NOT call set_last_sync_completed here — it would write a wall-clock window
            # boundary (start_windows_datetime_str) into the bookmark state.  That value is
            # time-varying and would be retained by write_bookmark's max-semantics, causing
            # the second sync to start from a different (or future) date than the first.
            # The record-date-based bookmark written by processor.write_bookmark() is sufficient.
            generator.remove_sub_stream_bookmark()
        return record_count

    def _process_child_records(self, record):
        from ..sync import sync_endpoint

        super(MultithreadedParentProcessor, self)._process_child_records(record)
        parent_replication_values = {}
        for replication_key in STREAMS.get(self.stream_name, {}).get("replication_keys", []):
            candidate_keys = {
                replication_key,
                convert(replication_key),
                _snake_to_camel(replication_key),
            }
            for record_key in candidate_keys:
                if record_key in record:
                    parent_replication_values[replication_key] = record[record_key]
                    break

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
                    parent_id=parent_id,
                    parent_replication_values=parent_replication_values)
                self.futures.append(future)
