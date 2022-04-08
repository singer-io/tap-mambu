from .processor import TapProcessor, LOGGER
from ..helpers import get_selected_streams


class ParentProcessor(TapProcessor):
    def _process_child_records(self, record):
        from ..sync import sync_endpoint

        total_records = super()._process_child_records(record)
        for child_stream_name in self.endpoint_child_streams:
            if child_stream_name in get_selected_streams(self.catalog):
                parent_id = record[self.endpoint_id_field]
                LOGGER.info(f'(processor) Syncing: {child_stream_name}, '
                            f'parent_stream: {self.stream_name}, '
                            f'parent_id: {parent_id}')

                child_synced_records = sync_endpoint(client=self.client,
                                                     catalog=self.catalog,
                                                     state=self.state,
                                                     stream_name=child_stream_name,
                                                     sub_type='self',
                                                     config=self.config,
                                                     parent_id=parent_id)
                total_records += child_synced_records
                LOGGER.info(f'(processor) Synced: {child_stream_name}, '
                            f'parent_id: {parent_id}, '
                            f'synced records: {child_synced_records}')
        return total_records
