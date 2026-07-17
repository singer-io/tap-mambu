from .processor import TapProcessor


class ChildProcessor(TapProcessor):
    def __init__(self, catalog, stream_name, client, config, state, sub_type, generators,
                 parent_id, parent_replication_values=None):
        super().__init__(catalog, stream_name, client, config, state, sub_type, generators)
        self.endpoint_parent_id = parent_id
        self.parent_replication_values = parent_replication_values or {}

    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_parent = 'parent'

    def process_record(self, record, time_extracted, bookmark_field):
        record[f'{self.endpoint_parent}_id'] = self.endpoint_parent_id
        for replication_key, value in self.parent_replication_values.items():
            if value is not None and replication_key not in record:
                record[replication_key] = value
        return super().process_record(record, time_extracted, bookmark_field)
