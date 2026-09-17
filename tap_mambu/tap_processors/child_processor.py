from .processor import TapProcessor


class ChildProcessor(TapProcessor):
    def __init__(self, catalog, stream_name, client, config, state, sub_type, generators,
                 parent_id, parent_replication_value=None):
        super().__init__(catalog, stream_name, client, config, state, sub_type, generators)
        self.endpoint_parent_id = parent_id
        self.parent_replication_value = parent_replication_value
        self.parent_replication_field = f'{self.endpoint_parent}_last_modified_date'
        for generator in self.generators:
            generator.endpoint_bookmark_field = self.parent_replication_field

    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_parent = 'parent'

    def process_record(self, record, time_extracted, bookmark_field):
        record[f'{self.endpoint_parent}_id'] = self.endpoint_parent_id
        record[self.parent_replication_field] = self.parent_replication_value
        return super().process_record(record, time_extracted, bookmark_field)

    def write_bookmark(self):
        pass
