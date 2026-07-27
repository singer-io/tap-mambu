from .processor import TapProcessor


class ChildProcessor(TapProcessor):
    def __init__(self, catalog, stream_name, client, config, state, sub_type, generators, parent_id):
        super().__init__(catalog, stream_name, client, config, state, sub_type, generators)
        self.endpoint_parent_id = parent_id

    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_parent = 'parent'

    def process_record(self, record, time_extracted, bookmark_field):
        record[f'{self.endpoint_parent}_id'] = self.endpoint_parent_id
        return super().process_record(record, time_extracted, bookmark_field)
