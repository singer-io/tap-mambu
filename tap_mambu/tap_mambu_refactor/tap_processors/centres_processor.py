from .parent_processor import ParentProcessor


class CentresProcessor(ParentProcessor):
    def _init_endpoint_config(self):
        super(CentresProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
