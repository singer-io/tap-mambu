from .parent_processor import TapProcessor


class CentresProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(CentresProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
