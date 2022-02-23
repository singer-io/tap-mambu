from .parent_processor import ParentProcessor


class ClientsProcessor(ParentProcessor):
    def _init_endpoint_config(self):
        super(ClientsProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
