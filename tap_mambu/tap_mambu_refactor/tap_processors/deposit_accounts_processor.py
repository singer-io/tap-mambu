from .parent_processor import ParentProcessor


class DepositAccountsProcessor(ParentProcessor):
    def _init_endpoint_config(self):
        super(DepositAccountsProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
        self.endpoint_child_streams = ["cards"]
