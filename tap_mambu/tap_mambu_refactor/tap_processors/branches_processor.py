from .parent_processor import TapProcessor


class BranchesProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(BranchesProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
