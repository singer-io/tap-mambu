from .child_processor import ChildProcessor


class DepositCardsProcessor(ChildProcessor):
    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_deduplication_key = 'reference_token'
        self.endpoint_id_field = "reference_token"
        self.endpoint_parent = "deposit"
