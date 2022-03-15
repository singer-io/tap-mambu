from .processor import TapProcessor


class LoanTransactionsProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(LoanTransactionsProcessor, self)._init_endpoint_config()
        self.endpoint_id_field = "encoded_key"
