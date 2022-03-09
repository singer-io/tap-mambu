from .processor import TapProcessor


class DepositTransactionsProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(DepositTransactionsProcessor, self)._init_endpoint_config()
