from .parent_processor import ParentProcessor


class LoanAccountsProcessor(ParentProcessor):
    def _init_endpoint_config(self):
        super(LoanAccountsProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
        self.endpoint_child_streams = ["loan_repayments"]
