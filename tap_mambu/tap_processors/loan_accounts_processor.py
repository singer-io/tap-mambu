from .deduplication_processor import DeduplicationProcessor
from .multithreaded_parent_processor import MultithreadedParentProcessor


class LoanAccountsProcessor(MultithreadedParentProcessor, DeduplicationProcessor):
    def _init_endpoint_config(self):
        super(LoanAccountsProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
        self.endpoint_child_streams = ["loan_repayments"]
