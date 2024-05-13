from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str


class LoanTransactionsGenerator(MultithreadedBookmarkGenerator):
    def _init_endpoint_config(self):
        super(LoanTransactionsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "loans/transactions:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria["field"] = "id"

    def prepare_batch_params(self):
        super(LoanTransactionsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)
