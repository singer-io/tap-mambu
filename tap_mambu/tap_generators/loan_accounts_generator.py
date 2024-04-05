from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str


class LoanAccountsLMGenerator(MultithreadedBookmarkGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(LoanAccountsLMGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.max_threads = 5

    def _init_endpoint_config(self):
        super(LoanAccountsLMGenerator, self)._init_endpoint_config()
        self.endpoint_path = "loans:search"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_sorting_criteria = {
            "field": "id",
            "order": "ASC"
        }

    def prepare_batch_params(self):
        super(LoanAccountsLMGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)


class LoanAccountsADGenerator(LoanAccountsLMGenerator):
    def _init_endpoint_config(self):
        super(LoanAccountsADGenerator, self)._init_endpoint_config()
        self.endpoint_bookmark_field = "lastAccountAppraisalDate"
