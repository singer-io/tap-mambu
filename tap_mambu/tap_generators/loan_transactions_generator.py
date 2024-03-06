from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str
from datetime import datetime


class LoanTransactionsGenerator(MultithreadedBookmarkGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(LoanTransactionsGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.date_windowing = True

    def _init_endpoint_config(self):
        super(LoanTransactionsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "loans/transactions:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria["field"] = "id"

    def modify_request_params(self, start, end):
        self.endpoint_body['filterCriteria'] = [
            {
                "field": "creationDate",
                "operator": "AFTER",
                "value": datetime.strftime(start, '%Y-%m-%dT00:00:00.000000Z')
            },
            {
                "field": "creationDate",
                "operator": "BEFORE",
                "value": datetime.strftime(end, '%Y-%m-%dT23:59:59.000000Z')
            }
        ]

    def prepare_batch_params(self):
        super(LoanTransactionsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)
