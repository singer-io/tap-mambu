from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime


class DepositAccountsGenerator(MultithreadedBookmarkGenerator):
    def _init_config(self):
        super()._init_config()
        self.max_threads = 25

    def _init_endpoint_config(self):
        super(DepositAccountsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "deposits:search"
        self.endpoint_sorting_criteria = {
            "field": "lastModifiedDate",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": datetime_to_utc_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))
            }
        ]
        self.endpoint_bookmark_field = "lastModifiedDate"

    def prepare_batch_params(self):
        super(DepositAccountsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)
