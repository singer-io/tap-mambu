from .generator import TapGenerator
from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_utc_str


class InterestAccrualBreakdownGenerator(MultithreadedBookmarkGenerator):
    def _init_endpoint_config(self):
        super(InterestAccrualBreakdownGenerator, self)._init_endpoint_config()
        self.endpoint_path = "accounting/interestaccrual:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria = {
            "field": "entryId",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = [
            {
                "field": "creationDate",
                "operator": "AFTER",
                "value": datetime_to_utc_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))[:10]
            }
        ]

    def prepare_batch_params(self):
        super(InterestAccrualBreakdownGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = self.endpoint_intermediary_bookmark_value
