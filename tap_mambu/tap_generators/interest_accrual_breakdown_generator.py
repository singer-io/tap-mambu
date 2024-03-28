from .multithreaded_bookmark_generator import MultithreadedBookmarkDayByDayGenerator
from ..helpers.datetime_utils import datetime_to_utc_str


class InterestAccrualBreakdownGenerator(MultithreadedBookmarkDayByDayGenerator):
    def _init_endpoint_config(self):
        super(InterestAccrualBreakdownGenerator, self)._init_endpoint_config()
        self.endpoint_path = "accounting/interestaccrual:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria = {
            "field": "entryId",
            "order": "ASC"
        }

    def prepare_batch_params(self):
        super(InterestAccrualBreakdownGenerator, self).prepare_batch_params()
        # look in db for the reason DateTimes don't work, but Dates do
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)[:10]
