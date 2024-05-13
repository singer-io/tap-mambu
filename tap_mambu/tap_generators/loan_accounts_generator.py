from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_datetime
from ..helpers import get_bookmark, write_bookmark


class LoanAccountsLMGenerator(MultithreadedBookmarkGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(LoanAccountsLMGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.max_threads = 3
        self.sub_stream_name = "loan_accounts_lmg"

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
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(
            self.endpoint_intermediary_bookmark_value)

    def write_sub_stream_bookmark(self, start):
        write_bookmark(self.state, self.sub_stream_name, self.sub_type, start)

    def get_default_start_value(self):
        # Historical sync will use start date as as date window
        # Increamental syncs will use last stream bookmark value
        # Interrupted syncs will use last winodow of sub-stream as first date window
        stream_bookmark = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)
        sub_stream_bookmark = get_bookmark(self.state, self.sub_stream_name, self.sub_type, stream_bookmark)
        if self.compare_bookmark_values(sub_stream_bookmark, stream_bookmark):
            start_value = stream_bookmark
        else:
            start_value = sub_stream_bookmark
        truncated_start_date = datetime_to_utc_str(str_to_datetime(start_value).replace(hour=0, minute=0, second=0))
        return truncated_start_date

    def remove_sub_stream_bookmark(self):
        # Remove sub-stream bookmark once we finish extraction till current date
        if self.sub_stream_name in self.state.get("bookmarks", {}):
            del self.state["bookmarks"][self.sub_stream_name]

    def set_intermediary_bookmark(self, record_bookmark_value):
        if self.endpoint_intermediary_bookmark_value is None or \
                self.compare_bookmark_values(record_bookmark_value,
                                             self.endpoint_intermediary_bookmark_value):
            self.endpoint_intermediary_bookmark_offset = 1
            return

        if record_bookmark_value == self.endpoint_intermediary_bookmark_value:
            self.endpoint_intermediary_bookmark_offset += 1
            return


class LoanAccountsADGenerator(LoanAccountsLMGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(LoanAccountsADGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.sub_stream_name = "loan_accounts_adg"

    def _init_endpoint_config(self):
        super(LoanAccountsADGenerator, self)._init_endpoint_config()
        self.endpoint_bookmark_field = "lastAccountAppraisalDate"
