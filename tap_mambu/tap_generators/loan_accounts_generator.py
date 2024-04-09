from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str


class LoanAccountsLMGenerator(MultithreadedBookmarkGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(LoanAccountsLMGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.max_threads = 3

    def _init_endpoint_config(self):
        super(LoanAccountsLMGenerator, self)._init_endpoint_config()
        self.endpoint_path = "loans:search"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_sorting_criteria = {
            "field": "lastModifiedDate",
            "order": "ASC"
        }

    def prepare_batch_params(self):
        super(LoanAccountsLMGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(
            self.endpoint_intermediary_bookmark_value)

    def set_last_sync_window_start(self, start):
        self.state["last_sync_windows_start_lmg"] = start
        self.state_changed = True

    def get_last_sync_window_start(self):
        return self.state.get("last_sync_windows_start_lmg")

    def remove_last_sync_window_start(self):
        if "last_sync_windows_start_ad" in self.state:
            del self.state["last_sync_windows_start_lmg"]


class LoanAccountsADGenerator(LoanAccountsLMGenerator):
    def _init_endpoint_config(self):
        super(LoanAccountsADGenerator, self)._init_endpoint_config()
        self.endpoint_bookmark_field = "lastAccountAppraisalDate"

    def set_last_sync_window_start(self, start):
        self.state["last_sync_windows_start_ad"] = start
        self.state_changed = True

    def get_last_sync_window_start(self):
        return self.state.get("last_sync_windows_start_ad")

    def remove_last_sync_window_start(self):
        if "last_sync_windows_start_ad" in self.state:
            del self.state["last_sync_windows_start_ad"]
