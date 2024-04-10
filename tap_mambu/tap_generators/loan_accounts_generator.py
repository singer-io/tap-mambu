from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str
from ..helpers import get_bookmark, write_bookmark


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
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        # self.state["bookmarks"]["lmg_last_sync_windows_start"] = start
        write_bookmark(self.state, "lmg_last_sync_windows_start", self.sub_type, start)

    def get_last_sync_window_start(self):
        last_bookmark = get_bookmark(
            self.state, self.stream_name, self.sub_type, self.start_date)
        return self.state.get("bookmarks", {}).get("lmg_last_sync_windows_start", last_bookmark)

    def remove_last_sync_window_start(self):
        if "lmg_last_sync_windows_start" in self.state["bookmarks"]:
            del self.state["bookmarks"]["lmg_last_sync_windows_start"]

    def get_default_start_value(self):
        return self.state.get("bookmarks", {}).get("lmg_last_multithread_sync_completed", self.start_date)

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
    def _init_endpoint_config(self):
        super(LoanAccountsADGenerator, self)._init_endpoint_config()
        self.endpoint_bookmark_field = "lastAccountAppraisalDate"

    def set_last_sync_window_start(self, start):
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        write_bookmark(self.state, "ad_last_sync_windows_start", self.sub_type, start)

    def get_last_sync_window_start(self):
        last_bookmark = get_bookmark(
            self.state, self.stream_name, self.sub_type, self.start_date)
        return self.state.get("bookmarks", {}).get("ad_last_sync_windows_start", last_bookmark)

    def remove_last_sync_window_start(self):
        if "ad_last_sync_windows_start" in self.state["bookmarks"]:
            del self.state["bookmarks"]["ad_last_sync_windows_start"]
            self.state_changed = True

    def get_default_start_value(self):
        return self.state.get("bookmarks", {}).get("ad_last_multithread_sync_completed", self.start_date)
