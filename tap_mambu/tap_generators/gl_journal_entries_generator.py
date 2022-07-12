from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime, utc_now


class GlJournalEntriesGenerator(MultithreadedBookmarkGenerator):
    def _init_config(self):
        super()._init_config()
        self.max_threads = 5

    def _init_endpoint_config(self):
        super(GlJournalEntriesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "gljournalentries:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria = {
            "field": "entryId",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = [
            {
                "field": "creationDate",
                "operator": "BETWEEN",
                "value": datetime_to_utc_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))),
                "secondValue": datetime_to_utc_str(utc_now())
            }
        ]

    def prepare_batch_params(self):
        super(GlJournalEntriesGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)
