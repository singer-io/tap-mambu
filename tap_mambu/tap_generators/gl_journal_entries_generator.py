from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_utc_str


class GlJournalEntriesGenerator(MultithreadedBookmarkGenerator):
    def _init_endpoint_config(self):
        super(GlJournalEntriesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "gljournalentries:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria = {
            "field": "entryId",
            "order": "ASC"
        }

    def prepare_batch_params(self):
        super(GlJournalEntriesGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)
