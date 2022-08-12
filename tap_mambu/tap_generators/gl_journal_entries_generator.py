from singer import utils

from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import transform_datetime, get_bookmark


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
                "value": transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)),
                "secondValue": utils.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
        ]

    def prepare_batch_params(self):
        super(GlJournalEntriesGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = self.endpoint_intermediary_bookmark_value
