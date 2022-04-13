from singer import utils

from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class GlJournalEntriesGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(GlJournalEntriesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "gljournalentries:search"
        self.endpoint_api_method = "POST"
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
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10],
                "secondValue": utils.now().strftime("%Y-%m-%d")[:10]
            }
        ]
