from singer import utils

from .generator import TapGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_utc_str


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
                "value": datetime_to_utc_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))[:10],
                "secondValue": utils.now().strftime("%Y-%m-%d")[:10]
            }
        ]
