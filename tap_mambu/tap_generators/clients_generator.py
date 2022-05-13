from .generator import TapGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_utc_str


class ClientsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(ClientsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "clients:search"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_sorting_criteria = {
            "field": "lastModifiedDate",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": datetime_to_utc_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))[:10]
            }
        ]
