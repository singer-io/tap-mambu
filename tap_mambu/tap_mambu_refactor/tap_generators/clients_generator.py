from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


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
                "value": transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
            }
        ]
