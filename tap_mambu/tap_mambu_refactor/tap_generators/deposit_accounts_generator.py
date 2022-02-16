from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class DepositAccountsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(DepositAccountsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "deposits:search"
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
        self.endpoint_bookmark_field = "lastModifiedDate"
