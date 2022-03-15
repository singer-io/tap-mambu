from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class DepositTransactionsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(DepositTransactionsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "deposits/transactions:search"
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_sorting_criteria = {
            "field": "creationDate",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = [
            {
                "field": "creationDate",
                "operator": "AFTER",
                "value": transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
            }
        ]
