from time import strftime
from singer import utils
from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class InstallmentsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(InstallmentsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "installments"
        self.endpoint_api_method = "GET"
        self.endpoint_params = {
            "dueFrom": transform_datetime(self.start_date)[:10],
            "dueTo": utils.now().strftime("%Y-%m-%d")[:10],
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "lastPaidDate"
