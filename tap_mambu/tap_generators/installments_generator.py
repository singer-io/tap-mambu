from singer import utils
from .generator import TapGenerator
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_utc_str


class InstallmentsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(InstallmentsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "installments"
        self.endpoint_api_method = "GET"
        self.endpoint_params = {
            "dueFrom": datetime_to_utc_str(str_to_localized_datetime(self.start_date))[:10],
            "dueTo": utils.now().strftime("%Y-%m-%d")[:10],
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "lastPaidDate"
