from .multithreaded_offset_generator import MultithreadedOffsetGenerator
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime, utc_now


class InstallmentsGenerator(MultithreadedOffsetGenerator):
    def _init_endpoint_config(self):
        super(InstallmentsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "installments"
        self.endpoint_api_method = "GET"
        self.endpoint_params = {
            "dueFrom": datetime_to_utc_str(str_to_localized_datetime(self.start_date))[:10],
            "dueTo": datetime_to_utc_str(utc_now())[:10],
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "lastPaidDate"

    def transform_batch(self, batch):
        temp_batch = super(InstallmentsGenerator, self).transform_batch(batch)
        for record in temp_batch:
            if "number" in record:
                record["number"] = "0"
        return temp_batch
