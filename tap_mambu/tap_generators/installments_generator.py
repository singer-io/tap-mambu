import json
from singer import utils
from .multithreaded_offset_generator import MultithreadedOffsetGenerator
from ..helpers import transform_datetime


class InstallmentsGenerator(MultithreadedOffsetGenerator):
    def _init_endpoint_config(self):
        super(InstallmentsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "installments"
        self.endpoint_api_method = "GET"
        self.endpoint_params = {
            "dueFrom": transform_datetime(self.start_date)[:10],
            "dueTo": utils.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:10],
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
