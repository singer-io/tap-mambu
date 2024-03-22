from .multithreaded_offset_generator import MultithreadedOffsetGenerator
from datetime import datetime


class InstallmentsGenerator(MultithreadedOffsetGenerator):
    def _init_endpoint_config(self):
        super(InstallmentsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "installments"
        self.endpoint_api_method = "GET"
        self.endpoint_bookmark_field = "lastPaidDate"

    def modify_request_params(self, start, end):
        self.static_params["dueFrom"] = datetime.strftime(start, '%Y-%m-%d')
        self.static_params["dueTo"] = datetime.strftime(end, '%Y-%m-%d')

    def transform_batch(self, batch):
        temp_batch = super(InstallmentsGenerator, self).transform_batch(batch)
        for record in temp_batch:
            if "number" in record:
                record["number"] = "0"
        return temp_batch
