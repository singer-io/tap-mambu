import json
from singer import utils
from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import transform_json
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_utc_str


class InstallmentsGenerator(MultithreadedBookmarkGenerator):
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

    def prepare_batch_params(self):
        super(InstallmentsGenerator, self).prepare_batch_params()
        self.static_params["dueFrom"] = self.endpoint_intermediary_bookmark_value[:10]

    def _get_transformed_records(self, future):
        records = set()
        for record in self.transform_batch(transform_json(future.result(), self.stream_name)):
            record["number"] = "0"
            records.add(json.dumps(record, ensure_ascii=False).encode("utf8"))
        return records
