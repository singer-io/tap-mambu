from .multithreaded_bookmark_generator import MultithreadedBookmarkDayByDayGenerator
from ..helpers.datetime_utils import datetime_to_utc_str
from datetime import datetime


class ActivitiesGenerator(MultithreadedBookmarkDayByDayGenerator):
    def _init_endpoint_config(self):
        super(ActivitiesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "activities"
        self.endpoint_api_method = "GET"
        self.endpoint_api_version = "v1"
        self.endpoint_bookmark_field = "timestamp"

    def modify_request_params(self, start, end):
        self.static_params["from"] = datetime.strftime(start, '%Y-%m-%d')
        self.static_params["to"] = datetime.strftime(end, '%Y-%m-%d')

    @staticmethod
    def unpack_activity(record):
        record.update(record["activity"])
        del record["activity"]
        return record

    def transform_batch(self, batch):
        return list(map(self.unpack_activity, super(ActivitiesGenerator, self).transform_batch(batch)))

    def prepare_batch_params(self):
        super(ActivitiesGenerator, self).prepare_batch_params()
        self.static_params['to'] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)[:10]

    def compare_bookmark_values(self, a, b):
        return a < b
