from singer import utils
from .multithreaded_bookmark_generator import MultithreadedBookmarkDayByDayGenerator
from ..helpers import get_bookmark, transform_datetime


class ActivitiesGenerator(MultithreadedBookmarkDayByDayGenerator):
    def _init_endpoint_config(self):
        super(ActivitiesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "activities"
        self.endpoint_api_method = "GET"
        self.endpoint_api_version = "v1"

        self.endpoint_params["from"] = transform_datetime(
            get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
        self.endpoint_params["to"] = utils.now().strftime("%Y-%m-%d")[:10]
        self.endpoint_bookmark_field = "timestamp"

    @staticmethod
    def unpack_activity(record):
        record.update(record["activity"])
        del record["activity"]
        return record

    def transform_batch(self, batch):
        return list(map(self.unpack_activity, super(ActivitiesGenerator, self).transform_batch(batch)))

    def prepare_batch_params(self):
        super(ActivitiesGenerator, self).prepare_batch_params()
        self.static_params['to'] = self.endpoint_intermediary_bookmark_value

    def compare_bookmark_values(self, a, b):
        return a < b
