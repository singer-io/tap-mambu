from .multithreaded_bookmark_generator import MultithreadedBookmarkDayByDayGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime, utc_now


class ActivitiesGenerator(MultithreadedBookmarkDayByDayGenerator):
    def _init_endpoint_config(self):
        super(ActivitiesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "activities"
        self.endpoint_api_method = "GET"
        self.endpoint_api_version = "v1"

        self.endpoint_params["from"] = datetime_to_utc_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))[:10]
        self.endpoint_params["to"] = datetime_to_utc_str(utc_now())[:10]
        self.endpoint_bookmark_field = "timestamp"

    def transform_batch(self, batch):
        for record in super(ActivitiesGenerator, self).transform_batch(batch):
            for key, value in record['activity'].items():
                record[key] = value
            del record['activity']
        return batch

    def prepare_batch_params(self):
        super(ActivitiesGenerator, self).prepare_batch_params()
        self.static_params['to'] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)[:10]

    def compare_bookmark_values(self, a, b):
        return a < b
