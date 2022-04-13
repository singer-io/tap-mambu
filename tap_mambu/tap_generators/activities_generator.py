from singer import utils
from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class ActivitiesGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(ActivitiesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "activities"
        self.endpoint_api_method = "GET"
        self.endpoint_api_version = "v1"

        self.endpoint_params["from"] = transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
        self.endpoint_params["to"] = utils.now().strftime("%Y-%m-%d")[:10]
        self.endpoint_bookmark_field = "timestamp"

    def fetch_batch(self):
        response = super(ActivitiesGenerator, self).fetch_batch()

        for record in response:
            for key, value in record['activity'].items():
                record[key] = value
            del record['activity']
        return response
