from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_local_str


class ClientsGenerator(MultithreadedBookmarkGenerator):
    def _init_config(self):
        super()._init_config()
        self.max_threads = 10

    def _init_endpoint_config(self):
        super(ClientsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "clients:search"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_sorting_criteria = {
            "field": "lastModifiedDate",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": datetime_to_local_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))
            }
        ]

    def prepare_batch_params(self):
        super(ClientsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_local_str(self.endpoint_intermediary_bookmark_value)
