from .multithreaded_bookmark_generator import MultithreadedBookmarkDayByDayGenerator
from ..helpers import get_bookmark, transform_datetime


class CommunicationsGenerator(MultithreadedBookmarkDayByDayGenerator):
    def _init_endpoint_config(self):
        super(CommunicationsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "communications/messages:search"
        self.endpoint_params = {
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "creationDate"
        self.endpoint_filter_criteria = [
            {
                "field": "state",
                "operator": "EQUALS",
                "value": "SENT"
            },
            {
                "field": "creationDate",
                "operator": "AFTER",
                "value": transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
            }
        ]

    def _init_endpoint_body(self):
        self.endpoint_body = self.endpoint_filter_criteria

    def prepare_batch_params(self):
        super(CommunicationsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[1]["value"] = self.endpoint_intermediary_bookmark_value[:10]
