from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_local_str, datetime_to_utc_str
from datetime import datetime


class CommunicationsGenerator(MultithreadedBookmarkGenerator):
    def _init_endpoint_config(self):
        super(CommunicationsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "communications/messages:search"
        self.endpoint_params = {
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "creationDate"

    def modify_request_params(self, start, end):
        self.endpoint_body = [
            {
                "field": self.endpoint_bookmark_field,
                "operator": "AFTER",
                "value": datetime_to_utc_str(start)
            },
            {
                "field": self.endpoint_bookmark_field,
                "operator": "BEFORE",
                "value": datetime_to_utc_str(start)
            },
            {
                "field": "state",
                "operator": "EQUALS",
                "value": "SENT"
            }
        ]

    def _init_endpoint_body(self):
        self.endpoint_body = self.endpoint_filter_criteria

    def prepare_batch_params(self):
        super(CommunicationsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_local_str(self.endpoint_intermediary_bookmark_value)
