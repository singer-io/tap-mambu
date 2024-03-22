from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers.datetime_utils import datetime_to_local_str


class GroupsGenerator(MultithreadedBookmarkGenerator):
    def _init_endpoint_config(self):
        super(GroupsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "groups:search"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_sorting_criteria = {
            "field": "lastModifiedDate",
            "order": "ASC"
        }

    def prepare_batch_params(self):
        super(GroupsGenerator, self).prepare_batch_params()
        self.endpoint_filter_criteria[0]["value"] = datetime_to_local_str(self.endpoint_intermediary_bookmark_value)
