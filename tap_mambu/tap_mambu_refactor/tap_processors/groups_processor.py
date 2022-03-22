from .processor import TapProcessor


class GroupsProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(GroupsProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
