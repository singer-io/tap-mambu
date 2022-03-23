from .processor import TapProcessor


class UsersProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(UsersProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
