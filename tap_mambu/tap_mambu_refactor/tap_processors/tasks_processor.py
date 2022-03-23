from .processor import TapProcessor


class TasksProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(TasksProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
