from .multithreaded_parent_processor import MultithreadedParentProcessor


class DepositAccountsProcessor(MultithreadedParentProcessor):
    def _init_endpoint_config(self):
        super(DepositAccountsProcessor, self)._init_endpoint_config()
        self.endpoint_child_streams = ["cards"]
