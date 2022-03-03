from .parent_processor import TapProcessor


class IndexRateSourcesProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(IndexRateSourcesProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "encoded_key"
