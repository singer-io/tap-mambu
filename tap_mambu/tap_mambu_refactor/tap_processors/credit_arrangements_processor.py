from .processor import TapProcessor


class CreditArrangementsProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(CreditArrangementsProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "id"
