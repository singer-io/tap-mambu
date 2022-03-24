from .processor import TapProcessor


class InterestAccrualBreakdownProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(InterestAccrualBreakdownProcessor, self)._init_endpoint_config()
        self.endpoint_deduplication_key = "entry_id"
