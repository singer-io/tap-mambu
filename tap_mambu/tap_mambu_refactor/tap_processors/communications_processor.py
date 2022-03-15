from .processor import TapProcessor


class CommunicationsProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(CommunicationsProcessor, self)._init_endpoint_config()
        self.endpoint_id_field = "encoded_key"

