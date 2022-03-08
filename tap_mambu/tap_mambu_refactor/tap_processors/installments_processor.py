from .parent_processor import TapProcessor


class InstallmentsProcessor(TapProcessor):
    def _init_endpoint_config(self):
        super(InstallmentsProcessor, self)._init_endpoint_config()
