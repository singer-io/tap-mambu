from .generator import TapGenerator


class CustomFieldSetsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(CustomFieldSetsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "customfieldsets"
        self.endpoint_api_method = "GET"
