from .generator import TapGenerator


class ClientsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(ClientsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "clients"
        self.endpoint_api_method = "GET"
