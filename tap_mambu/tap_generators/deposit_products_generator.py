from .generator import TapGenerator


class DepositProductsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(DepositProductsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "depositproducts"
        self.endpoint_api_method = "GET"
        self.endpoint_bookmark_field = "lastModifiedDate"
