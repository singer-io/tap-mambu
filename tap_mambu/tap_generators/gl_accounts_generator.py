from .generator import TapGenerator


class GlAccountsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(GlAccountsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "glaccounts"
        self.endpoint_api_method = "GET"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_params["type"] = self.sub_type
