from .generator import TapGenerator


class CreditArrangementsGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(CreditArrangementsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "creditarrangements"
        self.endpoint_api_method = "GET"
        self.endpoint_params["sortBy"] = "creationDate:ASC"
        self.endpoint_bookmark_field = "lastModifiedDate"
