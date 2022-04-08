from .generator import TapGenerator


class CentresGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(CentresGenerator, self)._init_endpoint_config()
        self.endpoint_path = "centres"
        self.endpoint_api_method = "GET"
        self.endpoint_params = {
            "sortBy": "lastModifiedDate:ASC",
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_bookmark_field = "lastModifiedDate"
