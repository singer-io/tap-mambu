from .generator import TapGenerator


class BranchesGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(BranchesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "branches"
        self.endpoint_api_method = "GET"
        self.endpoint_params["sortBy"] = "lastModifiedDate:ASC"
        self.endpoint_bookmark_field = "lastModifiedDate"
