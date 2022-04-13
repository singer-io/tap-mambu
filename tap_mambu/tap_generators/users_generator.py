from .generator import TapGenerator


class UsersGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(UsersGenerator, self)._init_endpoint_config()
        self.endpoint_path = "users"
        self.endpoint_api_method = "GET"
        self.endpoint_params["sortBy"] = "lastModifiedDate:ASC"
        self.endpoint_bookmark_field = "lastModifiedDate"
