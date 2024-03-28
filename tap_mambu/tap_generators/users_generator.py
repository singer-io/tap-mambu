from .multithreaded_offset_generator import MultithreadedOffsetGenerator


class UsersGenerator(MultithreadedOffsetGenerator):
    def __init__(self, stream_name, client, config, state, sub_type):
        super(UsersGenerator, self).__init__(stream_name, client, config, state, sub_type)
        self.date_windowing = False

    def _init_endpoint_config(self):
        super(UsersGenerator, self)._init_endpoint_config()
        self.endpoint_path = "users"
        self.endpoint_api_method = "GET"
        self.endpoint_params["sortBy"] = "id:ASC"
        self.endpoint_bookmark_field = "lastModifiedDate"
