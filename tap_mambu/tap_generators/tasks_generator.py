from .generator import TapGenerator


class TasksGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(TasksGenerator, self)._init_endpoint_config()
        self.endpoint_path = "tasks"
        self.endpoint_api_method = "GET"
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_params["sortBy"] = "lastModifiedDate:ASC"
        self.endpoint_sorting_criteria = {}

    def _init_params(self):
        super(TasksGenerator, self)._init_params()
        self.limit = 1000
