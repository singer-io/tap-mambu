from .generator import TapGenerator


class IndexRateSourcesGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(IndexRateSourcesGenerator, self)._init_endpoint_config()
        self.endpoint_path = "indexratesources"
        self.endpoint_api_method = "GET"
        self.endpoint_sorting_criteria = {}
        self.endpoint_filter_criteria = []
        self.endpoint_bookmark_field = ""
