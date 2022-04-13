from .child_generator import ChildGenerator
from .no_pagination_generator import NoPaginationGenerator


class DepositCardsGenerator(NoPaginationGenerator, ChildGenerator):
    def _init_endpoint_config(self):
        super(DepositCardsGenerator, self)._init_endpoint_config()
        self.endpoint_api_method = "GET"
        self.endpoint_path = f"deposits/{self.endpoint_parent_id}/cards"
        self.endpoint_sorting_criteria = {}
        self.endpoint_filter_criteria = []
