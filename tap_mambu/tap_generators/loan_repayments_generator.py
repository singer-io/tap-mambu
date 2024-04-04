from .child_generator import ChildGenerator
from typing import List


class LoanRepaymentsGenerator(ChildGenerator):
    def _init_buffers(self):
        self.buffer: List = list()
        self.max_buffer_size = 1000

    def _init_endpoint_config(self):
        super(LoanRepaymentsGenerator, self)._init_endpoint_config()
        self.endpoint_api_version = "v1"
        self.endpoint_api_method = "GET"
        # include parent id in endpoint path
        self.endpoint_path = f"loans/{self.endpoint_parent_id}/repayments"
