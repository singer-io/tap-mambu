from .child_generator import ChildGenerator


class LoanRepaymentsGenerator(ChildGenerator):
    def _init_endpoint_config(self):
        super(LoanRepaymentsGenerator, self)._init_endpoint_config()
        self.endpoint_api_version = "v1"
        self.endpoint_api_method = "GET"
        self.endpoint_path = f"loans/{self.endpoint_parent_id}/repayments"  # include parent id in endpoint path
