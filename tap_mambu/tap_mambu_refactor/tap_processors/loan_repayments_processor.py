from .child_processor import ChildProcessor


class LoanRepaymentsProcessor(ChildProcessor):
    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_parent = "loan_accounts"
