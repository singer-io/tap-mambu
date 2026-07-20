from .child_generator import ChildGenerator
from .no_pagination_generator import NoPaginationGenerator

class LoanRepaymentsGenerator(NoPaginationGenerator, ChildGenerator):

    def _init_endpoint_config(self):
        super(LoanRepaymentsGenerator, self)._init_endpoint_config()
        self.endpoint_api_method = "GET"
        # include parent id in endpoint path
        self.endpoint_path = f"loans/{self.endpoint_parent_id}/schedule"

    def transform_batch(self, batch):
        batch_installments = batch['installments']
        batch_currency = batch['currency']

        for installment in batch_installments: 
            installment['currency'] = batch_currency

        return super(LoanRepaymentsGenerator, self).transform_batch(batch_installments)
