from .child_generator import ChildGenerator
from singer import get_logger

from ..helpers.hashable_dict import HashableDict

LOGGER = get_logger()


class LoanRepaymentsGenerator(ChildGenerator):

    def _init_endpoint_config(self):
        super(LoanRepaymentsGenerator, self)._init_endpoint_config()
        self.endpoint_api_method = "GET"
        # include parent id in endpoint path
        self.endpoint_path = f"loans/{self.endpoint_parent_id}/schedule"
        
    def _init_params(self):
        super(LoanRepaymentsGenerator, self)._init_params()
        # set loan repayment request limit to zero since it returns all scheduled repayments on a loan
        self.limit = 0

    def transform_batch(self, batch):
        batch_installments = batch['installments']
        batch_currency = batch['currency']

        for installment in batch_installments: 
            installment['currency'] = batch_currency

        return super(LoanRepaymentsGenerator, self).transform_batch(batch_installments)
