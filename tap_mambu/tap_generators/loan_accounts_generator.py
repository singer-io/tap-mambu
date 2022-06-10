import abc

from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class LoanAccountsGenerator(TapGenerator):
    @abc.abstractmethod
    def _init_endpoint_config(self):
        super(LoanAccountsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "loans:search"
        self.endpoint_sorting_criteria = {
            "field": "id",
            "order": "ASC"
        }


class LoanAccountsLMGenerator(LoanAccountsGenerator):
    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_bookmark_field = "lastModifiedDate"
        self.endpoint_filter_criteria = [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
            }
        ]


class LoanAccountsADGenerator(LoanAccountsGenerator):
    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_bookmark_field = "lastAccountAppraisalDate"
        self.endpoint_filter_criteria = [
            {
                "field": "lastAccountAppraisalDate",
                "operator": "AFTER",
                "value": transform_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))[:10]
            }
        ]
