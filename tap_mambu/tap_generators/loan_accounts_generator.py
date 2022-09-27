import abc

from .generator import TapGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import str_to_localized_datetime, datetime_to_local_str


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
                "value": datetime_to_local_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))
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
                "value": datetime_to_local_str(str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)))
            }
        ]
