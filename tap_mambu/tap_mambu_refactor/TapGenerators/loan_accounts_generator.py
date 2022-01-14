import abc

from .generator import TapGenerator
from ..Helpers import transform_datetime, get_bookmark


class LoanAccountsGenerator(TapGenerator):
    @abc.abstractmethod
    def _init_endpoint_config(self):
        self.endpoint_config = {
            'path': 'loans:search',
            'api_version': 'v2',
            'api_method': 'POST',
            'params': {
                'detailsLevel': 'FULL',
                'paginationDetails': 'ON'
            },
            'body': {
                "sortingCriteria": {
                    "field": "id",
                    "order": "ASC"
                },
                "filterCriteria": [
                    {
                        "field": "",
                        "operator": "AFTER",
                        "value": transform_datetime(
                            get_bookmark(self.state, 'loan_accounts', 'self', self.start_date))[:10]
                    }
                ]
            },
            'bookmark_field': '',
            'bookmark_type': 'datetime',
            'id_fields': ['id'],
            'children': {
                'loan_repayments': {
                    'path': 'loans/{}/repayments',
                    'api_version': 'v1',
                    'api_method': 'GET',
                    'params': {
                        'detailsLevel': 'FULL',
                        'paginationDetails': 'ON'
                    },
                    'id_fields': ['encoded_key'],
                    'parent': 'loan_accounts'
                }
            }
        }


class LoanAccountsLMGenerator(LoanAccountsGenerator):
    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_config["body"]["filterCriteria"][0]["field"] = "lastModifiedDate"
        self.endpoint_config["bookmark_field"] = "lastModifiedDate"


class LoanAccountsADGenerator(LoanAccountsGenerator):
    def _init_endpoint_config(self):
        super()._init_endpoint_config()
        self.endpoint_config["body"]["filterCriteria"][0]["field"] = "lastAccountAppraisalDate"
        self.endpoint_config["bookmark_field"] = "lastAccountAppraisalDate"
