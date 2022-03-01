import mock
from mock import MagicMock

from ..constants import config_json


def test_loan_accounts_lm_generator():
    from tap_mambu.tap_mambu_refactor.tap_generators.loan_accounts_generator import LoanAccountsLMGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    lm_generator = LoanAccountsLMGenerator(stream_name="loan_accounts",
                                           client=client_mock,
                                           config=config_json,
                                           state={'currently_syncing': 'loan_accounts'},
                                           sub_type="self")
    assert lm_generator.endpoint_path == "loans:search"
    assert lm_generator.endpoint_bookmark_field == "lastModifiedDate"
    assert lm_generator.endpoint_sorting_criteria == {
            "field": "id",
            "order": "ASC"
        }
    assert lm_generator.endpoint_filter_criteria == [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": '2021-06-01'
            }
        ]


def test_loan_accounts_ad_generator():
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    from tap_mambu.tap_mambu_refactor.tap_generators.loan_accounts_generator import LoanAccountsADGenerator
    ad_generator = LoanAccountsADGenerator(stream_name="loan_accounts",
                                           client=client_mock,
                                           config=config_json,
                                           state={"currently_syncing": "loan_accounts"},
                                           sub_type="self")
    assert ad_generator.endpoint_path == "loans:search"
    assert ad_generator.endpoint_bookmark_field == "lastAccountAppraisalDate"
    assert ad_generator.endpoint_sorting_criteria == {
            "field": "id",
            "order": "ASC"
        }
    assert ad_generator.endpoint_filter_criteria == [
        {
            "field": "lastAccountAppraisalDate",
            "operator": "AFTER",
            "value": '2021-06-01'
        }
    ]
