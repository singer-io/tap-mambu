import mock
from mock import PropertyMock

from tap_mambu.tap_mambu_refactor import sync_endpoint_refactor
from .helpers import IsInstanceMockMatcher


@mock.patch("tap_mambu.tap_mambu_refactor.main.LoanAccountsLMGenerator")
@mock.patch("tap_mambu.tap_mambu_refactor.main.LoanAccountsADGenerator")
@mock.patch("tap_mambu.tap_mambu_refactor.main.LoanAccountsProcessor")
def test_sync_endpoint_refactor(mock_loan_accounts_processor, mock_loan_accounts_ad_generator,
                                mock_loan_accounts_lm_generator):
    type(mock_loan_accounts_lm_generator.return_value).type = PropertyMock(return_value="LoanAccountsLMGenerator")
    type(mock_loan_accounts_ad_generator.return_value).type = PropertyMock(return_value="LoanAccountsADGenerator")
    client = "client"
    catalog = "catalog"
    state = "state"
    stream_name = "loan_accounts"
    sub_type = "sub_type"
    config = "config"
    sync_endpoint_refactor(client=client, catalog=catalog, state=state, stream_name=stream_name,
                           sub_type=sub_type, config=config)
    mock_loan_accounts_lm_generator.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                            sub_type=sub_type, config=config)
    mock_loan_accounts_ad_generator.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                            sub_type=sub_type, config=config)
    mock_loan_accounts_processor.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                         sub_type=sub_type, config=config, catalog=catalog,
                                                         generators=[IsInstanceMockMatcher("LoanAccountsLMGenerator"),
                                                                     IsInstanceMockMatcher("LoanAccountsADGenerator")])
